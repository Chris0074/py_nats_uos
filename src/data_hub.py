from __future__ import annotations

import inspect
import logging
import sys
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path

from nats.aio.msg import Msg

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = Path(__file__).resolve().parent
for candidate in (str(PROJECT_ROOT), str(SRC_DIR)):
    if candidate not in sys.path:
        sys.path.insert(0, candidate)

from login import CLIENT_SCOPE, NATS_HOST, NATS_PORT
from models import (
    ACCESS_TYPE_TO_MODEL,
    DATA_TYPE_TO_MODEL,
    VariableAccess,
    VariableDefinitionModel,
    VariableInfo,
    VariableStateModel,
    VariableType,
)
from nats_authent import OAuthCredentials
from nats_connection import NatsConnection
from nats_payloads import (
    build_provider_definition_event,
    build_read_provider_definition_query,
    build_read_variables_query,
    build_read_variables_response,
    build_variables_changed_event,
    build_write_variables_command,
)
from nats_subjects import (
    provider_changed_event,
    read_variables_query,
    registry_provider_query,
    vars_changed_event,
    write_variables_command,
)
from weidmueller.ucontrol.hub.ProviderDefinition import ProviderDefinition
from weidmueller.ucontrol.hub.ReadProviderDefinitionQueryResponse import ReadProviderDefinitionQueryResponse
from weidmueller.ucontrol.hub.ReadVariablesQueryRequest import ReadVariablesQueryRequest
from weidmueller.ucontrol.hub.ReadVariablesQueryResponse import ReadVariablesQueryResponse
from weidmueller.ucontrol.hub.VariableValue import VariableValue
from weidmueller.ucontrol.hub.VariableValueBoolean import VariableValueBoolean
from weidmueller.ucontrol.hub.VariableValueFloat64 import VariableValueFloat64
from weidmueller.ucontrol.hub.VariableValueInt64 import VariableValueInt64
from weidmueller.ucontrol.hub.VariableValueString import VariableValueString
from weidmueller.ucontrol.hub.VariablesChangedEvent import VariablesChangedEvent
from weidmueller.ucontrol.hub.WriteVariablesCommand import WriteVariablesCommand

logger = logging.getLogger(__name__)

@dataclass
class DataHub:
    host: str
    client_name: str
    client_id: str
    client_secret: str
    client_scope: str = CLIENT_SCOPE
    nats_connection: NatsConnection | None = field(default=None, init=False, repr=False)

    def __post_init__(self):
        self.oauth = OAuthCredentials(
            nats_host= self.host,
            client_name=self.client_name,
            client_id=self.client_id,
            client_secret=self.client_secret,
            scope=self.client_scope,
        )

    async def connect(self):
        token = await self.oauth.request_token()
        nats_conn = NatsConnection(
            host=NATS_HOST,
            port=NATS_PORT,
            client_name=self.client_name,
            token=token,
        )
        await nats_conn.connect()
        self.nats_connection = nats_conn
        logger.info("Connected to NATS server at %s as %s", self.host, self.client_name)

    async def close(self):
        if self.nats_connection:
            return await self.nats_connection.close()
        self.nats_connection = None

    def _verify_connection(self) -> NatsConnection:
        conn = self.nats_connection
        if conn is None:
            raise ConnectionError("Not connected to any NATS server. Call connect() first.")
        return conn

class ProviderCommon(DataHub):
    def __init__(
        self,
        host: str,
        provider_id: str,
        client_name: str,
        client_id: str,
        client_secret: str,
        provider_fingerprint: int | None = None,
        variable_ids: dict[int, VariableInfo] | None = None,
        variable_name: dict[str, int] | None = None,
    ) -> None:
        super().__init__(
            host=host,
            client_name=client_name,
            client_id=client_id,
            client_secret=client_secret,
        )
        self.already_subscribed_to_change: bool = False
        self.provider_id = provider_id
        self.provider_fingerprint = provider_fingerprint
        self.variable_ids = variable_ids
        self.variable_name = variable_name
        self._registered_callbacks: dict[int, list[Callable[[int, dict[int, VariableStateModel]], None | Awaitable[None]]]] = {}

    def _verify_connection(self) -> NatsConnection:
        conn = self.nats_connection
        if conn is None:
            raise ConnectionError("Not connected to any NATS server. Call connect() first.")
        return conn

    def verify_startup(self) -> NatsConnection:
        """Verify that connection is established and provider definition is loaded."""
        conn = self._verify_connection()
        if self.variable_ids is None or self.variable_name is None:
            raise RuntimeError("Provider definition not loaded. Call get_definition() first.")
        return conn

    def _decode_definition_models(self, definition: ProviderDefinition) -> tuple[dict[str, int], dict[int, VariableInfo]]:
        var_names: dict[str, int] = {}
        var_ids: dict[int, VariableInfo] = {}

        self.provider_fingerprint = definition.Fingerprint()
        if definition is None or definition.VariableDefinitionsIsNone():
            return var_names, var_ids

        for idx in range(definition.VariableDefinitionsLength()):
            entry = definition.VariableDefinitions(idx)
            if entry is None:
                continue
            key = entry.Key()
            if isinstance(key, (bytes, bytearray)):
                key = key.decode("utf-8")
            access_raw = entry.AccessType()
            data_raw = entry.DataType()
            access = ACCESS_TYPE_TO_MODEL.get(access_raw)
            data_type = DATA_TYPE_TO_MODEL.get(data_raw)
            if access is None or data_type is None:
                continue

            model: VariableDefinitionModel | None = VariableDefinitionModel(
                id=entry.Id(),
                key=str(key),
                data_type=data_type,
                access=access,
                experimental=entry.Experimental(),
            )

            var_names[str(key)] = entry.Id()
            var_ids[entry.Id()] = VariableInfo(
                id=entry.Id(),
                key=str(key),
                access=access,
                data_type=data_type,
                experimental=entry.Experimental(),
                model=model,
            )
            self.variable_ids = var_ids
            self.variable_name = var_names
        return var_names, var_ids

    async def stop(self) -> None:
        """Close the connection to NATS server."""
        conn = self._verify_connection()
        await conn.close()
        self.nats_connection = None

    async def get_definition(self):
        conn = self._verify_connection()

        subject = registry_provider_query(self.provider_id)
        payload = build_read_provider_definition_query()
        msg = await conn.request(subject, payload)
        response = ReadProviderDefinitionQueryResponse.GetRootAsReadProviderDefinitionQueryResponse(msg.data, 0)
        definition = response.ProviderDefinition()
        if definition is None:
            raise ValueError(f"No definition of provider '{self.provider_id}' found.")

        return self._decode_definition_models(definition)

    def _get_variable_by_name(self, key: str) -> VariableInfo:
        if self.variable_name is None or self.variable_ids is None:
            raise RuntimeError("Provider definition not loaded. Call get_definition() first.")
        var_id = self.variable_name.get(key)
        if var_id is None:
            raise KeyError(f"Variable with key '{key}' not found in provider definition.")
        return self.variable_ids[var_id]

    def _get_variable_by_id(self, key: int) -> VariableInfo:
        if self.variable_ids is None:
            raise RuntimeError("Provider definition not loaded. Call get_definition() first.")
        var = self.variable_ids.get(key)
        if var is None:
            raise KeyError(f"Variable with ID '{key}' not found in provider definition.")
        return var

    def get_variable_from_definition(self, key_id: str | int) -> VariableInfo:
        if isinstance(key_id, str):
            return self._get_variable_by_name(key_id)
        if isinstance(key_id, int):
            return self._get_variable_by_id(key_id)
        raise TypeError("key_id must be either a string (variable key) or an integer (variable ID)")

    def _convert_value(self, model: VariableDefinitionModel, value: str):
        if model.data_type == VariableType.INT64:
            return int(value)
        if model.data_type == VariableType.FLOAT64:
            return float(value)
        if model.data_type == VariableType.STRING:
            return value
        if model.data_type == VariableType.BOOLEAN:
            if value.lower() in {"true", "1", "on", "yes"}:
                return True
            if value.lower() in {"false", "0", "off", "no"}:
                return False
            raise ValueError("Boolean-Value must be true/false or 1/0.")
        raise ValueError(f"Data typ {model.data_type} supports no writing.")

    async def write_value(self, key_id: str | int, value: float | str | bool) -> None:
        conn = self.verify_startup()

        variable = self.get_variable_from_definition(key_id)
        var_model: VariableDefinitionModel | None = variable.model

        if var_model is None:
            raise RuntimeError(f"No model for variable '{variable.key}' available")
        if var_model.access != VariableAccess.READ_WRITE:
            raise ValueError(f"Variable '{var_model.key}' (ID:{var_model.id}) is not writable.")

        converted_value = self._convert_value(var_model, str(value))
        state = VariableStateModel(id=var_model.id, key=var_model.key, value=converted_value, timestamp_ns=time.time_ns())

        subject = write_variables_command(self.provider_id)
        if self.provider_fingerprint is None:
            raise RuntimeError("Provider fingerprint not available. Provider definition may not be loaded. Call get_definition() first.")

        payload = build_write_variables_command([var_model], [state], self.provider_fingerprint)
        await conn.publish(subject, payload)

    def _get_value_from_item(self, item) -> int | float | str | bool | None:
        if item is None:
            return None
        value_table = item.Value()
        if value_table is None:
            return None
        value_type = item.ValueType()
        if value_type == VariableValue.Int64:
            holder = VariableValueInt64()
            holder.Init(value_table.Bytes, value_table.Pos)
            return holder.Value()
        if value_type == VariableValue.Float64:
            holder = VariableValueFloat64()
            holder.Init(value_table.Bytes, value_table.Pos)
            return holder.Value()
        if value_type == VariableValue.String:
            holder = VariableValueString()
            holder.Init(value_table.Bytes, value_table.Pos)
            raw_value = holder.Value()
            if raw_value is None:
                return None
            return raw_value.decode("utf-8")
        if value_type == VariableValue.Boolean:
            holder = VariableValueBoolean()
            holder.Init(value_table.Bytes, value_table.Pos)
            return bool(holder.Value())
        return "<unbekannter Typ>"

    def _decode_values(self, var_list, variable_ids: dict[int, VariableInfo] | None = None) -> list[VariableStateModel]:
        """Decode variable values from response or event data."""
        if not var_list:
            return []

        if variable_ids is None:
            variable_ids = self.variable_ids
            if variable_ids is None:
                raise RuntimeError("Provider definition not loaded. Call get_definition() first.")

        base_ts = var_list.BaseTimestamp()
        base_ns = base_ts.Seconds() * 1_000_000_000 + base_ts.Nanos()

        rows: list[VariableStateModel] = []
        for idx in range(var_list.ItemsLength()):
            item = var_list.Items(idx)
            value = self._get_value_from_item(item)
            if value is None:
                continue
            rows.append(
                VariableStateModel(
                    id=item.Id(),
                    key=item.Id(),
                    value=value,
                    timestamp_ns=base_ns,
                    definition=variable_ids.get(item.Id()),
                )
            )
        return rows

    async def _process_read_request(self, var_ids: list[int] | None) -> ReadVariablesQueryResponse:
        conn = self._verify_connection()
        subject = read_variables_query(self.provider_id)
        payload = build_read_variables_query(var_ids)
        response_msg = await conn.request(subject, payload)
        return ReadVariablesQueryResponse.GetRootAsReadVariablesQueryResponse(response_msg.data, 0)

    async def request_snapshot(self) -> dict[int, VariableStateModel]:
        self._verify_connection()
        response = await self._process_read_request(None)
        variables = self._decode_values(response.Variables())
        return {var.id: var for var in variables}

    async def _handle_read_request(self, msg) -> None:
        if self.provider_fingerprint is None:
            raise RuntimeError("Provider fingerprint not available. Provider definition may not be loaded. Call get_definition() first.")
        states: list[VariableStateModel] = [var for var in (await self.request_snapshot()).values()]
        conn = self._verify_connection()
        if self.variable_ids is None:
            raise RuntimeError("Provider definition not loaded. Call get_definition() first.")
        variables: list[VariableDefinitionModel] = [var.model for var in self.variable_ids.values() if var.model is not None]
        payload = build_read_variables_response(variables, states, self.provider_fingerprint)
        await conn.publish(msg.reply, payload)

    async def _read(self, key_id: str | int) -> list[VariableStateModel]:
        self.verify_startup()
        variable = self.get_variable_from_definition(key_id)
        response = await self._process_read_request([variable.id])
        values = self._decode_values(response.Variables())
        return [val for val in values if val.id == variable.id]

    async def read_value(self, key_id: str | int) -> int | float | str | bool | None:
        self.verify_startup()
        if self.variable_ids is None:
            raise RuntimeError("Provider definition not loaded. Call get_definition() first.")
        values = await self._read(key_id)
        if len(values) == 0:
            raise ConnectionError(f"No values received for variable '{key_id}'. Provider may be offline.")
        if len(values) > 1:
            raise RuntimeError(f"Multiple values received for variable '{key_id}'. Expected exactly one.")
        return values[0].value
    
    async def _handle_event(self, msg: Msg):
        event = VariablesChangedEvent.GetRootAsVariablesChangedEvent(msg.data, 0)
        changed_variables: list[VariableStateModel] = self._decode_values(event.ChangedVariables())
        if not changed_variables:
            return

        self.snapshot.update({var.id: var for var in changed_variables})
        var_ids = {var.id: var for var in changed_variables}

        for callback_id, callbacks in self._registered_callbacks.items():
            if callback_id in var_ids:
                for callback in callbacks:
                    result = callback(callback_id, self.snapshot)
                    if inspect.isawaitable(result):
                        await result
    
    async def subscribe_change(
        self,
        key_id: str | int,
        callback: Callable[[int, dict[int, VariableStateModel]], None | Awaitable[None]],
    ) -> None:
        conn = self.verify_startup()

        variable = self.get_variable_from_definition(key_id)
        if self._registered_callbacks.get(variable.id) is None:
            self._registered_callbacks[variable.id] = [callback]
        elif callback not in self._registered_callbacks[variable.id]:
            self._registered_callbacks[variable.id].append(callback)

        if not self.already_subscribed_to_change:
            subject = vars_changed_event(self.provider_id)
            await conn.subscribe(subject, callback=self._handle_event)
            self.already_subscribed_to_change = True
            self.snapshot = await self.request_snapshot()


class AccessProvider(ProviderCommon):
    def __init__(self, host: str, provider_id: str, client_name: str, client_id: str, client_secret: str):
        super().__init__(
            host=host,
            provider_id=provider_id,
            client_name=client_name,
            client_id=client_id,
            client_secret=client_secret,
        )
        self._registered_callbacks: dict[int, list[Callable[[int, dict[int, VariableStateModel]], None | Awaitable[None]]]] = {}
        self.snapshot: dict[int, VariableStateModel] = {}


class RegisterProvider(ProviderCommon):
    def __init__(self, host: str, provider_id: str, client_name: str, client_id: str, client_secret: str):
        super().__init__(
            host=host,
            provider_id=provider_id,
            client_name=client_name,
            client_id=client_id,
            client_secret=client_secret,
        )

        if provider_id != client_name:
            raise ValueError("Provider ID must match the client name for registering a provider.")

        self.provider_fingerprint: int | None = None
        # Local state store for the variables served by this provider.
        self._variable_states: dict[int, VariableStateModel] = {}
        self._serving: bool = False

    @staticmethod
    def _default_value(data_type: VariableType):
        if data_type == VariableType.INT64:
            return 0
        if data_type == VariableType.FLOAT64:
            return 0.0
        if data_type == VariableType.STRING:
            return ""
        if data_type == VariableType.BOOLEAN:
            return False
        return 0

    def _populate_local_definition(self, definitions: list[VariableDefinitionModel]) -> None:
        """Fill variable_ids / variable_name from the definitions we register ourselves."""
        var_names: dict[str, int] = {}
        var_ids: dict[int, VariableInfo] = {}
        for var in definitions:
            var_names[var.key] = var.id
            var_ids[var.id] = VariableInfo(
                id=var.id,
                key=var.key,
                access=var.access,
                data_type=var.data_type,
                experimental=var.experimental,
                model=var,
            )
            # Seed an initial state so consumers get a value on their first read.
            if var.id not in self._variable_states:
                self._variable_states[var.id] = VariableStateModel(
                    id=var.id,
                    key=var.key,
                    value=self._default_value(var.data_type),
                    timestamp_ns=time.time_ns(),
                )
        self.variable_ids = var_ids
        self.variable_name = var_names

    def _models_for_states(self, states: list[VariableStateModel]) -> list[VariableDefinitionModel]:
        if self.variable_ids is None:
            return []
        models: list[VariableDefinitionModel] = []
        for state in states:
            info = self.variable_ids.get(state.id)
            if info is not None and info.model is not None:
                models.append(info.model)
        return models

    async def _publish_variable_states(self, states: list[VariableStateModel]) -> None:
        """Publish a VariablesChangedEvent so consumers (and the web UI) see the values."""
        if not states:
            return
        if self.provider_fingerprint is None:
            raise RuntimeError("Provider fingerprint not set. Call register_provider_definition() first.")
        conn = self._verify_connection()
        payload = build_variables_changed_event(
            self._models_for_states(states), states, self.provider_fingerprint
        )
        await conn.publish(vars_changed_event(self.provider_id), payload)

    async def _handle_read_request(self, msg: Msg) -> None:
        """Answer a ReadVariablesQueryRequest from a consumer with our local state."""
        if self.provider_fingerprint is None or self.variable_ids is None:
            return
        request = ReadVariablesQueryRequest.GetRootAsReadVariablesQueryRequest(msg.data, 0)
        if request.IdsIsNone() or request.IdsLength() == 0:
            requested_ids = list(self._variable_states.keys())
        else:
            requested_ids = [request.Ids(i) for i in range(request.IdsLength())]

        states = [self._variable_states[i] for i in requested_ids if i in self._variable_states]
        payload = build_read_variables_response(
            self._models_for_states(states), states, self.provider_fingerprint
        )
        if msg.reply:
            conn = self._verify_connection()
            await conn.publish(msg.reply, payload)

    async def _handle_write_request(self, msg: Msg) -> None:
        """Accept a WriteVariablesCommand from a consumer, update local state and notify."""
        cmd = WriteVariablesCommand.GetRootAsWriteVariablesCommand(msg.data, 0)
        var_list = cmd.Variables()
        if var_list is None:
            return

        updated_states: list[VariableStateModel] = []
        for idx in range(var_list.ItemsLength()):
            item = var_list.Items(idx)
            if item is None:
                continue
            var_id = item.Id()
            current = self._variable_states.get(var_id)
            if current is None:
                continue
            new_value = self._get_value_from_item(item)
            if new_value is None:
                continue
            new_state = VariableStateModel(
                id=var_id,
                key=current.key,
                value=new_value,
                timestamp_ns=time.time_ns(),
            )
            self._variable_states[var_id] = new_state
            updated_states.append(new_state)

        if updated_states:
            await self._publish_variable_states(updated_states)

    async def _start_serving(self) -> None:
        """Subscribe to read queries and write commands addressed to this provider."""
        if self._serving:
            return
        conn = self._verify_connection()
        await conn.subscribe(
            read_variables_query(self.provider_id),
            callback=self._handle_read_request,
        )
        await conn.subscribe(
            write_variables_command(self.provider_id),
            callback=self._handle_write_request,
        )
        # Make sure both subscriptions are in place on the server before we
        # announce the provider (otherwise the first consumer read may race).
        await conn.flush()
        self._serving = True

    async def register_provider_definition(self, variable_definition: list[VariableDefinitionModel]) -> None:
        payload, fingerprint = build_provider_definition_event(variable_definition)
        self.provider_fingerprint = fingerprint
        self._populate_local_definition(variable_definition)

        # Subscribe to read/write BEFORE announcing so we can serve immediately.
        await self._start_serving()

        conn = self._verify_connection()
        await conn.publish(provider_changed_event(self.provider_id), payload)

        # Publish the initial values so the web UI has something to display.
        await self._publish_variable_states(list(self._variable_states.values()))

    async def write_value(self, key_id: str | int, value: float | str | bool) -> None:
        """Provider-side value update: change local state and publish a VariablesChangedEvent."""
        self._verify_connection()
        variable = self.get_variable_from_definition(key_id)
        var_model = variable.model
        if var_model is None:
            raise RuntimeError(f"No model for variable '{variable.key}' available")

        converted_value = self._convert_value(var_model, str(value))
        state = VariableStateModel(
            id=var_model.id,
            key=var_model.key,
            value=converted_value,
            timestamp_ns=time.time_ns(),
        )
        self._variable_states[var_model.id] = state
        await self._publish_variable_states([state])

    async def read_value(self, key_id: str | int) -> int | float | str | bool | None:
        """Provider-side read: return the current local state."""
        variable = self.get_variable_from_definition(key_id)
        state = self._variable_states.get(variable.id)
        return None if state is None else state.value
