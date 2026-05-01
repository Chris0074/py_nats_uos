import time
import inspect
import logging

from typing import Callable, Optional, Awaitable
from dataclasses import dataclass, field
from nats.aio.msg import Msg

from nats_connection import NatsConnection
from nats_authent import OAuthCredentials
from login import CLIENT_SCOPE, NATS_HOST, NATS_PORT
from models import VariableDefinitionModel, VariableInfo, VariableAccess, VariableType, VariableStateModel
from models import DATA_TYPE_TO_MODEL, ACCESS_TYPE_TO_MODEL
from nats_subjects import read_variables_query, registry_provider_query, write_variables_command, vars_changed_event
from nats_payloads import build_read_variables_query, build_read_provider_definition_query, build_write_variables_command
from weidmueller.ucontrol.hub.VariableValueInt64 import VariableValueInt64
from weidmueller.ucontrol.hub.VariableValue import VariableValue
from weidmueller.ucontrol.hub.VariableValueBoolean import VariableValueBoolean
from weidmueller.ucontrol.hub.VariableValueFloat64 import VariableValueFloat64
from weidmueller.ucontrol.hub.VariableValueString import VariableValueString
from weidmueller.ucontrol.hub.ReadVariablesQueryResponse import ReadVariablesQueryResponse
from weidmueller.ucontrol.hub.VariablesChangedEvent import VariablesChangedEvent
from weidmueller.ucontrol.hub.ReadProviderDefinitionQueryResponse import ReadProviderDefinitionQueryResponse

@dataclass
class DataHub:
    host: str
    client_name: str
    client_id: str
    client_secret: str
    nats_connection: NatsConnection | None = field(default=None, init=False, repr=False)

    def __post_init__(self):
        self.oauth = OAuthCredentials(
            nats_host= self.host,
            client_name=self.client_name,
            client_id=self.client_id,
            client_secret=self.client_secret,
            scope=CLIENT_SCOPE,
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
        logging.info("Connected to NATS server at %s as %s", self.host, self.client_name)

    def close(self):
        if self.nats_connection:
            return self.nats_connection.close()
        self.nats_connection = None

    def _verify_connection(self) -> NatsConnection:
        conn = self.nats_connection
        if conn is None:
            raise RuntimeError("Not connected to any NATS server")
        return conn


class AccessProvider(DataHub):    
    def __init__(self, host: str, provider_id: str, client_name: str, client_id: str, client_secret: str):
        super().__init__(host=host, client_name=client_name, client_id=client_id, client_secret=client_secret)
        self.provider_id = provider_id

        self.provider_fingerprint: int | None = None
        self.variable_ids: dict[int, VariableInfo] | None = None
        self.variable_name: dict[str, int] | None = None
        self._registered_callbacks: dict[int, list[Callable[[int, dict[int,VariableStateModel]], None | Awaitable[None]]]] = {}
        self.already_subscribed_to_change: bool = False
        self.snapshot: dict[int, VariableStateModel] = {}

    def _decode_definition_models(self, definition) -> tuple[dict[str,int], dict[int, VariableInfo]]:
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
                # Skip unsupported provider variable types.
                continue
            model: Optional[VariableDefinitionModel] = VariableDefinitionModel(
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
                model=model
            )
            self.variable_ids = var_ids
            self.variable_name = var_names
        return var_names, var_ids

    async def get_definition(self):
        conn = self._verify_connection()
        
        subject = registry_provider_query(self.provider_id)
        payload = build_read_provider_definition_query()
        msg = await conn.request(subject, payload)
        response = ReadProviderDefinitionQueryResponse.GetRootAsReadProviderDefinitionQueryResponse(msg.data, 0)
        definition = response.ProviderDefinition()
        if definition is None:
            raise RuntimeError(f"No definition of provider '{self.provider_id}' found.")
        
        return self._decode_definition_models(definition)
    
    def _get_variable_by_name(self, key: str) -> VariableInfo:
        if self.variable_name is None:
            raise RuntimeError("Provider definition not loaded. Call get_definition() first.")
        if self.variable_ids is None:
            raise RuntimeError("Provider definition not loaded. Call get_definition() first.")
        var_id = self.variable_name.get(key)
        if var_id is None:
            raise KeyError(f"Variable with key '{key}' not found in provider definition.")
        var = self.variable_ids.get(var_id)
        if var is None: 
            raise KeyError(f"Variable with key '{key}' not found in provider definition.")
        return var

    def _get_variable_by_id(self, key: int) -> VariableInfo:
        if self.variable_ids is None:
            raise RuntimeError("Provider definition not loaded. Call get_definition() first.")
        var = self.variable_ids.get(key)
        if var is None:
                raise KeyError(f"Variable with ID '{key}' not found in provider definition.")
        return var
    
    def verify_startup(self) -> NatsConnection:
        conn = self._verify_connection()
        if self.variable_ids is None or self.variable_name is None:
            raise RuntimeError("Provider definition not loaded. Call get_definition() first.")
        return conn
        
    def get_variable_from_definition(self, key_id: str | int) -> VariableInfo:
        if isinstance(key_id, str):
            variable = self._get_variable_by_name(key_id)            
            return variable
            
        elif isinstance(key_id, int):
            variable = self._get_variable_by_id(key_id)            
            return variable
        else:
            # Internal error, should not happen if type hints are respected
            raise ValueError("key_id must be either a string (variable key) or an integer (variable ID)")

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
            value = holder.Value()
        elif value_type == VariableValue.Float64:
            holder = VariableValueFloat64()
            holder.Init(value_table.Bytes, value_table.Pos)
            value = holder.Value()
        elif value_type == VariableValue.String:
            holder = VariableValueString()
            holder.Init(value_table.Bytes, value_table.Pos)
            raw_value = holder.Value()
            if raw_value is None:
                return None
            value = raw_value.decode("utf-8")
        elif value_type == VariableValue.Boolean:
            holder = VariableValueBoolean()
            holder.Init(value_table.Bytes, value_table.Pos)
            value = bool(holder.Value())
        else:
            value = "<unbekannter Typ>"

        return value
    
    def _decode_values(self, var_list, selected: dict[int, VariableInfo]) -> list[VariableStateModel]:
        if not var_list:
            return []
        base_ts = var_list.BaseTimestamp()
        base_ns = base_ts.Seconds() * 1_000_000_000 + base_ts.Nanos()

        rows: list[VariableStateModel] = []
        for idx in range(var_list.ItemsLength()):
            key = var_list.Items(idx).Id()
            item = var_list.Items(idx)
            value = self._get_value_from_item(item)
            if value is None:
                continue
            rows.append(                
                VariableStateModel(id=item.Id(), key=key, value=value, timestamp_ns=base_ns, definition=selected.get(item.Id()))
            )
        return rows
    
    def _update_states(self, var_list) -> list[VariableStateModel]:
        if not var_list:
            return []
        variable_ids = self.variable_ids
        if variable_ids is None:
            raise RuntimeError("Provider definition not loaded. Call get_definition() first.")
        base_ts = var_list.BaseTimestamp()
        base_ns = base_ts.Seconds() * 1_000_000_000 + base_ts.Nanos()

        changed: list[VariableStateModel] = []

        for i in range(var_list.ItemsLength()):
            key = var_list.Items(i).Id()
            item = var_list.Items(i)
            value = self._get_value_from_item(item)
            if value is None:
                continue
            state = VariableStateModel(id=item.Id(), key=key, value=value, timestamp_ns=base_ns, definition=variable_ids.get(item.Id()))
            changed.append(state)

        return changed
    
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
    
    async def _process_read_request(self, var_ids: list[int] | None) -> ReadVariablesQueryResponse:
        conn = self._verify_connection()
        subject = read_variables_query(self.provider_id)
        payload = build_read_variables_query(var_ids)
        response_msg = await conn.request(subject, payload)
        #print("Response-Read", response_msg.data.hex())
        return ReadVariablesQueryResponse.GetRootAsReadVariablesQueryResponse(response_msg.data, 0)

    async def read_value(self, key_id: str | int) -> int | float | str | bool | None:
        self.verify_startup()
        variable_ids = self.variable_ids
        if variable_ids is None:
            raise RuntimeError("Provider definition not loaded. Call get_definition() first.")
        variable = self.get_variable_from_definition(key_id)
        response = await self._process_read_request([variable.id])
        values = self._decode_values(response.Variables(), variable_ids)
        #print("Response-Payload:", response.Variables(), "Decoded:", value)

        if not values:
            raise ValueError(f"No values received for variable '{key_id}'.")
        elif len(values) > 1:
            raise ValueError(f"Warning: Multiple values received for variable '{key_id}'. Only one should be returned.")

        return values[0].value if values else None
    
    async def write_value(self, key_id: str | int, value: int | float | str | bool) -> None:
        conn = self.verify_startup()
        
        variable = self.get_variable_from_definition(key_id)
        var_model:VariableDefinitionModel | None = variable.model

        if var_model is None:
            raise RuntimeError(f"No model for variable '{variable.key}' available")
        if var_model.access != VariableAccess.READ_WRITE:
            raise RuntimeError(f"Variable '{var_model.key}' (ID:{var_model.id}) is not writable.")

        converted_value = self._convert_value(var_model, str(value))
        state = VariableStateModel(id=var_model.id, key=var_model.key, value=converted_value, timestamp_ns=time.time_ns())
        
        subject = write_variables_command(self.provider_id)
        if self.provider_fingerprint is None:
            # Sanity check. For writing we need a fingerprint otherwise the value is not written.
            # This should not happen if the provider definition is properly loaded at startup.
            raise RuntimeError("Provider definition not loaded. Call get_definition() first.")
        
        payload = build_write_variables_command([var_model], [state], self.provider_fingerprint)
        #print(f"Write-Subject: {subject}")
        #print("Write-Payload:", payload.hex())

        await conn.publish(subject, payload)
        #print(f"Befehl gesendet: {var_model.key} (ID {var_model.id}) <- {converted_value!r}")
    
    async def _handle_event(self, msg: Msg):
        event = VariablesChangedEvent.GetRootAsVariablesChangedEvent(msg.data, 0)
        changed_variables: list[VariableStateModel] = self._update_states(event.ChangedVariables())
        if not changed_variables:
            return
        # update snapshot with changed variables
        self.snapshot.update({var.id: var for var in changed_variables})
        var_ids = {var.id: var for var in changed_variables}
                    
        for id, callbacks in self._registered_callbacks.items():
            if id in var_ids.keys():
                for cllbck in callbacks:
                    result = cllbck(id, self.snapshot)
                    if inspect.isawaitable(result):
                        await result

    
    async def subscribe_change(self, key_id: str | int, callback: Callable[[int, dict[int, VariableStateModel]], None | Awaitable[None]]) -> None:
        conn = self.verify_startup()

        variable = self.get_variable_from_definition(key_id)
        if self._registered_callbacks.get(variable.id) is None:
            self._registered_callbacks[variable.id] = [callback]
        elif callback not in self._registered_callbacks[variable.id]: 
            self._registered_callbacks[variable.id].append(callback)

        if not self.already_subscribed_to_change:
            subject = vars_changed_event(self.provider_id)
            #print(f"Subscribing to changes of variable '{variable.key}' (ID {variable.id}) on subject '{subject}'")
            await conn.subscribe(subject, callback=self._handle_event)
            self.already_subscribed_to_change = True
            # Read snapshot to have an initial state of all variables. 
            # This is needed to provide the variable states in the callbacks for variable changes 
            # and to have a consistent state when the user reads variable values.
            self.snapshot = await self.request_snapshot()     

    async def request_snapshot(self) -> dict[int, VariableStateModel]:
        self._verify_connection()        
        response = await self._process_read_request(None)
        variables = self._update_states(response.Variables())
        return {var.id: var for var in variables}
    
    async def stop(self) -> None:
        conn = self._verify_connection()
        await conn.close()
        self.nats_connection = None