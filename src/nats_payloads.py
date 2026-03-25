from __future__ import annotations

import hashlib
from typing import Iterable, Sequence, Tuple

from flatbuffers import Builder

from models import VariableDefinitionModel, VariableStateModel, VariableAccess, VariableType

from weidmueller.ucontrol.hub.ProviderDefinitionChangedEvent import (
    ProviderDefinitionChangedEventT,
)
from weidmueller.ucontrol.hub.ProviderDefinition import ProviderDefinitionT
from weidmueller.ucontrol.hub.VariableDefinition import VariableDefinitionT
from weidmueller.ucontrol.hub.Variable import VariableT
from weidmueller.ucontrol.hub.VariableQuality import VariableQuality
from weidmueller.ucontrol.hub.VariableAccessType import VariableAccessType
from weidmueller.ucontrol.hub.VariableDataType import VariableDataType
from weidmueller.ucontrol.hub.Timestamp import TimestampT
from weidmueller.ucontrol.hub.VariablesChangedEvent import VariablesChangedEventT
from weidmueller.ucontrol.hub.VariableList import VariableListT
from weidmueller.ucontrol.hub.ReadVariablesQueryResponse import ReadVariablesQueryResponseT
from weidmueller.ucontrol.hub.VariableValue import VariableValue
from weidmueller.ucontrol.hub.VariableValueInt64 import VariableValueInt64T
from weidmueller.ucontrol.hub.VariableValueFloat64 import VariableValueFloat64T
from weidmueller.ucontrol.hub.VariableValueString import VariableValueStringT
from weidmueller.ucontrol.hub.VariableValueBoolean import VariableValueBooleanT
from weidmueller.ucontrol.hub.ReadVariablesQueryRequest import ReadVariablesQueryRequestT
from weidmueller.ucontrol.hub.ReadProvidersQueryRequest import ReadProvidersQueryRequestT
from weidmueller.ucontrol.hub.ReadProviderDefinitionQueryRequest import (
    ReadProviderDefinitionQueryRequestT,
)
from weidmueller.ucontrol.hub.WriteVariablesCommand import WriteVariablesCommandT

def _timestamp_from_state(state: VariableStateModel) -> TimestampT:
    ts = TimestampT()
    ts.seconds = state.timestamp_ns // 1_000_000_000
    ts.nanos = int(state.timestamp_ns % 1_000_000_000)
    return ts


def _quality_to_enum(quality: str) -> int:
    mapping = {
        "GOOD": VariableQuality.GOOD,
        "BAD": VariableQuality.BAD,
        "UNCERTAIN": VariableQuality.UNCERTAIN,
    }
    return mapping.get(quality.upper(), VariableQuality.GOOD)


def _value_to_union(
    definition: VariableDefinitionModel, state: VariableStateModel
) -> Tuple[int, object]:
    if definition.data_type == VariableType.INT64:
        value = VariableValueInt64T()
        value.value = int(state.value)
        return VariableValue.Int64, value
    if definition.data_type == VariableType.FLOAT64:
        value = VariableValueFloat64T()
        value.value = float(state.value)
        return VariableValue.Float64, value
    if definition.data_type == VariableType.STRING:
        value = VariableValueStringT()
        value.value = str(state.value)
        return VariableValue.String, value
    if definition.data_type == VariableType.BOOLEAN:
        value = VariableValueBooleanT()
        value.value = bool(state.value)
        return VariableValue.Boolean, value
    raise ValueError(f"Nicht unterstützter Datentyp: {definition.data_type}")


def _fingerprint(definitions: Sequence[VariableDefinitionModel]) -> int:
    hasher = hashlib.sha256()
    for var in sorted(definitions, key=lambda x: x.id):
        hasher.update(
            f"{var.id}:{var.key}:{var.data_type.value}:{var.access.value}:{var.experimental}".encode()
        )
    digest = hasher.digest()
    return int.from_bytes(digest[:8], "big", signed=False)


def _def_to_flat(var: VariableDefinitionModel) -> VariableDefinitionT:
    access = (
        VariableAccessType.READ_WRITE
        if var.access == VariableAccess.READ_WRITE
        else VariableAccessType.READ_ONLY
    )
    data_type_map = {
        VariableType.INT64: VariableDataType.INT64,
        VariableType.FLOAT64: VariableDataType.FLOAT64,
        VariableType.STRING: VariableDataType.STRING,
        VariableType.BOOLEAN: VariableDataType.BOOLEAN,
    }
    definition = VariableDefinitionT()
    definition.key = var.key
    definition.id = var.id
    definition.dataType = data_type_map[var.data_type]
    definition.accessType = access
    definition.experimental = var.experimental
    return definition


def _build_variable_list(
    variables: Sequence[VariableDefinitionModel],
    states: Iterable[VariableStateModel],
    fingerprint: int,
) -> VariableListT:
    states_by_id = {state.id: state for state in states}
    items: list[VariableT] = []

    for definition in variables:
        state = states_by_id.get(definition.id)
        if state is None:
            continue

        var = VariableT()
        var.id = definition.id
        value_type, value_obj = _value_to_union(definition, state)
        var.valueType = value_type
        var.value = value_obj
        var.timestamp = _timestamp_from_state(state)
        var.quality = _quality_to_enum(state.quality)
        items.append(var)

    base_ts = (
        _timestamp_from_state(next(iter(states_by_id.values())))
        if states_by_id
        else TimestampT()
    )
    var_list = VariableListT()
    var_list.providerDefinitionFingerprint = fingerprint
    var_list.baseTimestamp = base_ts
    var_list.items = items
    return var_list


def build_provider_definition_event(
    vars: Sequence[VariableDefinitionModel],
) -> tuple[bytes, int]:
    fingerprint = _fingerprint(vars)
    definition = ProviderDefinitionT()
    definition.fingerprint = fingerprint
    definition.variableDefinitions = [_def_to_flat(var) for var in vars]

    event = ProviderDefinitionChangedEventT()
    event.providerDefinition = definition
    builder = Builder(1024)
    root = event.Pack(builder)
    builder.Finish(root)
    return bytes(builder.Output()), fingerprint


def build_variables_changed_event(
    variables: Sequence[VariableDefinitionModel],
    states: Iterable[VariableStateModel],
    fingerprint: int,
) -> bytes:
    var_list = _build_variable_list(variables, states, fingerprint)
    event = VariablesChangedEventT()
    event.changedVariables = var_list
    builder = Builder(1024)
    root = event.Pack(builder)
    builder.Finish(root)
    return bytes(builder.Output())


def build_read_variables_response(
    variables: Sequence[VariableDefinitionModel],
    states: Iterable[VariableStateModel],
    fingerprint: int,
) -> bytes:
    var_list = _build_variable_list(variables, states, fingerprint)
    response = ReadVariablesQueryResponseT()
    response.variables = var_list
    builder = Builder(1024)
    root = response.Pack(builder)
    builder.Finish(root)
    return bytes(builder.Output())


def build_read_variables_query(ids: Iterable[int] | None) -> bytes:
    request = ReadVariablesQueryRequestT(ids=list(ids) if ids else None)
    builder = Builder(128)
    root = request.Pack(builder)
    builder.Finish(root)
    return bytes(builder.Output())


def build_read_providers_query() -> bytes:
    request = ReadProvidersQueryRequestT()
    builder = Builder(32)
    root = request.Pack(builder)
    builder.Finish(root)
    return bytes(builder.Output())


def build_read_provider_definition_query() -> bytes:
    request = ReadProviderDefinitionQueryRequestT()
    builder = Builder(32)
    root = request.Pack(builder)
    builder.Finish(root)
    return bytes(builder.Output())


def build_write_variables_command(
    variables: Sequence[VariableDefinitionModel],
    states: Iterable[VariableStateModel],
    fingerprint: int = 0,
) -> bytes:
    var_list = _build_variable_list(variables, states, fingerprint=fingerprint)
    command = WriteVariablesCommandT()
    command.variables = var_list
    builder = Builder(256)
    root = command.Pack(builder)
    builder.Finish(root)
    return bytes(builder.Output())
