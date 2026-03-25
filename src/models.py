from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from weidmueller.ucontrol.hub.VariableValue import VariableValue
from weidmueller.ucontrol.hub.VariableValueBoolean import VariableValueBoolean
from weidmueller.ucontrol.hub.VariableValueFloat64 import VariableValueFloat64
from weidmueller.ucontrol.hub.VariableValueInt64 import VariableValueInt64
from weidmueller.ucontrol.hub.VariableValueString import VariableValueString
from weidmueller.ucontrol.hub.ProviderDefinitionState import ProviderDefinitionState
from weidmueller.ucontrol.hub.VariableAccessType import VariableAccessType
from weidmueller.ucontrol.hub.VariableDataType import VariableDataType

@dataclass
class VariableInfo:
    id: int
    key: str
    data_type: VariableType
    access: VariableAccess
    experimental: bool = False
    model: VariableDefinitionModel | None = None

class VariableType(str, Enum):
    INT64 = "int64"
    FLOAT64 = "float64"
    STRING = "string"
    BOOLEAN = "boolean"


class VariableAccess(str, Enum):
    READ_ONLY = "read-only"
    READ_WRITE = "read-write"


@dataclass
class VariableDefinitionModel:
    id: int
    key: str
    data_type: VariableType
    access: VariableAccess
    experimental: bool = False

    def _print(self):
        name = self.__class__.__name__
        print(f"{name}(id={self.id}, key='{self.key}', data_type={self.data_type}, access={self.access}, experimental={self.experimental})")


@dataclass
class VariableStateModel:
    id: int
    key: str
    value: Any
    quality: str = "GOOD"
    timestamp_ns: int = field(default=0)
    definition: VariableInfo | None = None

    def _print(self):
        name = self.__class__.__name__
        print(f"{name}(id={self.id}, value={self.value}, quality='{self.quality}', timestamp_ns={self.timestamp_ns})")


@dataclass
class ConnectionSettings:
    host: str
    port: int
    provider_id: str
    client_name: str

ACCESS_TYPE_LABELS = {
    VariableAccessType.READ_ONLY: "READ_ONLY",
    VariableAccessType.READ_WRITE: "READ_WRITE",
}

DATA_TYPE_LABELS = {
    VariableDataType.BOOLEAN: "BOOLEAN",
    VariableDataType.FLOAT64: "FLOAT64",
    VariableDataType.INT64: "INT64",
    VariableDataType.STRING: "STRING",
}

ACCESS_TYPE_TO_MODEL = {
    VariableAccessType.READ_ONLY: VariableAccess.READ_ONLY,
    VariableAccessType.READ_WRITE: VariableAccess.READ_WRITE,
}

DATA_TYPE_TO_MODEL = {
    VariableDataType.BOOLEAN: VariableType.BOOLEAN,
    VariableDataType.FLOAT64: VariableType.FLOAT64,
    VariableDataType.INT64: VariableType.INT64,
    VariableDataType.STRING: VariableType.STRING,
}