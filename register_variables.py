import pathlib
import sys
from typing import Any

# Add the project root to Python path to enable absolute imports
ROOT_PATH = pathlib.Path(__file__).resolve().parent
if str(ROOT_PATH) not in sys.path:
    sys.path.insert(0, str(ROOT_PATH))

from collections.abc import Callable, Sequence
from login import CLIENT_ID, CLIENT_NAME, CLIENT_SECRET, NATS_HOST
from src.data_hub import RegisterProvider
from src.models import VariableInfo, VariableStateModel, VariableDefinitionModel, VariableAccess, VariableType

import logging

logger = logging.getLogger(__name__)

RaffTiltTime = VariableDefinitionModel(
    id = 1,
    key = "raffstore_tilt_time",
    data_type = VariableType.FLOAT64,
    access = VariableAccess.READ_WRITE)

RaffUpTime = VariableDefinitionModel(
    id = 2,
    key = "raffstore_up_time",
    data_type = VariableType.STRING,
    access = VariableAccess.READ_WRITE)

RaffDownTime = VariableDefinitionModel(
    id = 3,
    key = "raffstore_down_time",
    data_type = VariableType.STRING,
    access = VariableAccess.READ_WRITE)

TestVariable = VariableDefinitionModel(
    id = 4,
    key = "test_variable",
    data_type = VariableType.BOOLEAN,
    access = VariableAccess.READ_WRITE
)

# Keys appear exactly like this inside the u-OS Data Hub tree.
_variable_definitions = [
    RaffTiltTime,
    RaffUpTime,
    RaffDownTime,
    TestVariable
]


class InitProviderVariables:
    _setup_finished = False
    _definition = None
    
    def __init__(self, variable_definitions: list[VariableDefinitionModel] = _variable_definitions) -> None:
        self._variables: dict[int, VariableDefinitionModel] = {}
        self._registered_callbacks: dict[int, Sequence[Callable[[int], None]]] = {}
        
        self._var_defs = variable_definitions
        self._register_provider = RegisterProvider(
                host=NATS_HOST,
                provider_id=CLIENT_NAME,
                client_name=CLIENT_NAME,
                client_id=CLIENT_ID,
                client_secret=CLIENT_SECRET,
            )
        
        # Remember all variable definitions
        for var in variable_definitions:
            self._variables[var.id] = var
            
    async def setup(self):
        if not self._setup_finished:
            await self._register_provider.connect()
            logger.info("Info: Connected to data hub.")
            await self._register_provider.register_provider_definition(self._var_defs)
            logger.info("Provider definition registered successfully.")
            # Get definition to verify that it was registered correctly
            self._definition = await self._register_provider.get_definition()
            logger.info(f"Provider definition retrieved successfully: {self._definition}")
                    
    async def init(self, key_id: str | int, value: float | bool | str, callbacks:  Sequence[Callable[[Any], None]]):
        var = self._register_provider.get_variable_from_definition(key_id)
        self.sanity_check(var.id)
        self._registered_callbacks[var.id] = callbacks
        
        await self._register_provider.subscribe_change(var.id, self._on_change_var)
        # print(f"Debug: Subscribed to changes on {var.key}")
            
        await self._register_provider.write_value(key_id, value)
        logger.info(f"Written value for '{key_id}': {value}")
                
    def sanity_check(self, id: int):
        if id not in self._variables:
            raise ValueError(f"Internal Error: Unexpected variable ID: {id}, expected one of: {self._variables.keys()}")
        
    def _on_change_var(self, id: int, snapshot: dict[int, VariableStateModel]):
        # Sanity check 
        self.sanity_check(id)
        value = snapshot[id].value
        if id in self._registered_callbacks:
            if self._registered_callbacks[id]:
                for callback in self._registered_callbacks[id]:
                    callback(value)
