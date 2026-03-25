from typing import Awaitable, Callable
import sys
import pathlib
import asyncio
import inspect

# Add the project root to Python path to enable absolute imports
ROOT_PATH = pathlib.Path(__file__).resolve().parent
if str(ROOT_PATH) not in sys.path:
    sys.path.insert(0, str(ROOT_PATH))


from login import CLIENT_ID, CLIENT_NAME, CLIENT_SECRET, NATS_HOST, PROVIDER_SBM
from src.data_hub import AccessProvider
from src.models import VariableInfo, VariableStateModel

class Building():
    setup_done: bool = False

    def __init__(self, access_provider: AccessProvider | None = None):
        self.access: AccessProvider = access_provider or AccessProvider(
            host=NATS_HOST,
            provider_id=PROVIDER_SBM,
            client_name=CLIENT_NAME,
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
        )
        self.variables_names: dict[str, int] | None = None
        self.variables_ids: dict[int, VariableInfo] | None = None

    async def setup(self):
        if not Building.setup_done:
            await self.access.connect()
            #print(f"Info: Connected to data hub.")
            self.variables_names, self.variables_ids = await self.access.get_definition()
            Building.setup_done = True

    def require_variable_names(self) -> dict[str, int]:
        if self.variables_names is None:
            raise RuntimeError("Provider definition not loaded. Call setup() first.")
        return self.variables_names

    class Timer:
        def __init__(self, timeout: float, callback: Callable[[], None | Awaitable[None]] | None = None):
            self._timeout = timeout
            self._callback = callback
            self._task = None

        async def _run(self):
            try:
                await asyncio.sleep(self._timeout)
                # Only call the callback if not cancelled
                if self._callback:
                    callback_result = self._callback()
                    if inspect.isawaitable(callback_result):
                        await callback_result
                # print("Timer expired.")
            except asyncio.CancelledError:
                # Timer was killed, do nothing
                # print("Timer cancelled before expiration.")
                pass
            finally:
                self._task = None

        def start(self):
            self._task = asyncio.create_task(self._run())

        def kill(self):
            if self._task:
                # print("Killing timer...")
                self._task.cancel()
                
        def set_callback(self, function: Callable[[], None | Awaitable[None]]):
            self._callback = function
                
        @property
        def is_running(self):
            return False if self._task is None else True


building = Building()

class Switch:
    def __init__(self, key_in: str, key_out: str, on_time: int | None = None):
        self.key_in = key_in
        self.key_out = key_out
        self.on_time = on_time

        self._building = building
        self._timer = None

    async def setup(self):
        await self._building.setup()
        variable_names = self._building.require_variable_names()
        print(f"Debug: Subscribed to changes on {self.key_in} with variable ID {variable_names[self.key_in]}")
        await self._building.access.subscribe_change(self.key_in, self.on_change_di)

    def sanity_check(self, id: int):
        variable_names = self._building.require_variable_names()
        if id != variable_names[self.key_in]:
            raise ValueError(f"Internal Error: Unexpected variable ID: {id}, expected: {self.key_in}")

    
    def _timer_callback(self):
        # print(f"Debug: Timer expired, set output {self.key_out} off.")
        asyncio.create_task(self._building.access.write_value(self.key_out, False))

    def _timer_kill(self):
        if self._timer:
            self._timer.kill()
            self._timer = None

    def _set_value(self, value: bool):
        asyncio.create_task(self._building.access.write_value(self.key_out, value))
        # Is an automatic off timer requested ?
        if self.on_time is not None:
            if value is False:
                self._timer_kill()
            else:
                self._timer = self._building.Timer(timeout=self.on_time, callback=self._timer_callback)
                self._timer.start()

    async def on_change_di(self, id: int, snapshot: dict[int, VariableStateModel]):
        in_value = snapshot[id].value
        # Sanity check 
        self.sanity_check(id)
        print(f"Debug: Input {self .key_in} changed to {in_value}")

        if snapshot[id].value is True:
            self._timer_kill()
            # Toogle output value
            variable_names = self._building.require_variable_names()
            out_value = snapshot[variable_names[self.key_out]].value
            out_value = not out_value
            self._set_value(out_value)


class Raffstore:
    def __init__(self, in_up: str, in_down: str, out_up: str, out_down: str, run_time: int = 50):
        self.in_up = in_up
        self.in_down = in_down
        self.out_up = out_up
        self.out_down = out_down
        self.run_sec = run_time
        self.short_run_sec = 2
        self.wait_sec = 0.3

        self._building = building        
        self._up_active = False
        self._down_active = False
        self._delay_timer = self._building.Timer(timeout=self.wait_sec)
        self._short_run_timer = self._building.Timer(timeout=self.short_run_sec)
        self._long_run_timer = self._building.Timer(timeout=self.run_sec)

    async def setup(self):
        await self._building.setup()
        await self._building.access.subscribe_change(self.in_up, self.on_change_up)
        await self._building.access.subscribe_change(self.in_down, self.on_change_down)
        
    def _start_long_run_timer(self, key: str):
        self._short_run_timer.kill()
        self._long_run_timer.set_callback(function=lambda: self._set_value_false(key))
        self._long_run_timer.start()        

    def _set_value_true(self, key: str):
        self._delay_timer.kill()
        asyncio.create_task(self._building.access.write_value(key, True))        
        self._short_run_timer.set_callback(function=lambda: self._start_long_run_timer(key))
        self._short_run_timer.start()        

    def _set_value_false(self, key: str):
        asyncio.create_task(self._building.access.write_value(key, False))
        self._long_run_timer.kill()
        self._up_active = False
        self._down_active = False
        
    def _stop_active_task(self):
        self._delay_timer.kill()
        self._short_run_timer.kill()                
        self._long_run_timer.kill()            
        
        if self._up_active:            
            self._up_active = False
            asyncio.create_task(self._building.access.write_value(self.out_up, False))
        if self._down_active:                        
            asyncio.create_task(self._building.access.write_value(self.out_down, False))
            self._down_active = False        

    async def on_change_up(self, id: int, snapshot: dict[int, VariableStateModel]):
        in_value = snapshot[id].value        
        variable_names = self._building.require_variable_names()
        # Sanity check 
        if id != variable_names[self.in_up]:
            raise ValueError(f"Internal Error: Unexpected variable ID: {id}, expected: {self.in_up}")
        print(f"Debug: Input {self.in_up} changed to {in_value}")
        
        # To be on the save side switch off counterpart
        await self._building.access.write_value(self.out_down, False)

        out_value = snapshot[variable_names[self.out_up]].value
        if in_value is True and out_value is False:
            self._stop_active_task()
            self._up_active = True
            self._delay_timer.set_callback(function=lambda: self._set_value_true(self.out_up))
            self._delay_timer.start()            
        elif in_value is False and out_value is True:
            if self._short_run_timer.is_running:
                self._stop_active_task()
        elif in_value is True and out_value is True:
            pass
        elif in_value is False and out_value is False:
            # Raffstore is fully closed, reset timers and states
            self._stop_active_task()
            pass
        
    async def on_change_down(self, id: int, snapshot: dict[int, VariableStateModel]):
        in_value = snapshot[id].value        
        variable_names = self._building.require_variable_names()
        # Sanity check 
        if id != variable_names[self.in_down]:
            raise ValueError(f"Internal Error: Unexpected variable ID: {id}, expected: {self.in_down}")
        print(f"Debug: Input {self.in_down} changed to {in_value}")
        
        # To be on the save side switch off counterpart
        await self._building.access.write_value(self.out_up, False)

        out_value = snapshot[variable_names[self.out_down]].value
        if in_value is True and out_value is False:
            self._stop_active_task()
            self._down_active = True
            self._delay_timer.set_callback(function=lambda: self._set_value_true(self.out_down))
            self._delay_timer.start()
        elif in_value is False and out_value is True:
            if self._short_run_timer.is_running:
                self._stop_active_task()                
        elif in_value is True and out_value is True:
            pass
        elif in_value is False and out_value is False:
            # Raffstore is fully closed, reset timers and states
            self._stop_active_task()
