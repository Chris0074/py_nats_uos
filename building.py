from typing import Awaitable, Callable, Iterable, Tuple
from collections.abc import Iterable as AbcIterable
import sys
import pathlib
import asyncio
import inspect
import datetime
from zoneinfo import ZoneInfo

# Add the project root to Python path to enable absolute imports
ROOT_PATH = pathlib.Path(__file__).resolve().parent
if str(ROOT_PATH) not in sys.path:
    sys.path.insert(0, str(ROOT_PATH))


from login import CLIENT_ID, CLIENT_NAME, CLIENT_SECRET, NATS_HOST, PROVIDER_SBM
from src.data_hub import AccessProvider
from src.models import VariableInfo, VariableStateModel

class Building():
    _access_provider: AccessProvider | None = None
    _variables_names: dict[str, int] | None = None
    _variables_ids: dict[int, VariableInfo] | None = None
    _setup_finished = False
    
    def __init__(self, access_provider: AccessProvider | None = None):
        if Building._access_provider is None:
            #print("Init-Singleton carried out.")
            Building._access_provider = access_provider or AccessProvider(
                host=NATS_HOST,
                provider_id=PROVIDER_SBM,
            client_name=CLIENT_NAME,
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
            )
        self.access = Building._access_provider
        self.variables_names = Building._variables_names
        self.variables_ids = Building._variables_ids

    async def setup(self):
        if not Building._setup_finished:
            await self.access.connect()
            #print(f"Info: Connected to data hub.")
            Building._variables_names, Building._variables_ids = await self.access.get_definition()
            Building._setup_finished = True
        self.variables_names = Building._variables_names
        self.variables_ids = Building._variables_ids

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
            
        def set_timeout(self, timeout: float):
            self._timeout = timeout
                
        @property
        def is_running(self):
            return False if self._task is None else True


class CyclicTask:
    """Run one or more callbacks every day at a fixed time.

    Example:
        task = CyclicTask(hour=8, minute=0, callback=my_async_function)
        task.start()

        task = CyclicTask(hour=8, minute=0, callback=[task_a, task_b])
        task.start()
    """
    # Set a timezone
    time_zone = ZoneInfo("Europe/Vienna")

    def __init__(
        self,
        callback: Callable[[], None] | Iterable[Callable[[], None]],
        hour: int,
        minute: int = 0,
    ):
        if isinstance(callback, AbcIterable) and not callable(callback):
            self.callbacks = list(callback)
        else:
            self.callbacks = [callback]
        self.hour = hour
        self.minute = minute
        self._task: asyncio.Task | None = None
        self._cancelled = False
        
    def _now(self) -> datetime.datetime:
        return datetime.datetime.now(self.time_zone)

    def _next_run_time(self) -> datetime.datetime:
        now = self._now()
        next_run = now.replace(hour=self.hour, minute=self.minute, second=0, microsecond=0)
        if next_run <= now:
            next_run += datetime.timedelta(days=1)
        return next_run

    async def _run(self):
        try:
            while not self._cancelled:
                next_run = self._next_run_time()
                now = self._now()
                while next_run > now:
                    delay = (next_run - now).total_seconds()
                    # print(f"Debug: Cyclic task sleeping for {delay:.1f} seconds until next run at {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
                    # Sleeping is set to max 3600 seconds. If the next run is more than one hour away, the loop will wake up every hour 
                    # to check if the next run time has changed (e.g. due to daylight saving time changes).
                    await asyncio.sleep(min(3600, delay))
                    now = self._now()
                if self._cancelled:
                    break
                for callback in self.callbacks:
                    #print(f"Debug: Running cyclic callback {callback} at {self.now().strftime('%Y-%m-%d %H:%M:%S')}")
                    callback_result = callback()
                    if inspect.isawaitable(callback_result):
                        await callback_result
        except asyncio.CancelledError:
            pass
        finally:
            self._task = None

    def start(self):
        if self._task is not None:
            return
        self._cancelled = False
        self._task = asyncio.create_task(self._run())
        #'print(f"CyclicTask started, will run daily at {self.hour:02d}:{self.minute:02d}.")

    def stop(self):
        self._cancelled = True
        if self._task is not None:
            self._task.cancel()

    @property
    def is_running(self) -> bool:
        return self._task is not None


class Switch(Building):
    def __init__(self, key_in: str|Iterable[str], key_out: str|Iterable[str], on_time: int | None = None):
        super().__init__()
        self.key_in = [key_in,] if isinstance(key_in, str) else key_in
        self.key_out = [key_out,] if isinstance(key_out, str) else key_out
        self.on_time = on_time

        self._timer = None

    async def setup(self):
        await super().setup()
        for key in self.key_in:
            # print(f"Debug: Subscribed to changes on {self.key_in} with variable ID {self.require_variable_names()[key]}")
            await self.access.subscribe_change(key, self.on_change_di)
            
    def _verify_subscribed_ids(self, id: int, subscribed_keys: Iterable[str]):
        names = self.require_variable_names()
        ids = [names[key] for key in subscribed_keys]
        if id not in ids:
            raise ValueError(f"Internal Error: Unexpected variable ID: {id}, expected one of: {ids}")

    def sanity_check(self, id: int):
        self._verify_subscribed_ids(id, self.key_in)

    
    def _timer_callback(self):
        # print(f"Debug: Timer expired, set output {self.key_out} off.")
        for out in self.key_out:
            asyncio.create_task(self.access.write_value(out, False))

    def _timer_kill(self):
        if self._timer:
            self._timer.kill()
            self._timer = None

    def _set_value(self, value: bool):
        for out in self.key_out:
            asyncio.create_task(self.access.write_value(out, value))
            
        # Is an automatic off timer requested ?
        if self.on_time is not None and value is True:
            self._timer = self.Timer(timeout=self.on_time, callback=self._timer_callback)
            self._timer.start()

    async def on_change_di(self, id: int, snapshot: dict[int, VariableStateModel]):
        # Sanity check 
        self.sanity_check(id)
        # print(f"Debug: Input {self.key_in} changed to {snapshot[id].value}")
        if snapshot[id].value is True:
            self._timer_kill()
            self._set_value(True)
        else:
            if self._timer is None:
                self._set_value(False)

class Button(Building):
    def __init__(self, key_in: str|Iterable[str], key_out: str|Tuple[str], on_time: int | None = None):
        super().__init__()
        self.key_in: Iterable[str] = [key_in,] if isinstance(key_in, str) else key_in
        self.key_out: Tuple[str] = (key_out,) if isinstance(key_out, str) else key_out
        self.on_time = on_time

        self._timer = None

    async def setup(self):
        await super().setup()
        for key in self.key_in:
            # print(f"Debug: Subscribed to changes on {self.key_in} with variable ID {self.require_variable_names()[key]}")
            await self.access.subscribe_change(key, self.on_change_di)
            
    def _verify_subscribed_ids(self, id: int, subscribed_keys: Iterable[str]):
        names = self.require_variable_names()
        ids = [names[key] for key in subscribed_keys]
        if id not in ids:
            raise ValueError(f"Internal Error: Unexpected variable ID: {id}, expected one of: {ids}")

    def sanity_check(self, id: int):
        self._verify_subscribed_ids(id, self.key_in)

    
    def _timer_callback(self):
        # print(f"Debug: Timer expired, set output {self.key_out} off.")
        for out in self.key_out:
            asyncio.create_task(self.access.write_value(out, False))

    def _timer_kill(self):
        if self._timer:
            self._timer.kill()
            self._timer = None

    def _set_value(self, value: bool):
        for key in self.key_out:
            asyncio.create_task(self.access.write_value(key, value))
        # Is an automatic off timer requested ?
        if self.on_time is not None:
            if value is False:
                self._timer_kill()
            else:
                self._timer = self.Timer(timeout=self.on_time, callback=self._timer_callback)
                self._timer.start()

    async def on_change_di(self, id: int, snapshot: dict[int, VariableStateModel]):
        # Sanity check 
        self.sanity_check(id)
        # print(f"Debug: Input {self.key_in} changed to {in_value}")

        if snapshot[id].value is True:
            self._timer_kill()
            # Toogle output value
            variable_names = self.require_variable_names()
            out_value = snapshot[variable_names[self.key_out[0]]].value
            out_value = not out_value
            self._set_value(out_value)


class Raffstore(Building):
    def __init__(self, in_up: str|Iterable[str], in_down: str|Iterable[str], out_up: str, out_down: str, run_time: int = 50):
        super().__init__()
        self.in_up = in_up
        self.in_down = in_down
        self.out_up = out_up
        self.out_down = out_down
        self.run_sec = run_time
        self.short_run_sec = 1.5
        self.wait_sec = 0.1
        
        if isinstance(in_up, str):
            self.in_up = [in_up]
        if isinstance(in_down, str):
            self.in_down = [in_down]

     
        self._up_active = False
        self._down_active = False
        self._delay_timer = self.Timer(timeout=self.wait_sec)
        self._short_run_timer = self.Timer(timeout=self.short_run_sec)
        self._long_run_timer = self.Timer(timeout=self.run_sec)
        self._pulse_timer = self.Timer(timeout=0)

    async def setup(self):
        await super().setup()
        for up in self.in_up:
             #print(f"Debug: Subscribed to changes on {up}")
             await self.access.subscribe_change(up, self._on_change_up)
        for down in self.in_down:
             #print(f"Debug: Subscribed to changes on {down}")
             await self.access.subscribe_change(down, self._on_change_down)
             
    def up(self, time_in_sec: float):
        # To be on the save side switch off counterpart
        self._set_value_false(self.out_down)
        self._stop_active_task()
        self._up_active = True
        time_in_sec += self.wait_sec
        self._delay_timer.set_callback(function=lambda: self._set_value_true_pulse(self.out_up, time_in_sec))
        self._delay_timer.start()
        
    def down(self, time_in_sec: float):
        # To be on the save side switch off counterpart
        self._set_value_false(self.out_up)
        self._stop_active_task()
        self._down_active = True
        time_in_sec += self.wait_sec
        self._delay_timer.set_callback(function=lambda: self._set_value_true_pulse(self.out_down, time_in_sec))
        self._delay_timer.start()
             
    def up_long(self):
        # To be on the save side switch off counterpart
        self._set_value_false(self.out_down)
        self._up()
    
    def down_long(self):
        # To be on the save side switch off counterpart
        self._set_value_false(self.out_up)
        self._down()
        
    def _up(self):
        self._stop_active_task()
        self._up_active = True
        self._delay_timer.set_callback(function=lambda: self._set_value_true(self.out_up))
        self._delay_timer.start()
        
    def _down(self):
        self._stop_active_task()
        self._down_active = True
        self._delay_timer.set_callback(function=lambda: self._set_value_true(self.out_down))
        self._delay_timer.start()

    def _start_long_run_timer(self, key: str):
        self._short_run_timer.kill()
        self._long_run_timer.set_callback(function=lambda: self._set_value_false(key))
        self._long_run_timer.start()

    def _set_value_true(self, key: str):
        self._delay_timer.kill()
        asyncio.create_task(self.access.write_value(key, True))        
        self._short_run_timer.set_callback(function=lambda: self._start_long_run_timer(key))
        self._short_run_timer.start()

    # TODO: The call of this function can be replaced by "self._stop_active_task"
    def _set_value_false(self, key: str):
        asyncio.create_task(self.access.write_value(key, False))
        self._long_run_timer.kill()
        self._up_active = False
        self._down_active = False
        
    def _set_value_true_pulse(self, key: str, time_to_stop: float):
        self._delay_timer.kill()
        asyncio.create_task(self.access.write_value(key, True))
        self._pulse_timer.set_timeout(time_to_stop)        
        self._pulse_timer.set_callback(function=self._stop_active_task)
        self._pulse_timer.start()  
        
    def _stop_active_task(self):
        self._delay_timer.kill()
        self._short_run_timer.kill()                
        self._long_run_timer.kill()
        self._pulse_timer.kill()
        
        if self._up_active:            
            self._up_active = False
            asyncio.create_task(self.access.write_value(self.out_up, False))
        if self._down_active:                        
            asyncio.create_task(self.access.write_value(self.out_down, False))
            self._down_active = False
            
    def _verify_subscribed_ids(self, id: int, subscribed_keys: Iterable[str]):
        names = self.require_variable_names()
        ids = [names[key] for key in subscribed_keys]
        if id not in ids:
            raise ValueError(f"Internal Error: Unexpected variable ID: {id}, expected one of: {ids}")

    async def _on_change_up(self, id: int, snapshot: dict[int, VariableStateModel]):
        in_value = snapshot[id].value        
        variable_names = self.require_variable_names()
        # Sanity check 
        self._verify_subscribed_ids(id, self.in_up)
        # print(f"Debug: Input {self.in_up} changed to {in_value}")
        
        # To be on the save side switch off counterpart
        if snapshot[variable_names[self.out_down]].value is True:
            asyncio.create_task(self.access.write_value(self.out_down, False))

        out_value = snapshot[variable_names[self.out_up]].value
        if in_value is True and out_value is False:            
            self._up()            
        elif in_value is False and out_value is True:
            if self._short_run_timer.is_running:
                self._stop_active_task()
        elif in_value is True and out_value is True:
            pass
        elif in_value is False and out_value is False:
            # Raffstore is fully closed, reset timers and states
            self._stop_active_task()
        
    async def _on_change_down(self, id: int, snapshot: dict[int, VariableStateModel]):
        in_value = snapshot[id].value        
        variable_names = self.require_variable_names()
        # Sanity check 
        self._verify_subscribed_ids(id, self.in_down)
        # print(f"Debug: Input {self.in_down} changed to {in_value}")
        
        # To be on the save side switch off counterpart
        if snapshot[variable_names[self.out_up]].value is True:
            asyncio.create_task(self.access.write_value(self.out_up, False))

        out_value = snapshot[variable_names[self.out_down]].value
        if in_value is True and out_value is False:            
            self._down()
        elif in_value is False and out_value is True:
            if self._short_run_timer.is_running:
                self._stop_active_task()                
        elif in_value is True and out_value is True:
            pass
        elif in_value is False and out_value is False:
            # Raffstore is fully closed, reset timers and states
            self._stop_active_task()
