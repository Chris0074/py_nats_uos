from __future__ import annotations

import sys
import asyncio
import pathlib
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Add the project root to Python path to enable absolute imports
ROOT_PATH = pathlib.Path(__file__).resolve().parent
if str(ROOT_PATH) not in sys.path:
    sys.path.insert(0, str(ROOT_PATH))
SRC_PATH = ROOT_PATH.joinpath("src")
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from building import Switch, Raffstore, CyclicTask

from in_out import DI_3_0, DI_3_1, DI_3_2, DI_3_3, DI_3_4, DI_3_5, DI_3_6, DI_3_7, DI_3_8, DI_3_9, DI_3_10, DI_3_11, DI_3_12, DI_3_13, DI_3_14, DI_3_15
from in_out import DO_3_0, DO_3_1, DO_3_2, DO_3_3, DO_3_4, DO_3_5, DO_3_6, DO_3_7, DO_3_8, DO_3_9, DO_3_10, DO_3_11, DO_3_12, DO_3_13, DO_3_14, DO_3_15
from in_out import DI_1_0, DI_1_1, DI_1_2, DI_1_3, DI_1_4, DI_1_5, DI_1_6, DI_1_7, DI_1_8, DI_1_9, DI_1_10, DI_1_11, DI_1_12, DI_1_13, DI_1_14, DI_1_15

from in_out import DI_2_0, DI_2_1, DI_2_2, DI_2_3, DI_2_4, DI_2_5, DI_2_6, DI_2_7, DI_2_8, DI_2_9, DI_2_10, DI_2_11, DI_2_12, DI_2_13, DI_2_14, DI_2_15
from in_out import DO_2_0, DO_2_1, DO_2_2, DO_2_3, DO_2_4, DO_2_5, DO_2_6, DO_2_7, DO_2_8, DO_2_9, DO_2_10, DO_2_11, DO_2_12, DO_2_13, DO_2_14, DO_2_15

class RaffWrapper:
    def __init__(self, in_up, in_down, out_up, out_down, max_run_time=40, up_time=0.7):
        self.raffstore = Raffstore(in_up=in_up, in_down=in_down, out_up=out_up, out_down=out_down, run_time=max_run_time)
        self.up_time = up_time
        self.down_time = max_run_time
        
    async def setup(self) -> None:
        await self.raffstore.setup()
        
    def up(self) -> None:
        self.raffstore.up(time_in_sec=self.up_time)
        
    def down(self) -> None:
        self.raffstore.down(time_in_sec=self.down_time)


all_up = DI_1_8
all_down = DI_1_9 

async def async_main():
    # Simple push buttons    
    await Switch(key_in=DI_2_0, key_out=DO_2_0).setup()
    await Switch(key_in=DI_2_1, key_out=DO_2_1, on_time=5).setup()
    
    # Up/down time for cyclic timers
    raff_tilt_time = 0.9
    
    # Raffstore with two inputs, one for up and one for down. Both inputs are also used to raise/lower all raffstores at the same time.
    in_up = (DI_3_14, all_up)
    in_down = (DI_3_15, all_down)
    out_up = DO_3_14
    out_down = DO_3_15
    buero = RaffWrapper(in_up=in_up, in_down=in_down, out_up=out_up, out_down=out_down, up_time=raff_tilt_time)
    await buero.setup()

    # Raffstore with two inputs, one for up and one for down. Both inputs are also used to raise/lower all raffstores at the same time.
    in_up = (DI_3_12, all_up)
    in_down = (DI_3_13, all_down)
    out_up = DO_3_12
    out_down = DO_3_13
    keller_links = RaffWrapper(in_up=in_up, in_down=in_down, out_up=out_up, out_down=out_down, up_time=raff_tilt_time, max_run_time=60)
    await keller_links.setup()

    # Clyclic timer to raise all raffstores at 07:00
    callbacks_up = [buero.up, keller_links.up]
    CyclicTask(callback=callbacks_up, hour=7, minute=0).start()
    
    # Cyclic timer to lower all raffstores at 22:00    
    callback_down = [buero.down, keller_links.down]
    CyclicTask(callback=callback_down, hour=22, minute=0).start()

    try:
        while True:            
            await asyncio.sleep(10)
    except KeyboardInterrupt:
        logging.info("Stop...")
    finally:
        #await access_provider.close()
        pass

if __name__ == "__main__":
    asyncio.run(async_main())