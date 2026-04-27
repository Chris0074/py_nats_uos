from __future__ import annotations

import sys
import asyncio
import pathlib

# Add the project root to Python path to enable absolute imports
ROOT_PATH = pathlib.Path(__file__).resolve().parent
if str(ROOT_PATH) not in sys.path:
    sys.path.insert(0, str(ROOT_PATH))
SRC_PATH = ROOT_PATH.joinpath("src")
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from building import Switch, Raffstore, CyclicTask

from in_out import DI_3_8, DI_3_9, DI_3_10, DI_3_11, DI_3_12, DI_3_13, DI_3_14, DI_3_15
from in_out import DO_3_8, DO_3_9, DO_3_10, DO_3_11, DO_3_12, DO_3_13, DO_3_14, DO_3_15
from in_out import DI_1_0, DI_1_1, DI_1_2, DI_1_3, DI_1_4, DI_1_5, DI_1_6, DI_1_7, DI_1_8, DI_1_9, DI_1_10, DI_1_11, DI_1_12, DI_1_13, DI_1_14, DI_1_15


all_up = DI_1_8
all_down = DI_1_9 

async def async_main():
    # Simple push buttons    
    # await Switch(key_in=DI_1_1, key_out=DO_1_1).setup()
    # await Switch(key_in=DI_1_0, key_out=DO_1_0, on_time=5).setup()
    # input = (DI_1_0,DI_1_2)
    # await Switch(key_in=input, key_out=DO_1_1).setup()
    
    # Raffstore push buttons
    up_time = 0.7
    down_time = 50
    
    # Raffstore with two inputs, one for up and one for down. Both inputs are also used to raise/lower all raffstores at the same time.
    in_up = (DI_3_14, all_up)
    in_down = (DI_3_15, all_down)
    out_up = DO_3_14
    out_down = DO_3_15
    buero = Raffstore(in_up=in_up, in_down=in_down, out_up=out_up, out_down=out_down, run_time=50)
    await buero.setup()
    def buero_up() -> None: buero.up(time_in_sec=up_time)
    def buero_down() -> None: buero.down(time_in_sec=down_time)

    # Raffstore with two inputs, one for up and one for down. Both inputs are also used to raise/lower all raffstores at the same time.
    in_up = (DI_3_8, all_up)
    in_down = (DI_3_9, all_down)
    out_up = DO_3_8
    out_down = DO_3_9
    kueche_ost = Raffstore(in_up=in_up, in_down=in_down, out_up=out_up, out_down=out_down, run_time=50)
    await kueche_ost.setup()
    def kueche_ost_up() -> None: kueche_ost.up(time_in_sec=up_time)
    def kueche_ost_down() -> None: kueche_ost.down(time_in_sec=down_time)

    # Clyclic timer to raise all raffstores at 8:00
    callbacks_up = [buero_up, kueche_ost_up]
    all_raff_up = CyclicTask(callback=callbacks_up, hour=8, minute=0)
    all_raff_up.start()
    
    # Cyclic timer to lower all raffstores at 22:00
    callback_down = [buero_down, kueche_ost_down]
    all_raff_down = CyclicTask(callback=callback_down, hour=22, minute=00)
    all_raff_down.start()

    try:
        while True:            
            await asyncio.sleep(10)
    except KeyboardInterrupt:
        print("Stop...")
    finally:
        #await access_provider.close()
        pass

if __name__ == "__main__":
    asyncio.run(async_main())