from __future__ import annotations

import sys
import asyncio
import inspect
import pathlib
from datetime import datetime

from typing import Iterable, Callable
from dataclasses import dataclass, field

# Add the project root to Python path to enable absolute imports
ROOT_PATH = pathlib.Path(__file__).resolve().parent
if str(ROOT_PATH) not in sys.path:
    sys.path.insert(0, str(ROOT_PATH))
SRC_PATH = ROOT_PATH.joinpath("src")
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from login import CLIENT_ID, CLIENT_NAME, CLIENT_SECRET, NATS_HOST, PROVIDER_SBM
from src.models import VariableStateModel
from building import building, Switch, Raffstore
            

async def async_main():    
    #await Switch(key_in="ur20_16di_p_1.process_data.channel_1.di", key_out="ur20_16do_p_1.process_data.channel_1.do").setup()
    #await Switch(key_in="ur20_16di_p_1.process_data.channel_0.di", key_out="ur20_16do_p_1.process_data.channel_0.do", on_time=5).setup()
    in_up="ur20_16di_p_1.process_data.channel_0.di"
    in_down="ur20_16di_p_1.process_data.channel_1.di"
    out_up="ur20_16do_p_1.process_data.channel_0.do"
    out_down="ur20_16do_p_1.process_data.channel_1.do"
    await Raffstore(in_up=in_up, in_down=in_down, out_up=out_up, out_down=out_down, run_time=10).setup()

    try:
        while True:            
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("Beenden...")
    finally:
        #await access_provider.close()
        pass

if __name__ == "__main__":
    asyncio.run(async_main())