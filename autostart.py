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

from building import Switch, Raffstore
            

async def async_main():
    # Simple push buttons    
    #await Switch(key_in="ur20_16di_p_1.process_data.channel_1.di", key_out="ur20_16do_p_1.process_data.channel_1.do").setup()
    #await Switch(key_in="ur20_16di_p_1.process_data.channel_0.di", key_out="ur20_16do_p_1.process_data.channel_0.do", on_time=5).setup()
    
    # Raffstore push buttons
    in_up="ur20_16di_p_1.process_data.channel_0.di"
    in_down="ur20_16di_p_1.process_data.channel_1.di"
    out_up="ur20_16do_p_1.process_data.channel_0.do"
    out_down="ur20_16do_p_1.process_data.channel_1.do"
    await Raffstore(in_up=in_up, in_down=in_down, out_up=out_up, out_down=out_down, run_time=10).setup()
    
    #in_up="ur20_16di_p_1.process_data.channel_2.di"
    in_up="ur20_16do_p_1.process_data.channel_0.do"
    in_down="ur20_16di_p_1.process_data.channel_3.di"
    out_up="ur20_16do_p_1.process_data.channel_2.do"
    out_down="ur20_16do_p_1.process_data.channel_3.do"
    await Raffstore(in_up=in_up, in_down=in_down, out_up=out_up, out_down=out_down, run_time=10).setup()

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