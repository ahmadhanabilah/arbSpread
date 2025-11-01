import asyncio
from helper_lighter_web import LighterAPI
from helper_extended_web import ExtendedAPI
from telegram_api import send_telegram_message

import os, json, asyncio, logging, subprocess, csv, re
from telegram_api import send_telegram_message

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("unified_backend")


async def main():
    EXT = ExtendedAPI()
    LIG = LighterAPI()

    # ✅ initialize sequentially to be sure both clients ready
    await EXT.init()
    await LIG.init()

    # await LIG.getTrades()
    LIG.mergeTrades_lig()



if __name__ == "__main__":
    asyncio.run(main())