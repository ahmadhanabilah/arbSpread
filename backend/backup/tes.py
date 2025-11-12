
import sys
import asyncio
import logging
import os
from dotenv import load_dotenv
from helpers import HELPERS
from helper_lighter_web import LighterAPI
from helper_extended_web import ExtendedAPI

from telegram_api import send_telegram_message, send_tele_crit
import json
import subprocess
import threading

# Load config.json
def load_config():
    with open("config.json", "r") as f:
        return json.load(f)

load_dotenv()
logging.basicConfig(level=logging.INFO)
logging.getLogger().setLevel(logging.INFO)

# --- Main Trading Loop ---
async def main():
    L, E            = LighterAPI(), ExtendedAPI()
    await asyncio.gather(L.init(), E.init())

    # await L.getRealPnl()
    # await L.getTrades()
    # L.aggregateTrades()
    await L.getFundingFee()
    # L.calculateDailyPnL()

    # await E.getPositionsHistory()
    # await E.getFundingPayment()
    


# --- Entry Point ---
if __name__ == "__main__":
    asyncio.run(main())