import asyncio
import logging
import os
from dotenv import load_dotenv
import json
import subprocess
import threading
from pathlib import Path
from db_ext.api import ExtendedAPI
from db_ext import p_fifo, p_cycle, p_daily

load_dotenv()
logger                          = logging.getLogger("db_ext.main")
logger.setLevel                 (logging.INFO)

async def processDbExt():
    L       = ExtendedAPI()
    await L.init()

    while True:
        try:
            await L.getTrades()
            L.split_trades_by_symbol()
            await L.getFundingPayment()

            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, p_fifo.process_all_fifo)
            await loop.run_in_executor(None, p_fifo.build_allSymbols)
            await loop.run_in_executor(None, p_daily.build_daily)

            await loop.run_in_executor(None, p_cycle.process_all_cycles)
            await loop.run_in_executor(None, p_cycle.build_allSymbols)

            logger.info("✅ Extended sync cycle complete.")
        except Exception as e:
            logger.error(f"⚠️ Sync error (Extended): {e}")
        await asyncio.sleep(60)

# --- Entry Point ---
if __name__ == "__main__":
    asyncio.run(main())