import sys
import asyncio
import logging
import os
from dotenv import load_dotenv
from helpers import HELPERS
from helper_lighter import LighterAPI
from helper_extended import ExtendedAPI
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
async def main(symbol, cfg):
    MIN_SPREAD              = cfg["MIN_SPREAD"]
    SPREAD_TP               = cfg["SPREAD_TP"]
    MIN_TRADE_VALUE         = cfg["MIN_TRADE_VALUE"]
    MAX_TRADE_VALUE_ENTRY   = cfg["MAX_TRADE_VALUE_ENTRY"]
    MAX_TRADE_VALUE_EXIT    = cfg["MAX_TRADE_VALUE_EXIT"]
    MAX_INVENTORY_VALUE     = cfg["MAX_INVENTORY_VALUE"]
    PERC_OF_OB              = cfg["PERC_OF_OB"] / 100
    checkSpreadInterval     = cfg["CHECK_SPREAD_INTERVAL"]

    logging.info            (f"🚀 Starting arbSpread_lighter_extended for {symbol} ...")

    L, E                    = LighterAPI(symbol), ExtendedAPI(symbol)
    await asyncio.gather(L.init(), E.init())
    await asyncio.gather(L.initPair(), E.initPair())
    await asyncio.gather(L.loadPos(), E.loadPos())

    # # Wait for all WS connections
    # ready                       = asyncio.Event()
    # def ws_callback(wsType):
    #     nonlocal ready
    #     ws_flags[wsType]        = True
    #     if all(ws_flags.values()):
    #         ready.set()
            
    # ws_flags                    = {"l_ob": False, "l_acc": False, "e_ob": False}
    # asyncio.create_task         (L.startWs(wsCallback=ws_callback))
    # asyncio.create_task         (L.startWsFunding())
    # asyncio.create_task         (E.startWs(wsCallback=ws_callback))
    # await ready.wait            ()
    # logging.info                ("✅ All WebSockets connected.")

    # await E.placeOrder(side="BUY",price=100000,qty=0.001)
    # await E.cancelOrders()
    
    # await L.placeOrder("BUY", 108000, 0.0005)
    await L.cancelOrders()
    

# --- Entry Point ---
if __name__ == "__main__":
    if len(sys.argv) > 1:
        symbol              = sys.argv[1]
        configs             = load_config()
        cfg                 = next((item for item in configs["symbols"] if item["symbol"] == symbol), None)

        if cfg is None:
            logging.info(f"❌ Symbol {symbol} not found in config.json")
            sys.exit(1)

        asyncio.run(main(symbol, cfg))
