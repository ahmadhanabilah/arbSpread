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

_live_lock = threading.Lock()

def update_live(symbol, text):
    """Keep only the latest live line for this symbol, formatted with newlines."""
    os.makedirs("logs", exist_ok=True)
    live_path = f"logs/{symbol}_live.txt"

    # Replace "|" with a newline for clearer formatting
    formatted = text.strip().replace("|", "\n")

    with _live_lock:
        with open(live_path, "w", encoding="utf-8") as f:
            f.write(formatted + "\n")

class ReverseFileHandler(logging.FileHandler):
    """Custom handler that writes newest logs at the top of the file."""
    def emit(self, record):
        msg = self.format(record)
        try:
            # Read old content (if exists)
            if os.path.exists(self.baseFilename):
                with open(self.baseFilename, "r", encoding="utf-8") as f:
                    old = f.read()
            else:
                old = ""
            # Write new message first, then the old logs
            with open(self.baseFilename, "w", encoding="utf-8") as f:
                f.write(msg + "\n" + old)
        except Exception:
            self.handleError(record)

def setup_logger(symbol):
    os.makedirs("logs", exist_ok=True)
    log_path = f"logs/{symbol}.log"

    # Remove existing handlers
    for h in logging.root.handlers[:]:
        logging.root.removeHandler(h)

    # Configure logger
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            ReverseFileHandler(log_path),
            logging.StreamHandler(sys.stdout)
        ]
    )

    logging.getLogger().info(f"✅ Logger initialized for {symbol}, newest logs appear on top.")

    
async def restart_bot(symbol, reason):
    logging.info("🔁 Restarting bot for symbol %s...", symbol)
    await send_telegram_message(f"⚠️ Restarting bot for symbol {symbol}... Reason:{reason}")
    await asyncio.sleep(1)
    os.execv(sys.executable, ['python3'] + sys.argv)

# Load config.json
def load_config():
    with open("config.json", "r") as f:
        return json.load(f)

load_dotenv()
logging.basicConfig(level=logging.INFO)
logging.getLogger().setLevel(logging.INFO)

# --- Utility Functions ---
def calc_spreads(L, E):
    lbid, lask                  = L.ob["bidPrice"], L.ob["askPrice"]
    ebid, eask                  = E.ob["bidPrice"], E.ob["askPrice"]
    spreadLE                    = (ebid - lask) / lask * 100 if ebid and lask else None
    spreadEL                    = (lbid - eask) / eask * 100 if lbid and eask else None
    return spreadLE, spreadEL

def calc_inv(L, E):
    l_qty, l_entry              = L.accountData["qty"], L.accountData["entry_price"]
    e_qty, e_entry              = E.accountData["qty"], E.accountData["entry_price"]
    return l_qty, e_qty, l_entry, e_entry

async def execute_trade(L, E, sideL, sideE, qty, tradeData):
    label                       = tradeData["direction"]
    logging.info                (f"✅ {label}: qty={qty}")
    L_AllSymInvValueBef         = L.invValue
    logL, logE = await asyncio.gather(
        L.placeMarketOrder(sideL, qty, label.startswith("Exit")),
        E.placeMarketOrder(sideE, qty, label.startswith("Exit"))
    )
    await asyncio.sleep(1)
    msg                         = await HELPERS.initInfo(L, E, tradeData, L_AllSymInvValueBef)
    await asyncio.gather        (L.loadPos(), E.loadPos())
    await asyncio.sleep         (1)
    L_AllSymInvValueAft         = L.invValue
    await HELPERS.sendInfo      (msg, L, E, L_AllSymInvValueAft, logL, logE)
    logging.info                (f"{label} Done ✅")


async def balance_positions(L, E):
    await asyncio.gather                (L.loadPos(), E.loadPos())
    await asyncio.sleep                 (1)
    l_qty, e_qty, l_entry, e_entry      = calc_inv(L, E)

    # Already balanced (within tolerance)
    if abs(l_qty + e_qty) < 1e-8:
        return False

    msg = f"⚖️ {E.pair["symbol"]} Balancing positions...\nL={l_qty}, E={e_qty}"

    # Decide which side has larger absolute position
    if abs(l_qty) > abs(e_qty):
        # Lighter has bigger position → reduce L
        qty                     = abs(l_qty + e_qty)

        if qty < E.pair["min_size"]:
            msg_                = f"Balancing Paused...\nCalculated qty {qty} is less than min_size {E.pair['min_size']}\nBot Will Sleep 5mins..\nBalance your position manually!!"
            logging.info        (msg_)
            await send_tele_crit(msg_)            
            await asyncio.sleep (300)
            return

        if l_qty > 0:
            await HELPERS.safePlaceOrder(L, "SELL", qty, True)
            msg                 += f"\n🟠 Reduced L long by {qty}"
        else:
            await HELPERS.safePlaceOrder(L, "BUY" , qty, True)
            msg                 += f"\n🟠 Reduced L short by {qty}"

    else:
        qty                     = abs(l_qty + e_qty)
        if qty < E.pair["min_size"]:
            msg_                = f"Balancing Paused...\nCalculated qty {qty} is less than min_size {E.pair['min_size']}\nBot Will Sleep 5mins..\nBalance your position manually!!"
            logging.info        (msg_)
            await send_tele_crit(msg_)            
            await asyncio.sleep (300)
            return

        if e_qty > 0:
            await HELPERS.safePlaceOrder(E, "SELL", qty, True)
            msg                 += f"\n🔵 Reduced E long by {qty}"
        else:
            await HELPERS.safePlaceOrder(E, "BUY" , qty, True)
            msg                 += f"\n🔵 Reduced E short by {qty}"

    await asyncio.sleep         (10)
    await asyncio.gather        (L.loadPos(), E.loadPos())    
    msg                         = msg + f"\n✅ Balancing done. \n\nInventory: \nL:{L.accountData["qty"]}, E:{E.accountData["qty"]}, check manually!!\nBot Will Sleep 5mins.."

    await send_tele_crit(msg_)            
    await asyncio.sleep         (300)
    
def fmt_rate(a, b):
    if a is None or b is None:
        return "N/A"
    return f"{b - a:.6f}%"

def printInfos(L, E, spreadLE, spreadEL, symbol=None):
    lbid, lszb, lask, lsza  = L.ob["bidPrice"], L.ob["bidSize"], L.ob["askPrice"], L.ob["askSize"]
    ebid, eszb, eask, esza  = E.ob["bidPrice"], E.ob["bidSize"], E.ob["askPrice"], E.ob["askSize"]
        
    l_qty, l_entry_price    = L .accountData["qty"], L .accountData["entry_price"]
    e_qty, e_entry_price    = E.accountData["qty"], E.accountData["entry_price"]

    invQty                  = abs(l_qty) if l_qty else 0

    if spreadLE is None or spreadEL is None:
        # print("--------------------------------------------------------------")
        return

    spreadInv               = 0 
    if   l_qty>0 and e_qty<0:
        spreadInv           = (e_entry_price-l_entry_price)/l_entry_price*100 if l_entry_price not in [None, 0] else None
    elif l_qty<0 and e_qty>0:
        spreadInv           = (l_entry_price-e_entry_price)/e_entry_price*100 if e_entry_price not in [None, 0] else None

    dir                     = 'LE' if l_qty > 0 and e_qty < 0 else ('EL' if l_qty < 0 and e_qty > 0 else '')

    MIN_SPREAD              = cfg["MIN_SPREAD"]
    SPREAD_TP               = cfg["SPREAD_TP"]
    MIN_TRADE_VALUE         = cfg["MIN_TRADE_VALUE"]
    MAX_TRADE_VALUE_ENTRY   = cfg["MAX_TRADE_VALUE_ENTRY"]
    MAX_TRADE_VALUE_EXIT    = cfg["MAX_TRADE_VALUE_EXIT"]
    MAX_INVENTORY_VALUE     = cfg["MAX_INVENTORY_VALUE"]
    PERC_OF_OB              = cfg["PERC_OF_OB"] / 100
    checkSpreadInterval     = cfg["CHECK_SPREAD_INTERVAL"]

    line = (
        f"---"
        f'|{L.pair["symbol"]}'
        f"|---"
        f"|Orderbook Data (Based on Taker Prices)"
        f"|SpreadLE: {spreadLE:.2f}%"
        f"|SpreadEL: {spreadEL:.2f}%"
        f"|---"
        f"|Funding Rate"
        f"|Net LE  : {fmt_rate(L.currFundRate, E.currFundRate)}"
        f"|Net EL  : {fmt_rate(E.currFundRate, L.currFundRate)}"
        f"|---"
        f"|Inventory"
        f"|Δ       : {spreadInv:.2f}"
        f"|Dir     : {dir}"
        f"|qtyL    : {l_qty} @ {l_entry_price} / ${(l_qty*l_entry_price):.2f}"
        f"|qtyE    : {e_qty} @ {e_entry_price} / ${(e_qty*e_entry_price):.2f}"
        f"|---"
        f"|Config"
        f"|MIN_SPREAD: {MIN_SPREAD}%"
        f"|SPREAD_TP : {SPREAD_TP}%"
        f"|MIN_TRADE_VALUE       : ${MIN_TRADE_VALUE}"
        f"|MAX_TRADE_VALUE_ENTRY : ${MAX_TRADE_VALUE_ENTRY}"
        f"|MAX_TRADE_VALUE_EXIT  : ${MAX_TRADE_VALUE_EXIT}"
        f"|MAX_INVENTORY_VALUE   : ${MAX_INVENTORY_VALUE}"
        f"|PERC_OF_OB            : {PERC_OF_OB*100}%"
        f"|CHECK_SPREAD_INTERVAL : {checkSpreadInterval}s"
        )

    update_live(symbol, line)

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

    # Wait for all WS connections
    ready                       = asyncio.Event()
    def ws_callback(wsType):
        nonlocal ready
        ws_flags[wsType]        = True
        if all(ws_flags.values()):
            ready.set()
            
    ws_flags                    = {"l_ob": False, "l_acc": False, "e_ob": False}
    asyncio.create_task         (L.startWs(wsCallback=ws_callback))
    # asyncio.create_task         (L.startWsFunding())
    asyncio.create_task         (E.startWs(wsCallback=ws_callback))
    await ready.wait            ()
    logging.info                ("✅ All WebSockets connected.")


    # CheckSpreadLoop
    while True:
        spreadLE, spreadEL              = calc_spreads(L, E)
        if spreadLE is None or spreadEL is None:
            await asyncio.sleep(0.1)
            continue

        l_qty, e_qty, l_entry, e_entry  = calc_inv(L, E)
        l_inv_value                     = abs(l_qty) * l_entry  
        e_inv_value                     = abs(e_qty) * e_entry

        # Balance check
        if l_qty + e_qty != 0:
            await balance_positions(L, E)
            continue

        printInfos(L, E, spreadLE, spreadEL, symbol)

        # --- ENTRY ---
        entryCond_LE            = (l_qty >= 0 and e_qty <= 0) and spreadLE > MIN_SPREAD and l_inv_value < MAX_INVENTORY_VALUE and e_inv_value < MAX_INVENTORY_VALUE 
        entryCond_EL            = (l_qty <= 0 and e_qty >= 0) and spreadEL > MIN_SPREAD and l_inv_value < MAX_INVENTORY_VALUE and e_inv_value < MAX_INVENTORY_VALUE
        
        # --- EXIT ---
        spreadInv               = 0
        if l_qty > 0 and e_qty < 0:
            spreadInv           = (e_entry - l_entry) / l_entry * 100 if l_entry else 0
        elif l_qty < 0 and e_qty > 0:
            spreadInv           = (l_entry - e_entry) / e_entry * 100 if e_entry else 0

        exitCond_fromLE         = l_qty > 0 and e_qty < 0 and spreadInv+spreadEL > SPREAD_TP
        exitCond_fromEL         = l_qty < 0 and e_qty > 0 and spreadInv+spreadLE > SPREAD_TP

        if entryCond_LE:
            qty                 = min(L.ob["askSize"]*PERC_OF_OB, E.ob["bidSize"]*PERC_OF_OB, MAX_TRADE_VALUE_ENTRY/L.ob["askPrice"])
            qty                 = HELPERS.extGetAllowedNum(qty, E.pair["min_size_change"])
            
            if qty and qty * L.ob["askPrice"] > MIN_TRADE_VALUE:

                if qty < E.pair["min_size"] or qty*L.ob["askPrice"] < L.pair["min_value"] or qty < L.pair["min_size"]:
                    reason          = ''
                    if qty < E.pair["min_size"]:
                        reason      += f"qty {qty} < E.min_size {E.pair['min_size']}. "
                    if qty*L.ob["askPrice"] < L.pair["min_value"]:
                        reason      += f"qty*L.askPrice {qty*L.ob['askPrice']:.2f} < L.min_value {L.pair['min_value']}. "
                    if qty < L.pair["min_size"]:
                        reason      += f"qty {qty} < L.min_size {L.pair['min_size']}. "

                    msg_            = f"Stopping Bot..\n[entryCond_LE]\nReason:{reason}\nToDo: increase MIN_TRADE_VALUE"
                    logging.info    (msg_)
                    await send_tele_crit (msg_)       
                    await asyncio.sleep(1)         
                    sys.exit(1)
                
                logging.info(f'entryCond_LE MET')
            
                newTradeData    = {
                    "spread"    : spreadLE,
                    "direction" : 'Entry-LE',
                    "qty"       : qty,
                    "value"     : qty * L.ob["askPrice"],
                    "askPrice"  : L.ob["askPrice"],
                    "askSize"   : L.ob["askSize"],
                    "bidPrice"  : E.ob["bidPrice"],
                    "bidSize"   : E.ob["bidSize"],
                }
                await execute_trade(L, E, "BUY", "SELL", qty, newTradeData)
                await asyncio.sleep(checkSpreadInterval)
                continue

            if not qty or qty <= 0:
                logging.warning(f"⚠️ [entryCond_LE] Calculated qty={qty} is zero or invalid. Restarting bot...")
                await restart_bot(symbol, 'Invalid trade quantity calculated in entryCond_LE')
                return

        if entryCond_EL:
            qty                 = min(E.ob["askSize"]*PERC_OF_OB, L.ob["bidSize"]*PERC_OF_OB, MAX_TRADE_VALUE_ENTRY/E.ob["askPrice"])
            qty                 = HELPERS.extGetAllowedNum(qty, E.pair["min_size_change"])

            if qty and qty * E.ob["askPrice"] > MIN_TRADE_VALUE:
                if qty < E.pair["min_size"] or qty*L.ob["bidPrice"] < L.pair["min_value"] or qty < L.pair["min_size"]:
                    reason          = ''
                    if qty < E.pair["min_size"]:
                        reason      += f"qty {qty} < E.min_size {E.pair['min_size']}. "
                    if qty*L.ob["bidPrice"] < L.pair["min_value"]:
                        reason      += f"qty*L.bidPrice {qty*L.ob['bidPrice']:.2f} < L.min_value {L.pair['min_value']}. "
                    if qty < L.pair["min_size"]:
                        reason      += f"qty {qty} < L.min_size {L.pair['min_size']}. "
                    
                    msg_            = f"Stopping Bot..\n[entryCond_EL]\nReason:{reason}\nToDo: increase MIN_TRADE_VALUE"
                    logging.info    (msg_)
                    await send_tele_crit (msg_)                
                    await asyncio.sleep(1)         
                    sys.exit(1)

                logging.info(f'entryCond_EL MET')
            
                newTradeData    = {
                    "spread"    : spreadEL,
                    "direction" : 'Entry-EL',
                    "qty"       : qty,
                    "value"     : qty * E.ob["askPrice"],
                    "askPrice"  : E.ob["askPrice"],
                    "askSize"   : E.ob["askSize"],
                    "bidPrice"  : L.ob["bidPrice"],
                    "bidSize"   : L.ob["bidSize"],
                }
                await execute_trade(L, E, "SELL", "BUY", qty, newTradeData)
                await asyncio.sleep(checkSpreadInterval)
                continue

            if not qty or qty <= 0:
                logging.warning(f"⚠️ [entryCond_EL] Calculated qty={qty} is zero or invalid. Restarting bot...")
                await restart_bot(symbol, 'Invalid trade quantity calculated in entryCond_EL')
                return

        if exitCond_fromLE:
            qty                 = min(E.ob["askSize"]*PERC_OF_OB, L.ob["bidSize"]*PERC_OF_OB, MAX_TRADE_VALUE_EXIT/E.ob["askPrice"])
            qty                 = HELPERS.extGetAllowedNum(qty, E.pair["min_size_change"])
            qtyInv              = abs(l_qty)
            remaining_value     = (qtyInv - qty) * E.ob["askPrice"]

            if remaining_value < MIN_TRADE_VALUE:
                qty             = qtyInv

            if qty and qty * E.ob["askPrice"] > MIN_TRADE_VALUE:
                if qty < E.pair["min_size"] or qty*L.ob["bidPrice"] < L.pair["min_value"] or qty < L.pair["min_size"]:
                    reason          = ''
                    if qty < E.pair["min_size"]:
                        reason      += f"qty {qty} < E.min_size {E.pair['min_size']}. "
                    if qty*L.ob["bidPrice"] < L.pair["min_value"]:
                        reason      += f"qty*L.bidPrice {qty*L.ob['bidPrice']:.2f} < L.min_value {L.pair['min_value']}. "
                    if qty < L.pair["min_size"]:
                        reason      += f"qty {qty} < L.min_size {L.pair['min_size']}. "
                        
                    msg_            = f"Stopping Bot..\n[exitCond_fromLE]\nReason:{reason}\nToDo: increase MIN_TRADE_VALUE"
                    logging.info    (msg_)
                    await send_tele_crit (msg_)                
                    await asyncio.sleep(1)         
                    sys.exit(1)
                    
                logging.info(f'exitCond_fromLE MET')

                newTradeData    = {
                    "spread"    : spreadEL,
                    "direction" : 'Exit-fromLE-withEL',
                    "qty"       : qty,
                    "value"     : qty * E.ob["askPrice"],
                    "askPrice"  : E.ob["askPrice"],
                    "askSize"   : E.ob["askSize"],
                    "bidPrice"  : L.ob["bidPrice"],
                    "bidSize"   : L.ob["bidSize"],
                }
                await execute_trade(L, E, "SELL", "BUY", qty, newTradeData)
                await asyncio.sleep(checkSpreadInterval)
                continue

            if not qty or qty <= 0:
                logging.warning(f"⚠️ [exitCond_fromLE] Calculated qty={qty} is zero or invalid. Restarting bot...")
                await restart_bot(symbol, 'Invalid trade quantity calculated in exitCond_fromLE')
                return


        if exitCond_fromEL:
            qty                 = min(L.ob["askSize"]*PERC_OF_OB, E.ob["bidSize"]*PERC_OF_OB, MAX_TRADE_VALUE_EXIT/L.ob["askPrice"])
            qty                 = HELPERS.extGetAllowedNum(qty, E.pair["min_size_change"])
            qtyInv              = abs(l_qty)
            remaining_value     = (qtyInv - qty) * L.ob["askPrice"]

            if remaining_value < MIN_TRADE_VALUE:
                qty             = qtyInv

            if qty and qty * L.ob["askPrice"] > MIN_TRADE_VALUE:
                if qty < E.pair["min_size"] or qty*L.ob["askPrice"] < L.pair["min_value"] or qty < L.pair["min_size"]:
                    reason          = ''
                    if qty < E.pair["min_size"]:
                        reason      += f"qty {qty} < E.min_size {E.pair['min_size']}. "
                    if qty*L.ob["askPrice"] < L.pair["min_value"]:
                        reason      += f"qty*L.askPrice {qty*L.ob['askPrice']:.2f} < L.min_value {L.pair['min_value']}. "
                    if qty < L.pair["min_size"]:
                        reason      += f"qty {qty} < L.min_size {L.pair['min_size']}. "
                    msg_            = f"[entryCond_EL] Calculated qty {qty} is less than min_size {E.pair['min_size']}, stopping bot..\n todo: increase MIN_TRADE_VALUE"
                    logging.info    (msg_)
                    await send_tele_crit (msg_)                
                    await asyncio.sleep(1)         
                    sys.exit(1)
                    
                logging.info(f'exitCond_fromEL MET')

                newTradeData    = {
                    "spread"    : spreadLE,
                    "direction" : 'Exit-fromEL-withLE',
                    "qty"       : qty,
                    "value"     : qty * L.ob["askPrice"],
                    "askPrice"  : L.ob["askPrice"],
                    "askSize"   : L.ob["askSize"],
                    "bidPrice"  : E.ob["bidPrice"],
                    "bidSize"   : E.ob["bidSize"],
                }
                await execute_trade(L, E, "BUY", "SELL", qty, newTradeData)
                await asyncio.sleep(checkSpreadInterval)
                continue

            if not qty or qty <= 0:
                logging.warning(f"⚠️ [exitConf_fromEL] Calculated qty={qty} is zero or invalid. Restarting bot...")
                await restart_bot(symbol, 'Invalid trade quantity calculated in exitCond_fromEL')
                return


        await asyncio.sleep(checkSpreadInterval)


# --- Entry Point ---
if __name__ == "__main__":
    if len(sys.argv) > 1:
        symbol              = sys.argv[1]
        setup_logger        (symbol)
        configs             = load_config()
        cfg                 = next((item for item in configs["symbols"] if item["symbol"] == symbol), None)

        if cfg is None:
            logging.info(f"❌ Symbol {symbol} not found in config.json")
            sys.exit(1)

        asyncio.run(main(symbol, cfg))
