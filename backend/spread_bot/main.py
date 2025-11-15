import sys, time
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

def update_live(symbolL, symbolE, text):
    """Keep only the latest live line for this symbol, formatted with newlines."""
    os.makedirs("spread_bot/logs", exist_ok=True)
    live_path = f"spread_bot/logs/{symbolL}_{symbolE}_live.txt"

    # Replace "|" with a newline for clearer formatting
    formatted = text.strip().replace("|", "\n")

    with _live_lock:
        with open(live_path, "w", encoding="utf-8") as f:
            f.write(formatted + "\n")

def clear_live(symbolL, symbolE):
    """Clear the live log file for this symbol pair."""
    
    os.makedirs("spread_bot/logs", exist_ok=True)
    live_path = f"spread_bot/logs/{symbolL}_{symbolE}_live.txt"

    with _live_lock:
        # Overwrite the file with an empty string
        with open(live_path, "w", encoding="utf-8") as f:
            f.write("--")  # or just `pass` if you want truly empty file

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

def setup_logger(symbolL, symbolE):
    os.makedirs("spread_bot/logs", exist_ok=True)
    log_path = f"spread_bot/logs/{symbolL}_{symbolE}.log"

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

    
async def restart_bot(symbolL, symbolE, reason):
    logging.info(f"🔁 Restarting bot for {symbolL}_{symbolE}...", )
    await send_telegram_message(f"⚠️ Restarting bot for symbol {symbolL}_{symbolE}... Reason: {reason}")
    await asyncio.sleep(1)
    os.execv(sys.executable, ['python3'] + sys.argv)

# Load config.json
def load_config():
    with open("spread_bot/config.json", "r") as f:
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

async def execute_trade(L, E, sideL, sideE, qty, tradeData, TRADES_INTERVAL):
    label                       = tradeData["direction"]
    logging.info                (f"✅ {label}: qty={qty}")
    L_AllSymInvValueBef         = L.invValue
    logL, logE = await asyncio.gather(
        L.placeMarketOrder(sideL, qty, label.startswith("Exit")),
        E.placeMarketOrder(sideE, qty, label.startswith("Exit"))
    )
    await asyncio.sleep         (TRADES_INTERVAL)
    msg                         = await HELPERS.initInfo(L, E, tradeData, L_AllSymInvValueBef)
    await asyncio.gather        (L.loadPos(), E.loadPos())
    await asyncio.sleep         (1)
    L_AllSymInvValueAft         = L.invValue
    await HELPERS.sendInfo      (msg, L, E, L_AllSymInvValueAft, logL, logE)
    logging.info                (f"{label} Done ✅")


# ---------------------
_need_balancing             = False
_ts_since_need_balancing    = 0.0
_ts_since_last_action       = 0.0
_need_report_unbalanced     = False
_reducing_msg               = ''

async def balance_positions(L, E):
    """
    Returns True if balanced, False otherwise.
    """
    global _need_balancing, _ts_since_need_balancing, _ts_since_last_action, _need_report_unbalanced, _reducing_msg
    now                 = time.time()
    l_qty, e_qty, _, _  = calc_inv(L, E)
    net                 = l_qty + e_qty

    # already balanced
    if abs(net) < 1e-8:
        if _need_balancing:
            _need_balancing = False
            await send_tele_crit(
                f'✅ {L.pair["symbol"]} balanced again\n'
                f'L: {L.accountData["qty"]}  E: {E.accountData["qty"]}\n'
                f'(at {time.strftime("%d-%m-%Y %H:%M:%S")})'
            )
        return True

    # first detection : this will be executed if only not balanced
    if not _need_balancing:
        _need_balancing             = True
        _ts_since_need_balancing    = now
        logging.info                (f'⚠️ {L.pair["symbol"]} is UNBALANCED')
        return False 

    # cooldown: 60s since flagged, 60s since last action
    if (now - _ts_since_need_balancing >= 60.0) and (now - _ts_since_last_action >= 60.0):
        qty_need                    = abs(net)
        target                      = "L" if abs(l_qty) >= abs(e_qty) else "E"

        if target == "L":
            min_size                = L.pair.get("min_size") 
            if qty_need < min_size:
                logging.info('⚠️ {L.pair["symbol"]} BOT is STOPPED: balancing_qty is less than Lighter min_size')
                await send_tele_crit(
                    f'⚠️ {L.pair["symbol"]} BOT is STOPPED: balancing_qty is less than Lighter min_size\n'
                    f'Balance manually (reduce-only) then restart.'                    
                )
                sys.exit(1)

            side = "SELL" if l_qty > 0 else "BUY"
            try:
                await asyncio.gather        (L.cancelOrders(), E.cancelOrders())
                await L.placeMarketOrder    (side, float(qty_need), True)
                logging.info                (f'🟠 Rebalance L: {side} {qty_need:.8f}')
                _ts_since_last_action       = now
                _need_report_unbalanced     = True
                _reducing_msg               = f'✅ Reduced {qty_need:.8f} on Lighter'
            except Exception as e:
                logging.error(f'bal L error: {e}')
                await send_tele_crit(f'❌ bal L error: {e}')

        else:
            min_size                        = E.pair.get("min_size")
            if qty_need < min_size:
                logging.info                ('⚠️ {L.pair["symbol"]} BOT is STOPPED: balancing_qty is less than Lighter min_size')
                await send_tele_crit        (
                    f'⚠️ {L.pair["symbol"]} BOT is STOPPED: balancing_qty is less than Extended min_size\n'
                    f'Balance manually (reduce-only) then restart.'
                )
                sys.exit(1)

            side = "SELL" if e_qty > 0 else "BUY"
            try:
                await asyncio.gather        (L.cancelOrders(), E.cancelOrders())
                await E.placeMarketOrder(side, float(qty_need), True)
                logging.info                (f'🔵 Rebalance E: {side} {qty_need:.8f}' )
                _ts_since_last_action       = now
                _need_report_unbalanced     = True
                _reducing_msg               = f'✅ Reduced {qty_need:.8f} on Extended'
            except Exception as e:
                logging.error(f'bal E error: {e}')
                await send_tele_crit(f'❌ bal E error: {e}')

        # recheck after the nudge
        await asyncio.sleep(1)
        await asyncio.gather(L.loadPos(), E.loadPos())

    # while flagged, keep reporting unbalanced to the caller
    await asyncio.gather            (L.loadPos(), E.loadPos())
    l2, e2, _, _                    = calc_inv(L, E)
    if abs(l2 + e2) < 1e-8:
        _need_balancing             = False
        logging.info                (f'✅ {L.pair["symbol"]} balanced again')
        if not _need_report_unbalanced:
            await send_tele_crit    (f'⚠️ {L.pair["symbol"]} Order Fills Delayed')
        else:
            await send_tele_crit    (
                f'✅ Canceled All Open Orders\n'
                f'{_reducing_msg}\n'
                f'✅ {L.pair["symbol"]} is balanced again\n'
                f'it was not balanced since 60s ago\n'
                f'Current Inv [L: {L.accountData["qty"]}, E: {E.accountData["qty"]}]\n'
            )
            _need_report_unbalanced = False
        return True
    return False










    
def fmt_rate(a, b):
    if a is None or b is None:
        return "N/A"
    return f"{b - a:.6f}%"

def fmt_funding(rate):
    if rate is None:
        return "N/A"
    try:
        return f"{float(rate):.6f}%"
    except (ValueError, TypeError):
        return "N/A"


def printInfos(L, E, minSpread_toEntry):
    lbid, lszb, lask, lsza  = L.ob["bidPrice"], L.ob["bidSize"], L.ob["askPrice"], L.ob["askSize"]
    ebid, eszb, eask, esza  = E.ob["bidPrice"], E.ob["bidSize"], E.ob["askPrice"], E.ob["askSize"]
        
    l_qty, l_entry_price    = L .accountData["qty"], L .accountData["entry_price"]
    e_qty, e_entry_price    = E.accountData["qty"], E.accountData["entry_price"]

    invQty                  = abs(l_qty) if l_qty else 0

    spreadInv               = 0 
    if   l_qty>0 and e_qty<0:
        spreadInv           = (e_entry_price-l_entry_price)/l_entry_price*100 if l_entry_price not in [None, 0] else None
    elif l_qty<0 and e_qty>0:
        spreadInv           = (l_entry_price-e_entry_price)/e_entry_price*100 if e_entry_price not in [None, 0] else None

    dir                     = 'LE' if l_qty > 0 and e_qty < 0 else ('EL' if l_qty < 0 and e_qty > 0 else '')

    TRADES_INTERVAL         = cfg["TRADES_INTERVAL"]
    MIN_SPREAD              = cfg["MIN_SPREAD"]
    SPREAD_MULTIPLIER       = cfg["SPREAD_MULTIPLIER"]
    SPREAD_TP               = cfg["SPREAD_TP"]
    MIN_TRADE_VALUE         = cfg["MIN_TRADE_VALUE"]
    MAX_TRADE_VALUE_ENTRY   = cfg["MAX_TRADE_VALUE_ENTRY"]
    MAX_TRADE_VALUE_EXIT    = cfg["MAX_TRADE_VALUE_EXIT"]
    MAX_INVENTORY_VALUE     = cfg["MAX_INVENTORY_VALUE"]
    SPREAD_MULTIPLIER       = cfg["SPREAD_MULTIPLIER"]
    INV_LEVEL_TO_MULT       = cfg["INV_LEVEL_TO_MULT"]
    PERC_OF_OB              = cfg["PERC_OF_OB"] / 100

    spreadLE_TT             = (ebid - lask) / lask * 100 if ebid and lask else None
    spreadLE_TM             = (eask - lask) / lask * 100 if ebid and lask else None
    spreadLE_MT             = (ebid - lbid) / ebid * 100 if ebid and lask else None  
    
    spreadEL_TT             = (lbid - eask) / eask * 100 if lbid and eask else None
    spreadEL_TM             = (lask - eask) / eask * 100 if lbid and eask else None  
    spreadEL_MT             = (lbid - ebid) / lbid * 100 if lbid and eask else None

    # --- inventory levels table ---
    levels_text = ""
    if MAX_INVENTORY_VALUE > 0 and INV_LEVEL_TO_MULT > 0:
        INV_STEP_VALUE      = MAX_INVENTORY_VALUE / INV_LEVEL_TO_MULT
        levels_text         += "\n---\nSpread Averaging\nValue($) MinSpread(%)"
        for i in range(INV_LEVEL_TO_MULT):
            vol             = (i) * INV_STEP_VALUE
            spread          = MIN_SPREAD * (SPREAD_MULTIPLIER ** i)
            levels_text     += f"\n{vol:>7.0f} {spread:.2f}"
    else:
        levels_text         = "\n---\nBot isn't Looking for Entry"

    line = (
        f"---"
        f'|L:{L.pair["symbol"]} E:{E.pair["symbol"]}'
        f"|---"
        f"|Orderbook Data"
        f"|SpreadLE : [TT:{spreadLE_TT:.2f}%] [TM:{spreadLE_TM:.2f}%] [MT:{spreadLE_MT:.2f}%]"
        f"|SpreadEL : [TT:{spreadEL_TT:.2f}%] [TM:{spreadEL_TM:.2f}%] [MT:{spreadEL_MT:.2f}%]"
        f"|L        : {L.ob}"
        f"|E        : {E.ob}"
        f"|---"
        f"|Funding Rate"
        f"|Lighter  : {fmt_funding(L.currFundRate)}"
        f"|Extended : {fmt_funding(E.currFundRate)}"
        f"|Net LE   : {fmt_rate(L.currFundRate, E.currFundRate)}"
        f"|Net EL   : {fmt_rate(E.currFundRate, L.currFundRate)}"
        f"|---"
        f"|Inventory"
        f"|Δ        : {spreadInv:.2f}%"
        f"|Dir      : {dir}"
        f"|qtyL     : {l_qty} @ {l_entry_price} / ${(l_qty*l_entry_price):.2f}"
        f"|qtyE     : {e_qty} @ {e_entry_price} / ${(e_qty*e_entry_price):.2f}"
        f"{levels_text}"
        f"|---"
        f"|Configs"
        f"|TRADES_INTERVAL       : {TRADES_INTERVAL}s"
        f"|MIN_SPREAD            : {MIN_SPREAD}%"
        f"|SPREAD_MULTIPLIER     : {SPREAD_MULTIPLIER}"
        f"|SPREAD_TP             : {SPREAD_TP}%"
        f"|MIN_TRADE_VALUE       : ${MIN_TRADE_VALUE}"
        f"|MAX_TRADE_VALUE_ENTRY : ${MAX_TRADE_VALUE_ENTRY}"
        f"|MAX_TRADE_VALUE_EXIT  : ${MAX_TRADE_VALUE_EXIT}"
        f"|MAX_INVENTORY_VALUE   : ${MAX_INVENTORY_VALUE}"
        f"|INV_LEVEL_TO_MULT     : {INV_LEVEL_TO_MULT}"
        f"|PERC_OF_OB            : {PERC_OF_OB*100}%"
        f"|---"
        f"|DEX Configs"
        f'|Lighter               : min_size [{L.pair["min_size"]}], min_value [{L.pair["min_value"]}]'
        f'|Extended              : min_size [{E.pair["min_size"]}]'
        
        )

    update_live(L.pair["symbol"], E.pair["symbol"], line)

# --- Main Trading Loop ---
async def main(symbolL, symbolE, cfg):
    clear_live(symbolL, symbolE)

    TRADES_INTERVAL             = cfg["TRADES_INTERVAL"]
    MIN_SPREAD                  = cfg["MIN_SPREAD"]
    SPREAD_MULTIPLIER           = cfg["SPREAD_MULTIPLIER"]
    SPREAD_TP                   = cfg["SPREAD_TP"]
    MIN_TRADE_VALUE             = cfg["MIN_TRADE_VALUE"]
    MAX_TRADE_VALUE_ENTRY       = cfg["MAX_TRADE_VALUE_ENTRY"]
    MAX_TRADE_VALUE_EXIT        = cfg["MAX_TRADE_VALUE_EXIT"]
    MAX_INVENTORY_VALUE         = cfg["MAX_INVENTORY_VALUE"]
    INV_LEVEL_TO_MULT           = cfg["INV_LEVEL_TO_MULT"]
    PERC_OF_OB                  = cfg["PERC_OF_OB"] / 100

    logging.info                (f"🚀 Starting Bot for {symbolL}_{symbolE} ...")

    L, E                        = LighterAPI(symbolL), ExtendedAPI(symbolE)
    await asyncio.gather(L.init(), E.init())
    await asyncio.gather(L.initPair(), E.initPair())
    logging.info                ("✅ Both Exchange Initial is Done.")
    
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
    asyncio.create_task         (L.startWsFunding())
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


        # Calculate inventory spread
        spreadInv               = 0
        if l_qty > 0 and e_qty < 0:
            spreadInv           = (e_entry - l_entry) / l_entry * 100 if l_entry else 0
        elif l_qty < 0 and e_qty > 0:
            spreadInv           = (l_entry - e_entry) / e_entry * 100 if e_entry else 0

        inv_value               = max(l_inv_value, e_inv_value)
        INV_STEP_VALUE          = MAX_INVENTORY_VALUE / INV_LEVEL_TO_MULT if MAX_INVENTORY_VALUE > 0 else 0

        if MAX_INVENTORY_VALUE > 0 and INV_STEP_VALUE > 0:
            inv_level           = int(inv_value // INV_STEP_VALUE)
            minSpread_toEntry   = MIN_SPREAD * (SPREAD_MULTIPLIER ** inv_level)
        else:
            # reduce-only mode (no scaling)
            inv_level           = 0
            minSpread_toEntry   = MIN_SPREAD


        # minSpread_toEntry     = max(MIN_SPREAD, spreadInv*SPREAD_MULTIPLIER)
        printInfos(L, E, minSpread_toEntry)


        # Balance check
        balanced = await balance_positions(L, E)
        if not balanced:
            # still unbalanced → skip trading until user fixes it
            await asyncio.sleep(3)
            continue




        # --- ENTRY ---
        entryCond_LE            = ((l_qty >= 0 and e_qty <= 0) and 
                                 spreadLE > minSpread_toEntry and
                                 l_inv_value < MAX_INVENTORY_VALUE and 
                                 e_inv_value < MAX_INVENTORY_VALUE
        )
                
        entryCond_EL            = ((l_qty <= 0 and e_qty >= 0) and 
                                 spreadEL > minSpread_toEntry and 
                                 l_inv_value < MAX_INVENTORY_VALUE and 
                                 e_inv_value < MAX_INVENTORY_VALUE
        )
        
        # --- EXIT ---
        exitCond_fromLE         = l_qty > 0 and e_qty < 0 and spreadInv+spreadEL > SPREAD_TP
        exitCond_fromEL         = l_qty < 0 and e_qty > 0 and spreadInv+spreadLE > SPREAD_TP



        # --- TRADE EXECUTION ---
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
                        reason      += f"• Less Than Extended Minimum Trade Size\n"                        
                    if qty*L.ob["bidPrice"] < L.pair["min_value"]:
                        reason      += f"• Less Than Lighter Minimum Notional\n"
                    if qty < L.pair["min_size"]:
                        reason      += f"• Less Than Lighter Minimum Trade Size"
                        
                    msg_            = f'⚠️ {L.pair["symbol"]} Bot is Stopped..\n[exitCond_fromLE]\nReason: {reason}\nToDo: increase MIN_TRADE_VALUE'
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
                await execute_trade(L, E, "SELL", "BUY", qty, newTradeData, TRADES_INTERVAL)
                await asyncio.sleep(TRADES_INTERVAL)
                continue

            if not qty or qty <= 0:
                logging.warning(f"⚠️ [exitCond_fromLE] Calculated qty={qty} is zero or invalid. Restarting bot...")
                await restart_bot(symbolL, symbolE, 'Invalid trade quantity calculated in exitCond_fromLE')
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
                        reason      += f"• Less Than Extended Minimum Trade Size\n"
                    if qty*L.ob["askPrice"] < L.pair["min_value"]:
                        reason      += f"• Less Than Lighter Minimum Notional\n"
                    if qty < L.pair["min_size"]:
                        reason      += f"• Less Than Lighter Minimum Trade Size"

                    msg_            = f'⚠️ {L.pair["symbol"]} Bot is Stopped..\n[exitCond_fromEL]\nReason: {reason}\nToDo: increase MIN_TRADE_VALUE'
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
                await execute_trade(L, E, "BUY", "SELL", qty, newTradeData, TRADES_INTERVAL)
                continue

            if not qty or qty <= 0:
                logging.warning(f"⚠️ [exitConf_fromEL] Calculated qty={qty} is zero or invalid. Restarting bot...")
                await restart_bot(symbolL, symbolE, 'Invalid trade quantity calculated in exitCond_fromEL')
                return


        if entryCond_LE:
            qty                 = min(L.ob["askSize"]*PERC_OF_OB, E.ob["bidSize"]*PERC_OF_OB, MAX_TRADE_VALUE_ENTRY/L.ob["askPrice"])
            qty                 = HELPERS.extGetAllowedNum(qty, E.pair["min_size_change"])
            
            if qty and qty * L.ob["askPrice"] > MIN_TRADE_VALUE:

                if qty < E.pair["min_size"] or qty*L.ob["askPrice"] < L.pair["min_value"] or qty < L.pair["min_size"]:
                    reason          = ''
                    if qty < E.pair["min_size"]:
                        reason      += f"• Less Than Extended Minimum Trade Size\n"
                    if qty*L.ob["askPrice"] < L.pair["min_value"]:
                        reason      += f"• Less Than Lighter Minimum Notional\n"
                    if qty < L.pair["min_size"]:
                        reason      += f"• Less Than Lighter Minimum Trade Size"

                    msg_            = f'⚠️ {L.pair["symbol"]} Bot is Stopped..\n[entryCond_LE]\nReason: {reason}\nToDo: increase MIN_TRADE_VALUE'
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
                await execute_trade(L, E, "BUY", "SELL", qty, newTradeData, TRADES_INTERVAL)
                continue

            if not qty or qty <= 0:
                logging.warning(f"⚠️ [entryCond_LE] Calculated qty={qty} is zero or invalid. Restarting bot...")
                await restart_bot(symbolL, symbolE, 'Invalid trade quantity calculated in entryCond_LE')
                return

        if entryCond_EL:
            qty                 = min(E.ob["askSize"]*PERC_OF_OB, L.ob["bidSize"]*PERC_OF_OB, MAX_TRADE_VALUE_ENTRY/E.ob["askPrice"])
            qty                 = HELPERS.extGetAllowedNum(qty, E.pair["min_size_change"])

            if qty and qty * E.ob["askPrice"] > MIN_TRADE_VALUE:
                if qty < E.pair["min_size"] or qty*L.ob["bidPrice"] < L.pair["min_value"] or qty < L.pair["min_size"]:
                    reason          = ''
                    if qty < E.pair["min_size"]:
                        reason      += f"• Less Than Extended Minimum Trade Size\n"
                    if qty*L.ob["bidPrice"] < L.pair["min_value"]:
                        reason      += f"• Less Than Lighter Minimum Notional\n"
                    if qty < L.pair["min_size"]:
                        reason      += f"• Less Than Lighter Minimum Trade Size"
                    
                    msg_            = f'⚠️ {L.pair["symbol"]} Bot is Stopped..\n[entryCond_EL]\nReason: {reason}\nToDo: increase MIN_TRADE_VALUE'
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
                await execute_trade(L, E, "SELL", "BUY", qty, newTradeData, TRADES_INTERVAL)
                continue

            if not qty or qty <= 0:
                logging.warning(f"⚠️ [entryCond_EL] Calculated qty={qty} is zero or invalid. Restarting bot...")
                await restart_bot(symbolL, symbolE, 'Invalid trade quantity calculated in entryCond_EL')
                return



        await asyncio.sleep(0.1)


# --- Entry Point ---
if __name__ == "__main__":
    if len(sys.argv) > 1:
        symbolL             = sys.argv[1]
        symbolE             = sys.argv[2]
        setup_logger        (symbolL, symbolE)
        configs             = load_config()
        cfg                 = next((item for item in configs["symbols"] if item["SYMBOL_LIGHTER"] == symbolL), None)

        if cfg is None:
            logging.info(f"❌ Symbol {symbolL} not found in config.json")
            sys.exit(1)

        asyncio.run(main(symbolL, symbolE, cfg))

# ---