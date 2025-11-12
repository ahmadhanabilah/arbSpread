import asyncio
from telegram_api import send_telegram_message 
import logging
import traceback
from datetime import datetime
import os

logger                          = logging.getLogger("helpers")
logger.setLevel                 (logging.INFO)

def quantize_by_increment(value: float, increment: float) -> float:
    if value is None or increment is None or increment == 0:
        return 0.0
    str_inc             = f"{increment:.10f}".rstrip('0')  # remove trailing zeros
    if '.' in str_inc:
        decimals        = len(str_inc.split('.')[1])
    else:
        decimals        = 0
    return round((value // increment) * increment, decimals)

from decimal import Decimal

def fmtNumInv(val, MIN_CHANGE):
    if val is None:
        return "N/A"
    d = Decimal(str(MIN_CHANGE))   # keep scientific notation intact
    size_decimals = max(0, -d.as_tuple().exponent)
    return f"{val:.{size_decimals}f}"

def fmt_spread_inv(val):
    return f"{val:.2f}%" if val is not None else "N/A"

class HELPERS:
    @staticmethod
    def extGetAllowedNum(qty:float, MIN_CHANGE) -> float:
        fixNum          = quantize_by_increment(qty, MIN_CHANGE)
        return fixNum

    @staticmethod
    def lighterFmtDecimal(val, DECIMAL_NUMBER):
        return int(val * 10 ** DECIMAL_NUMBER)

    @staticmethod
    def extendedFmtDecimal(val, DECIMAL_NUMBER: int) -> str:
        formatted = f"{val:.{DECIMAL_NUMBER}f}"
        if DECIMAL_NUMBER == 0:
            return formatted.split(".")[0]
        return formatted
    
    @staticmethod
    async def safePlaceOrder(api, side, qty, isReduceOnly, retries=5, delay=0.1):
        for attempt in range(1, retries + 1):
            try:
                result              = await api.placeMarketOrder(side, qty, isReduceOnly)
                if result:
                    return result
                else:
                    raise Exception ("Empty response from placeMarketOrder")
            except Exception as e:
                logging.error       (f"[{api.__class__.__name__}] Order attempt {attempt}/{retries} failed: {e}")
                if attempt < retries:
                    await asyncio.sleep(delay)
                else:
                    logging.error(f"❌ All retries failed for {side} {qty}")
                    return None

    async def initInfo(LIGHTER_API, EXTENDED_API, newTradeData, L_AllSymInvValue):
        extPair                 = EXTENDED_API.pair
        l_qty, l_entry_price    = LIGHTER_API .accountData["qty"], LIGHTER_API .accountData["entry_price"]
        e_qty, e_entry_price    = EXTENDED_API.accountData["qty"], EXTENDED_API.accountData["entry_price"]

        inventory               = {"dir":"", "spread":0}
        if l_qty > 0 and e_qty < 0:
            inventory["dir"]    = "LE"
            inventory["spread"] = (e_entry_price - l_entry_price) / l_entry_price  * 100
        if l_qty < 0 and e_qty > 0:
            inventory["dir"]    = "EL"
            inventory["spread"] = (l_entry_price - e_entry_price) / e_entry_price * 100
        if l_qty == 0 and e_qty == 0:
            inventory["dir"]    = "-"
            inventory["spread"] = 0
                        
        msg = (
            f"💵 {extPair['symbol']}\n"
            f"✅ {newTradeData['direction']}\n"
            f"📌 Qty:{newTradeData['qty']} | Value:${newTradeData['value']:.2f}\n\n"
            
            f"📘 Orderbook Data (Buy:Ask, Sell:Bid)\n"
            f"📌 BidPrice:{newTradeData['bidPrice']}, BidSize:{newTradeData['bidSize']}\n"
            f"📌 AskPrice:{newTradeData['askPrice']}, AskSize:{newTradeData['askSize']}\n"
            f"📌 Spread Δ : {fmt_spread_inv(newTradeData['spread'])}\n\n"
            
            f"📦 Inventory Before:\n"
            f"📊 Direction = {inventory['dir']}\n"
            f"📌 Spread Δ  = {fmt_spread_inv(inventory['spread'])}\n"
            f"⚡ L {extPair['symbol']} Inv : {fmtNumInv(l_qty, extPair["min_size_change"])} @ {fmtNumInv(l_entry_price, extPair["min_price_change"])}\n"
            f"🌐 E {extPair['symbol']} Inv : {fmtNumInv(e_qty, extPair["min_size_change"])} @ {fmtNumInv(e_entry_price, extPair["min_price_change"])}\n\n"
        )

        return msg

    async def sendInfo(msg, LIGHTER_API, EXTENDED_API, L_AllSymInvValue, logL, logE):
        extPair                 = EXTENDED_API.pair
        l_qty, l_entry_price    = LIGHTER_API .accountData["qty"], LIGHTER_API .accountData["entry_price"]
        e_qty, e_entry_price    = EXTENDED_API.accountData["qty"], EXTENDED_API.accountData["entry_price"]

        inventory               = {"dir":"", "spread":0}
        if l_qty > 0 and e_qty < 0:
            inventory["dir"]    = "LE"
            inventory["spread"] = (e_entry_price - l_entry_price) / l_entry_price  * 100
        if l_qty < 0 and e_qty > 0:
            inventory["dir"]    = "EL"
            inventory["spread"] = (l_entry_price - e_entry_price) / e_entry_price * 100
        if l_qty == 0 and e_qty == 0:
            inventory["dir"]    = "-"
            inventory["spread"] = 0
            
        msg += (
            f"📦 Inventory After:\n"
            f"📊 Direction = {inventory['dir']}\n"
            f"📌 Spread Δ  = {fmt_spread_inv(inventory['spread'])}\n"

            f"⚡ L {extPair['symbol']} Inv: {fmtNumInv(l_qty, extPair["min_size_change"])} @ {fmtNumInv(l_entry_price, extPair["min_price_change"])}\n"
            f"🌐 E {extPair['symbol']} Inv: {fmtNumInv(e_qty, extPair["min_size_change"])} @ {fmtNumInv(e_entry_price, extPair["min_price_change"])}\n\n"

            f"📝 L Trade Log:\n{logL}\n"
            f"📝 E Trade Log:\n{logE}\n"
        )
        
        # --- Telegram Message ---
        await send_telegram_message(msg)
        return msg


    @staticmethod
    def record_error(error_message, log_file="errorLog.txt", include_traceback=True):
        logger.info("Recording error to log file.")
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(error_message, Exception):
            if include_traceback:
                error_text = ''.join(traceback.format_exception(type(error_message), error_message, error_message.__traceback__))
            else:
                error_text = str(error_message)
        else:
            error_text = str(error_message)

        log_entry = f"[{timestamp}] {error_text}\n{'-'*80}\n"
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(log_entry)

