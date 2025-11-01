
from collections import defaultdict
import csv
from datetime import datetime
import re
from collections import defaultdict
import csv
from datetime import datetime
import time
import os
import asyncio
import aiohttp
import logging
import lighter
from lighter import WsClient
from decimal import Decimal
from dotenv import load_dotenv

load_dotenv()

logger                          = logging.getLogger("helper_lighter")
logger.setLevel                 (logging.INFO)
load_dotenv                     ()

class LighterAPI:
    def __init__(self):
        self.client             = None
        self.ws_client          = None
        self.config             = {
            "base_url"          : "https://mainnet.zklighter.elliot.ai",
            "private_key"       : os.getenv("LIGHTER_API_PRIVATE_KEY"),
            "account_index"     : int(os.getenv("LIGHTER_ACCOUNT_INDEX")),
            "api_key_index"     : int(os.getenv("LIGHTER_API_KEY_INDEX")),
            "slippage"          : float(os.getenv("ALLOWED_SLIPPAGE")) / 100,
        }
        self.market_map         = []
        self.auth_token         = None
        self.auth_expiry        = 0  # unix timestamp
        
    async def init(self):
        self.client             = lighter.SignerClient(
            url                 = self.config["base_url"],
            private_key         = self.config["private_key"],
            account_index       = self.config["account_index"],
            api_key_index       = self.config["api_key_index"],
        )

        url                     = f"{self.config["base_url"]}/api/v1/orderBookDetails"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    raise Exception(f"Failed to fetch market metadata: {resp.status}")
                data            = await resp.json()

        details                 = data.get("order_book_details", [])

        async def saveAllMarketsToCsv():
            filename = "config_lighterMarkets.csv"
            with open(filename, "w", newline="") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=["symbol", "market_id"]
                )
                writer.writeheader()
                for d in details:
                    writer.writerow({
                        "symbol": d["symbol"],
                        "market_id": d["market_id"],
                    })
            logger.info(f"Saved {len(details)} Lighter markets → {filename}")

        await saveAllMarketsToCsv()
        self.market_map             = self._load_market_map() 

        logger.info(f"Lighter init Done")

    async def _get_auth_token(self):
        """Ensure we have a valid auth token; refresh if expired or near expiry."""
        now = time.time()
        if not self.auth_token or now > (self.auth_expiry - 60):  # refresh 1 minute early
            try:
                auth, err = self.client.create_auth_token_with_expiry(
                    lighter.SignerClient.DEFAULT_10_MIN_AUTH_EXPIRY
                )
                if auth:
                    print(f"{auth}")
                if err:
                    logger.error(f"Failed to create auth token: {err}")
                    return None
                self.auth_token = auth
                self.auth_expiry = now + (lighter.SignerClient.DEFAULT_10_MIN_AUTH_EXPIRY / 1000)
                logger.info("🔑 Refreshed Lighter auth token")
            except Exception as e:
                logger.error(f"Error refreshing auth token: {e}")
                return None
        return self.auth_token
            
    def _load_market_map(self):
        filename = "config_lighterMarkets.csv"
        market_map = {}
        if not os.path.exists(filename):
            logger.error(f"Market config file not found: {filename}. Trades will lack symbols.")
            return market_map

        try:
            with open(filename, "r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Key is market_id, Value is symbol
                    market_map[row["market_id"]] = row["symbol"]
            logger.info(f"Loaded {len(market_map)} markets from {filename}")
        except Exception as e:
            logger.error(f"Error loading market map from {filename}: {e}")

        return market_map


    async def getTrades(self, page_size: int = 100, max_retries: int = 5):
        """
        Fetch trades from Lighter:
        - If no CSV exists → full sync
        - If CSV exists → fetch only newer trades (based on timestamp)
        - Dynamically saves all columns provided by API, ensuring CONSISTENT COLUMN ORDER.
        """
        try:
            filename = "trades_lig.csv"
            known_fieldnames = None # Variable to hold the definitive, consistent column order

            # --- Step 1: Get auth token ---
            auth = await self._get_auth_token()
            if not auth:
                logger.error("❌ Auth token unavailable.")
                return

            # --- Step 2: Detect latest timestamp and retrieve existing header (if file exists) ---
            newest_ts = 0
            file_exists = os.path.exists(filename)
            
            if file_exists:
                # First, check header for known_fieldnames
                with open(filename, "r", newline="", encoding="utf-8") as f_read:
                    reader_check = csv.reader(f_read)
                    try:
                        known_fieldnames = next(reader_check)
                    except StopIteration:
                        # File exists but is empty, treat as new file for fieldnames setting
                        pass
                
                # Now read data to find the newest timestamp
                with open(filename, newline="", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        ts_str = row.get("timestamp")
                        if not ts_str:
                            continue
                        try:
                            if str(ts_str).isdigit():
                                ts_val = int(ts_str)
                            else:
                                ts_val = int(datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
                            newest_ts = max(newest_ts, ts_val)
                        except Exception:
                            continue
                            
                logger.info(f"📂 Found existing {filename} — newest timestamp: {newest_ts}")
                logger.info(f"🔢 Using {len(known_fieldnames)} existing columns for consistency.")
            else:
                logger.info("🆕 No existing file found — starting full sync.")

            # --- Step 3: Request setup ---
            url = f"{self.config['base_url']}/api/v1/trades"
            headers = {
                "accept": "application/json",
                "authorization": auth,
            }
            params = {
                "account_index": self.config["account_index"],
                "sort_by": "timestamp",
                "sort_dir": "desc",
                "limit": page_size,
            }

            total_new = 0
            cursor = None

            async with aiohttp.ClientSession() as session:
                while True:
                    if cursor:
                        params["cursor"] = cursor

                    # --- Retry loop ---
                    for attempt in range(max_retries):
                        try:
                            async with session.get(url, headers=headers, params=params, timeout=30) as resp:
                                data = await resp.json()
                                break
                        except Exception as e:
                            wait = 2 ** attempt
                            logger.warning(f"⚠️ Request failed ({e}); retrying in {wait}s...")
                            await asyncio.sleep(wait)
                    else:
                        logger.error("❌ Max retries reached; stopping fetch.")
                        break

                    trades = data.get("trades", [])
                    next_cursor = data.get("next_cursor")
                    if not trades:
                        logger.info("✅ No more trades found.")
                        break

                    # --- Stop when reaching older trades ---
                    # Sort trades ascending to find the true stop point (API returns descending)
                    trades.sort(key=lambda x: x.get("timestamp", 0))
                    
                    # Check how many trades are truly new
                    new_trades = [t for t in trades if t.get("timestamp", 0) > newest_ts]
                    
                    # If the first trade in the batch is older than our newest_ts, we stop
                    if not new_trades:
                        logger.info("⏹ Reached already-known trades, stopping.")
                        break
                    
                    # Re-sort to write in descending order (or just write in the order they were processed)
                    new_trades.sort(key=lambda x: x.get("timestamp", 0), reverse=True)


                    # --- FIX: Ensure Consistent Column Order and Write ---
                    
                    # 1. If this is a brand new file (no known_fieldnames), define them now.
                    if known_fieldnames is None:
                        all_keys = set()
                        for t in new_trades:
                            all_keys.update(t.keys())
                        # Alphabetically sort to establish a consistent order for the first time
                        known_fieldnames = sorted(list(all_keys)) 
                        
                    # 2. Write the trades using the established fieldnames
                    # We use 'a' (append) mode.
                    with open(filename, "a", newline="", encoding="utf-8") as f:
                        # Use 'known_fieldnames' for consistent order. 
                        # 'extrasaction' = 'ignore' is crucial: ignores any new keys in the current batch
                        # that weren't present in the initial/existing header.
                        writer = csv.DictWriter(f, fieldnames=known_fieldnames, extrasaction='ignore')
                        
                        # Only write the header if the file was just created (i.e., known_fieldnames was just set
                        # and we are appending to a file that was just opened, and we are on the first cursor page).
                        if not file_exists and cursor is None:
                            writer.writeheader()
                            
                        writer.writerows(new_trades)

                    total_new += len(new_trades)
                    logger.info(f"📦 Saved {len(new_trades)} new trades (total {total_new})")

                    if not next_cursor:
                        break
                    cursor = next_cursor
                    await asyncio.sleep(0.5)

            logger.info(f"✅ Completed getTrades — {total_new} new trades added to {filename}")
            print(f"✅ Completed getTrades — {total_new} new trades added to {filename}")

        except Exception as e:
            logger.error(f"⚠️ Fatal error in getTrades: {e}", exc_info=True)
            print(f"⚠️ Fatal error in getTrades: {e}")


    def mergeTrades(self):
        input_file  = "trades_lig.csv"
        output_file = "trades_merged_lig.csv"
        markets_file = "config_lighterMarkets.csv"

        if not os.path.exists(input_file):
            print("❌ trades_lig.csv not found.")
            return

        # --- Load market map ---
        market_map = {}
        if os.path.exists(markets_file):
            with open(markets_file, newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    market_map[row["market_id"]] = row["symbol"]
            logger.info(f"Loaded {len(market_map)} markets from {markets_file}")
        else:
            logger.warning(f"⚠️ Market map file not found: {markets_file}")

        ACCOUNT_ID = str(self.config["account_index"])
        trades_by_market = defaultdict(list)

        def safe_float(x, default=0.0):
            try:
                return float(x)
            except (TypeError, ValueError):
                return default

        # --- Step 1: Read and normalize ---
        with open(input_file, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    row["price"] = safe_float(row.get("price"))
                    row["size"] = safe_float(row.get("size"))
                    row["usd_amount"] = safe_float(row.get("usd_amount"))
                    row["maker_fee_bps"] = safe_float(row.get("maker_fee"))
                    row["taker_fee_bps"] = safe_float(row.get("taker_fee"))

                    trade_value = row["price"] * abs(row["size"])

                    # 🧮 Determine if WE are maker or taker
                    if str(row.get("ask_account_id")) == ACCOUNT_ID or str(row.get("bid_account_id")) == ACCOUNT_ID:
                        if str(row.get("ask_account_id")) == ACCOUNT_ID:
                            is_maker = bool(row.get("is_maker_ask") == "True" or row.get("is_maker_ask") is True)
                        elif str(row.get("bid_account_id")) == ACCOUNT_ID:
                            # maker if bid side was maker
                            is_maker = bool(row.get("is_maker_ask") == "False" or row.get("is_maker_ask") is False)
                        else:
                            is_maker = False
                    else:
                        # Skip if neither side matches our account
                        continue

                    if is_maker:
                        fee_usd = trade_value * (row["maker_fee_bps"] / 1_000_000)
                    else:
                        fee_usd = trade_value * (row["taker_fee_bps"] / 1_000_000)

                    row["fee_usd"] = fee_usd
                    row["is_maker"] = is_maker

                    # parse timestamp
                    ts_raw = row.get("timestamp")
                    if not ts_raw:
                        continue
                    ts_val = int(float(ts_raw))
                    row["timestamp"] = datetime.fromtimestamp(ts_val / 1000)

                    # determine side
                    if row.get("ask_account_id") == ACCOUNT_ID:
                        row["side"] = "SELL"
                    elif row.get("bid_account_id") == ACCOUNT_ID:
                        row["side"] = "BUY"
                    else:
                        continue

                    if row["price"] == 0 or row["size"] == 0:
                        continue

                    market = row.get("market_id", "UNKNOWN")
                    trades_by_market[market].append(row)
                except Exception as e:
                    logger.warning(f"⚠️ Skipping row parse error: {e}")

        if not trades_by_market:
            logger.warning("⚠️ No valid trades found to merge.")
            return

        # --- Step 2: Merge trades per market ---
        merged = []
        for market, trades in trades_by_market.items():
            trades.sort(key=lambda x: x["timestamp"])

            running_qty = 0.0
            entry_side = None
            entry_time = None
            entry_trade_id = None
            qty_opened = qty_closed = entry_price_total = exit_price_total = 0.0
            total_fee = 0.0

            for t in trades:
                side = t["side"]
                qty = t["size"]
                price = t["price"]
                time = t["timestamp"]
                fee = safe_float(t.get("fee_usd", 0.0))
                trade_id = t["trade_id"]

                total_fee += fee

                # start new position
                if abs(running_qty) < 1e-12:
                    qty_opened = qty_closed = entry_price_total = exit_price_total = 0.0
                    total_fee = fee
                    entry_side = side
                    entry_trade_id = trade_id
                    entry_time = time

                if side == entry_side:
                    qty_opened += qty
                    entry_price_total += price * qty
                    running_qty += qty if side == "BUY" else -qty
                else:
                    qty_closed += qty
                    exit_price_total += price * qty
                    running_qty += qty if side == "BUY" and entry_side == "SELL" else -qty

                # --- close position ---
                if abs(running_qty) < 1e-8:
                    avg_entry = entry_price_total / qty_opened if qty_opened else 0.0
                    avg_exit = exit_price_total / qty_closed if qty_closed else 0.0

                    if entry_side == "BUY":
                        pnl = (avg_exit - avg_entry) * qty_closed
                    else:
                        pnl = (avg_entry - avg_exit) * qty_closed

                    net_pnl = pnl - total_fee

                    merged.append({
                        "symbol": market_map.get(market, "UNKNOWN"),
                        "market_id": market,
                        "side": entry_side,
                        "entry_time": entry_time.strftime("%Y-%m-%d %H:%M:%S"),
                        "exit_time": time.strftime("%Y-%m-%d %H:%M:%S"),
                        "qty_opened": round(qty_opened, 8),
                        "qty_closed": round(qty_closed, 8),
                        "avg_entry_price": round(avg_entry, 8),
                        "avg_exit_price": round(avg_exit, 8),
                        "pnl": round(pnl, 8),
                        "net_pnl": round(net_pnl, 8),
                        "fee_total": round(total_fee, 8),
                        "entry_trade_id": entry_trade_id,
                        "exit_trade_id": trade_id,
                        "status": "CLOSED",
                    })

                    running_qty = 0.0
                    entry_side = entry_trade_id = None
                    qty_opened = qty_closed = entry_price_total = exit_price_total = total_fee = 0.0

            # --- handle open position ---
            if entry_side:
                avg_entry = entry_price_total / qty_opened if qty_opened else 0.0
                merged.append({
                    "symbol": market_map.get(market, "UNKNOWN"),
                    "market_id": market,
                    "side": entry_side,
                    "entry_time": entry_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "exit_time": "",
                    "qty_opened": round(qty_opened, 8),
                    "qty_closed": round(qty_closed, 8),
                    "avg_entry_price": round(avg_entry, 8),
                    "avg_exit_price": "" if qty_closed == 0 else round(exit_price_total / qty_closed, 8),
                    "pnl": "",
                    "net_pnl": "",
                    "fee_total": round(total_fee, 8),
                    "entry_trade_id": entry_trade_id,
                    "exit_trade_id": "",
                    "status": "OPEN",
                })

        # --- Step 3: Save merged summary ---
        merged.sort(key=lambda x: x["entry_time"], reverse=True)
        with open(output_file, "w", newline="", encoding="utf-8") as f:
            fieldnames = [
                "symbol", "market_id", "side", "entry_time", "exit_time",
                "qty_opened", "qty_closed",
                "avg_entry_price", "avg_exit_price", "pnl", "net_pnl", "fee_total",
                "entry_trade_id", "exit_trade_id", "status"
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(merged)

        print(f"✅ Merged {len(merged)} trades saved to {output_file}")



    def calculateDailyPnL(self):
        INPUT_FILE = "trades_merged_lig.csv"
        OUTPUT_FILE = "trades_daily_pnl_lig.csv"

        if not os.path.exists(INPUT_FILE):
            print("❌ trades_merged_lig.csv not found.")
            return

        daily_pnl = defaultdict(float)
        daily_volume = defaultdict(float)

        with open(INPUT_FILE, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if not row["exit_time"] or row["status"].upper() != "CLOSED":
                    continue
                try:
                    date_str        = row["exit_time"].split(" ")[0]  # YYYY-MM-DD
                    pnl             = float(row["net_pnl"])
                    entryvolume     = float(row.get("qty_opened", 0))  * float(row.get("avg_entry_price", 0))  
                    exitvolume      = float(row.get("qty_closed", 0))  * float(row.get("avg_exit_price", 0))

                    daily_pnl[date_str]     += pnl
                    daily_volume[date_str]  += entryvolume + exitvolume
                except Exception as e:
                    print(f"⚠️ Skipping row due to error: {e}")

        # Sort by date descending
        sorted_daily = sorted(daily_pnl.keys(), reverse=True)

        with open(OUTPUT_FILE, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["date", "daily_pnl_usd", "daily_volume_usd"])
            for date in sorted_daily:
                writer.writerow([date, round(daily_pnl[date], 8), round(daily_volume[date], 8)])

        print(f"✅ Daily PnL and volume summary saved to {OUTPUT_FILE}")
