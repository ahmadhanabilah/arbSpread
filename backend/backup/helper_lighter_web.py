
from collections import defaultdict
import csv
from datetime import datetime, timedelta
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
            filename = "database/config_lighterMarkets.csv"
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

    async def _get_auth_token(self, force):
        """Ensure we have a valid auth token; refresh if expired or near expiry."""
        now = time.time()
        if not self.auth_token or now > (self.auth_expiry - 60) or force:  # refresh 1 minute early
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
                self.auth_expiry = now + ( 9*60 )
                logger.info("🔑 Refreshed Lighter auth token")
            except Exception as e:
                logger.error(f"Error refreshing auth token: {e}")
                return None
        return self.auth_token
            
    def _load_market_map(self):
        filename = "database/config_lighterMarkets.csv"
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


    async def getFundingFee(self, page_size: int = 100, max_retries: int = 5):
        try:
            filename            = "database/fundings_lig.csv"
            known_fieldnames    = None
            newest_ts           = 0
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            file_exists         = os.path.exists(filename)

            # --- Step 1: Auth token ---
            auth = await self._get_auth_token(False)
            if not auth:
                logger.error("❌ Auth token unavailable.")
                return

            # --- Step 2: Load market map ---
            market_map          = {}
            market_file         = "database/config_lighterMarkets.csv"
            if os.path.exists(market_file):
                with open(market_file, newline="", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        market_map[str(row["market_id"])] = row["symbol"]
                logger.info(f"📘 Loaded {len(market_map)} market mappings.")
            else:
                logger.warning(f"⚠️ Market file not found: {market_file}")

            # --- Step 3: Load existing CSV for incremental updates ---
            if file_exists:
                with open(filename, "r", newline="", encoding="utf-8") as f_read:
                    reader_check = csv.reader(f_read)
                    try:
                        known_fieldnames = next(reader_check)
                    except StopIteration:
                        pass

                with open(filename, newline="", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        ts_str = row.get("timestamp")
                        if not ts_str:
                            continue
                        try:
                            ts_val = int(ts_str)
                            newest_ts = max(newest_ts, ts_val)
                        except Exception:
                            continue

                logger.info(f"📂 Found existing {filename} — newest timestamp: {newest_ts}")
                logger.info(f"🔢 Using {len(known_fieldnames) if known_fieldnames else 0} existing columns for consistency.")
            else:
                logger.info("🆕 No existing file found — starting full sync.")

            # --- Step 4: API setup ---
            url = f"{self.config['base_url']}/api/v1/positionFunding"
            headers = {
                "accept": "application/json",
                "authorization": auth,
            }
            params = {
                "account_index": self.config["account_index"],
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

                    fundings = data.get("position_fundings", [])
                    next_cursor = data.get("next_cursor")
                    if not fundings:
                        logger.info("✅ No more funding entries found.")
                        break

                    # --- Add symbol + sort + filter new data ---
                    for f_ in fundings:
                        market_id = str(f_.get("market_id", ""))
                        f_["symbol"] = market_map.get(market_id, "UNKNOWN")

                    fundings.sort(key=lambda x: x.get("timestamp", 0))
                    # --- Load existing rows as a set of unique signatures
                    existing_rows = set()
                    if file_exists:
                        with open(filename, newline="", encoding="utf-8") as f:
                            reader = csv.DictReader(f)
                            for row in reader:
                                row_signature = tuple(row.items())  # make it hashable
                                existing_rows.add(row_signature)

                    # --- Filter out any fundings that already exist fully
                    new_data = []
                    for f_ in fundings:
                        # Convert all values to str for consistent comparison
                        signature = tuple(sorted((k, str(v)) for k, v in f_.items()))
                        if signature not in existing_rows:
                            new_data.append(f_)

                    if not new_data:
                        logger.info("⏹ Reached already-known funding data, stopping.")
                        break

                    new_data.sort(key=lambda x: x.get("timestamp", 0), reverse=True)

                    # --- Determine consistent fieldnames ---
                    if known_fieldnames is None:
                        all_keys = set()
                        for f_ in new_data:
                            all_keys.update(f_.keys())
                        known_fieldnames = sorted(list(all_keys))

                    # --- Write batch to CSV ---
                    with open(filename, "a", newline="", encoding="utf-8") as f:
                        writer = csv.DictWriter(f, fieldnames=known_fieldnames, extrasaction="ignore")
                        if not file_exists and cursor is None:
                            writer.writeheader()
                        writer.writerows(new_data)

                    # --- Step X: Sort entire file by timestamp after writing
                    try:
                        with open(filename, newline="", encoding="utf-8") as f:
                            reader = list(csv.DictReader(f))
                            reader.sort(key=lambda x: int(x.get("timestamp", 0)), reverse=True)

                        with open(filename, "w", newline="", encoding="utf-8") as f:
                            writer = csv.DictWriter(f, fieldnames=known_fieldnames, extrasaction="ignore")
                            writer.writeheader()
                            writer.writerows(reader)

                        logger.info("🧾 Sorted funding file by timestamp.")
                    except Exception as sort_err:
                        logger.warning(f"⚠️ Failed to sort CSV: {sort_err}")


                    total_new += len(new_data)
                    logger.info(f"📦 Saved {len(new_data)} new fundings (total {total_new})")

                    if not next_cursor:
                        break
                    cursor = next_cursor
                    await asyncio.sleep(0.5)

            logger.info(f"✅ Completed getFundingFee — {total_new} new fundings added to {filename}")
            print(f"✅ Completed getFundingFee — {total_new} new fundings added to {filename}")

        except Exception as e:
            logger.error(f"⚠️ Fatal error in getFundingFee: {e}", exc_info=True)
            print(f"⚠️ Fatal error in getFundingFee: {e}")




    async def getTrades(self, page_size: int = 100, max_retries: int = 5):
        try:
            filename = "database/trades_lig.csv"
            known_fieldnames = None # Variable to hold the definitive, consistent column order
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            
            newest_ts = 0
            file_exists = os.path.exists(filename)
            
            if file_exists:
                with open(filename, "r", newline="", encoding="utf-8") as f_read:
                    reader_check = csv.reader(f_read)
                    try:
                        known_fieldnames = next(reader_check)
                    except StopIteration:
                        pass
                
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

            total_new = 0
            cursor = None

            async with aiohttp.ClientSession() as session:
                while True:
                    auth            = await self._get_auth_token(force=False)
                    if not auth:
                        logger.error("❌ Auth token unavailable.")
                        return

                    url             = f"{self.config['base_url']}/api/v1/trades"
                    headers         = {"accept": "application/json", "authorization": auth}
                    params          = {"account_index": self.config["account_index"], "sort_by": "timestamp", "sort_dir": "desc", "limit": page_size}

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

                    # --- NEW: handle API internal error / code not 200 ---
                    if data.get("code") != 200:
                        logger.warning(f"⚠️ API returned code={data.get('code')} message={data.get('message', 'no message')}. Retrying in 1s...")
                        await self._get_auth_token(force=True)
                        await asyncio.sleep(1)
                        continue

                    trades = data.get("trades", [])
                    next_cursor = data.get("next_cursor")

                    if not trades:
                        logger.info(f'✅ No more trades found. code: {data.get("code")}')
                        break

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

                    # 1. If this is a brand new file (no known_fieldnames), define them now.
                    if known_fieldnames is None:
                        all_keys = set()
                        for t in new_trades:
                            all_keys.update(t.keys())
                        known_fieldnames = sorted(list(all_keys)) 
                        
                    # 2. Write the trades using the established fieldnames
                    with open(filename, "a", newline="", encoding="utf-8") as f:
                        writer = csv.DictWriter(f, fieldnames=known_fieldnames, extrasaction='ignore')
                        if not file_exists and cursor is None:
                            writer.writeheader()
                            
                        writer.writerows(new_trades)

                    total_new       += len(new_trades)
                    logger.info(f"📦 Saved {len(new_trades)} new trades (total {total_new}) cursor:{cursor}")

                    if not next_cursor:
                        break
                    cursor = next_cursor
                    await asyncio.sleep(0.1)

            logger.info(f"✅ Completed getTrades — {total_new} new trades added to {filename}")
            print(f"✅ Completed getTrades — {total_new} new trades added to {filename}")

        except Exception as e:
            logger.error(f"⚠️ Fatal error in getTrades: {e}", exc_info=True)
            print(f"⚠️ Fatal error in getTrades: {e}")

    def mergeTrades(self):
        input_file          = "database/trades_lig.csv"
        output_file         = "database/trades_merged_lig.csv"
        markets_file        = "database/config_lighterMarkets.csv"

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

    async def getRealPnl(self):
        try:
            filename = "database/realPnl_lig.csv"
            os.makedirs(os.path.dirname(filename) or ".", exist_ok=True)

            async with aiohttp.ClientSession() as session:
                auth = await self._get_auth_token(force=False)
                if not auth:
                    logger.error("❌ Auth token unavailable.")
                    return

                start_ts = 0
                end_ts = int(datetime.utcnow().timestamp())

                url = (
                    f"{self.config['base_url']}/api/v1/pnl?"
                    f"auth={auth}"
                    f"&by=index"
                    f"&value={self.config['account_index']}"
                    f"&resolution=1d"
                    f"&start_timestamp={start_ts}"
                    f"&end_timestamp={end_ts}"
                    f"&count_back=0"
                    f"&ignore_transfers=false"
                )

                headers = {
                    "accept": "application/json",
                    "authorization": auth,
                }

                logger.info(f"📡 Requesting PnL data for account_index={self.config['account_index']}")
                async with session.get(url, headers=headers, timeout=60) as resp:
                    if resp.status != 200:
                        logger.error(f"❌ Failed to fetch PnL data: HTTP {resp.status}")
                        text = await resp.text()
                        logger.error(text)
                        return

                    data = await resp.json()

                pnl_data = data.get("pnl", data)
                if not pnl_data:
                    logger.warning("⚠️ No PnL data returned.")
                    return

                filtered_rows = []
                last_date = None  # keep track of last valid date (for realtime snapshot)

                for t in pnl_data:
                    try:
                        ts_val = int(t.get("timestamp", 0))
                        if not ts_val:
                            continue

                        dt = datetime.utcfromtimestamp(ts_val)
                        cum_pnl = float(t.get("trade_pnl", 0))

                        if dt.hour == 0 and dt.minute == 0 and dt.second == 0:
                            # 00:00 UTC snapshot = previous day
                            date_for_pnl = (dt - timedelta(days=1)).strftime("%Y-%m-%d")
                        else:
                            # anything after 00:00 belongs to the same day
                            date_for_pnl = dt.strftime("%Y-%m-%d")

                        last_date = date_for_pnl


                        filtered_rows.append({
                            "timestamp": ts_val,
                            "date": date_for_pnl,
                            "cumulative_pnl": cum_pnl,
                        })

                    except Exception as e:
                        logger.warning(f"⚠️ Skipping row due to error: {e}")
                        continue

                if not filtered_rows:
                    logger.warning("⚠️ No valid PnL rows found.")
                    return

                # Sort ascending by date to calculate ΔPnL
                filtered_rows.sort(key=lambda x: (x["date"], x["timestamp"]))

                # Compute daily ΔPnL
                prev_pnl = None
                for row in filtered_rows:
                    cum_pnl = row["cumulative_pnl"]
                    if prev_pnl is None:
                        row["pnl"] = 0.0
                    else:
                        row["pnl"] = round(cum_pnl - prev_pnl, 8)
                    prev_pnl = cum_pnl

                # Sort descending by date for final output
                filtered_rows.sort(key=lambda x: (x["date"], x["timestamp"]), reverse=True)

                # Write CSV
                with open(filename, "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=["timestamp", "date", "cumulative_pnl", "pnl"])
                    writer.writeheader()
                    writer.writerows(filtered_rows)

                logger.info(f"✅ Saved {len(filtered_rows)} PnL entries (00:00 + realtime snapshot) to {filename}")

        except Exception as e:
            logger.error(f"⚠️ Fatal error in getRealPnl: {e}", exc_info=True)

    def calculateDailyPnL(self):
        TRADES_MERGED_FILE = "database/trades_merged_lig.csv"
        REALPNL_FILE       = "database/realPnl_lig.csv"
        OUTPUT_FILE        = "database/trades_daily_pnl_lig.csv"

        if not os.path.exists(TRADES_MERGED_FILE):
            print("❌ trades_merged_lig.csv not found.")
            return

        daily_pnl    = defaultdict(float)
        daily_volume = defaultdict(float)
        daily_real   = defaultdict(float)

        # --- load trades data ---
        with open(TRADES_MERGED_FILE, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if not row.get("exit_time") or row.get("status", "").upper() != "CLOSED":
                    continue
                try:
                    date_str = row["exit_time"].split(" ")[0]  # YYYY-MM-DD
                    pnl      = float(row.get("net_pnl", 0))
                    entryvol = float(row.get("qty_opened", 0)) * float(row.get("avg_entry_price", 0))
                    exitvol  = float(row.get("qty_closed", 0)) * float(row.get("avg_exit_price", 0))
                    daily_pnl[date_str]    += pnl
                    daily_volume[date_str] += entryvol + exitvol
                except Exception as e:
                    print(f"⚠️ Skipping row due to error: {e}")

        # --- load real PnL data ---
        if os.path.exists(REALPNL_FILE):
            with open(REALPNL_FILE, newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    try:
                        ts                      = int(row.get("timestamp", 0))
                        date_str                = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
                        trade_pnl               = float(row.get("pnl", 0))
                        daily_real[date_str]    += trade_pnl
                    except Exception as e:
                        print(f"⚠️ Skipping realPnL row due to error: {e}")
        else:
            print("⚠️ realPnl_lig.csv not found — daily_realPnl will be empty.")

        # --- write merged daily summary ---
        sorted_dates = sorted(daily_pnl.keys(), reverse=True)
        with open(OUTPUT_FILE, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["date", "daily_pnl_usd", "daily_volume_usd", "daily_realPnl"])
            for date in sorted_dates:
                writer.writerow([
                    date,
                    round(daily_pnl[date], 8),
                    round(daily_volume[date], 8),
                    round(daily_real.get(date, 0), 8)
                ])

        print(f"✅ Daily PnL summary with realPnL saved to {OUTPUT_FILE}")
