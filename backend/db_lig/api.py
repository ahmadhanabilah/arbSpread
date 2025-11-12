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

logger                          = logging.getLogger("db_lig.api")
logger.setLevel                 (logging.INFO)
load_dotenv                     ('/root/arbSpread/backend/.env')

def _sort_csv_by_int_field_desc(path: str, field: str, fieldnames: list[str]) -> None:
    """Read entire CSV, sort by int(field) desc (missing/invalid -> 0), rewrite with header."""
    if not os.path.exists(path):
        return
    try:
        with open(path, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        # robust numeric parse (ms or s or bad strings)
        def to_int(v):
            try:
                x = int(str(v).strip())
            except Exception:
                return 0
            # normalize: if looks like ms since epoch, keep ms; just sort numerically anyway
            return x
        rows.sort(key=lambda r: to_int(r.get(field, 0)), reverse=True)

        # Ensure we write the same columns order we used while appending
        headers = fieldnames or (list(rows[0].keys()) if rows else [field])
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)
    except Exception as e:
        logger.warning(f"⚠️ Failed to sort {path} by {field} desc: {e}", exc_info=True)


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
            filename        = "/root/arbSpread/backend/db_lig/config/lighterMarkets.csv"
            os.makedirs(os.path.dirname(filename), exist_ok=True)
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
                    logger.info(f"{auth}")
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
        filename = "/root/arbSpread/backend/db_lig/config/lighterMarkets.csv"
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

    async def getFundingPayment(self, page_size: int = 100, max_retries: int = 5):
        try:
            filename            = "/root/arbSpread/backend/db_lig/raw/_fundings.csv"
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
            market_file         = "/root/arbSpread/backend/db_lig/config/lighterMarkets.csv"
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
                    _sort_csv_by_int_field_desc(filename, "timestamp", known_fieldnames)
                    logger.info("🧾 Sorted funding file by timestamp (desc).")

                    total_new += len(new_data)
                    logger.info(f"📦 Saved {len(new_data)} new fundings (total {total_new})")

                    if not next_cursor:
                        break
                    cursor = next_cursor
                    await asyncio.sleep(0.5)

            logger.info(f"✅ Completed getFundingFee — {total_new} new fundings added to {filename}")

        except Exception as e:
            logger.error(f"⚠️ Fatal error in getFundingFee: {e}", exc_info=True)

    async def getTrades(self, page_size: int = 100, max_retries: int = 5):
        try:
            filename            = "/root/arbSpread/backend/db_lig/raw/_trades.csv"
            known_fieldnames    = None # Variable to hold the definitive, consistent column order
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

            auth            = await self._get_auth_token(force=False)
            if not auth:
                logger.error("❌ Auth token unavailable.")
                return

            async with aiohttp.ClientSession() as session:
                while True:

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
                    
                    if not next_cursor:
                        break
                    cursor = next_cursor
                    await asyncio.sleep(0.1)

            _sort_csv_by_int_field_desc(filename, "timestamp", known_fieldnames)
            logger.info(f"✅ Completed getTrades — {total_new} new trades added to {filename}")

        except Exception as e:
            logger.error(f"⚠️ Fatal error in getTrades: {e}", exc_info=True)

    def split_trades_by_symbol(self):
        input_file  ="/root/arbSpread/backend/db_lig/raw/_trades.csv"
        output_dir  ="/root/arbSpread/backend/db_lig/raw"

        # 0) Safety: ensure input exists
        if not os.path.exists(input_file):
            logger.info(f"❌ Input trades file not found: {input_file}")
            return

        # 1) Load mapping
        market_map = self._load_market_map()

        # 2) Read and group rows
        os.makedirs(output_dir, exist_ok=True)
        grouped = defaultdict(list)

        with open(input_file, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames or []
            for row in reader:
                mid = str(row.get("market_id", "")).strip()
                symbol = market_map.get(mid, f"UNKNOWN-{mid or 'NOID'}")
                grouped[symbol].append(row)

        # 3) Write each symbol file (sorted by timestamp DESC)
        for symbol, rows in grouped.items():
            # Normalize filename (no slashes, spaces)
            safe_symbol = symbol.replace("/", "-").replace(":", "-").replace(" ", "")
            out_path = os.path.join(output_dir, f"{safe_symbol}.csv")

            # sort by timestamp desc; treat missing timestamp as 0
            def _ts(row):
                v = row.get("timestamp")
                try:
                    return int(str(v).strip())
                except Exception:
                    return 0
            rows.sort(key=_ts)

            with open(out_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(rows)
