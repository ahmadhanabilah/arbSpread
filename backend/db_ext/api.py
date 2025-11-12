from collections import defaultdict
from datetime import datetime
import csv
import traceback
import os
import asyncio
import logging
import aiohttp
from dotenv import load_dotenv
from decimal import Decimal
from pathlib import Path

from x10.perpetual.accounts import StarkPerpetualAccount
from x10.perpetual.configuration import MAINNET_CONFIG
from x10.perpetual.orders import OrderSide as ExtendedOrderSide
from x10.perpetual.trading_client import PerpetualTradingClient
from x10.perpetual.stream_client import PerpetualStreamClient
from x10.utils.http import send_get_request

logger                          = logging.getLogger("db_ext.api")
logger.setLevel                 (logging.INFO)
load_dotenv                     ("/root/arbSpread/backend/.env")

class ExtendedAPI:
    def __init__(self):
        self.client             = None
        self.ws_client          = None
        self.starkPerpAcc       = None 
        self.config             = {
            "vault_id"          : int(os.getenv("EXTENDED_VAULT_ID")),
            "private_key"       : os.getenv("EXTENDED_PRIVATE_KEY"),
            "public_key"        : os.getenv("EXTENDED_PUBLIC_KEY"),
            "api_key"           : os.getenv("EXTENDED_API_KEY"),
            "slippage"          : float(os.getenv("ALLOWED_SLIPPAGE")) / 100,
        }
        self.allSymbols         = []

    async def init(self):
        starkPerpAcc            = StarkPerpetualAccount(
            vault               = self.config["vault_id"],
            private_key         = self.config["private_key"],
            public_key          = self.config["public_key"],
            api_key             = self.config["api_key"],
        )
        self.client             = PerpetualTradingClient(
            MAINNET_CONFIG,
            starkPerpAcc
        )
        self.ws_client          = PerpetualStreamClient(api_url=MAINNET_CONFIG.stream_url)
        
    async def getAllSymbols(self):
        url                     = f"https://api.starknet.extended.exchange/api/v1/info/markets"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                data            = await resp.json()

        if data.get("status") != "OK":
            raise Exception(f"Failed to fetch market info: {data}")

        self.allSymbols = [market["name"] for market in data["data"] if market.get("active")]

    async def getTrades(self):
        try:
            filename    = "/root/arbSpread/backend/db_ext/raw/_trades.csv"
            limit       = 300
            cursor      = None
            all_trades  = []

            # --- Step 1: Determine latest timestamp ---
            newest_timestamp = 0
            existing_fieldnames = None
            os.makedirs(os.path.dirname(filename), exist_ok=True)

            if os.path.exists(filename):
                with open(filename, newline="", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    existing_fieldnames = reader.fieldnames
                    for row in reader:
                        ts = row.get("created_time")
                        if ts and str(ts).isdigit():
                            newest_timestamp = max(newest_timestamp, int(ts))
                logger.info(f"[ExtendedAPI] Found existing file, newest timestamp: {newest_timestamp}")
            else:
                logger.info("[ExtendedAPI] No existing file found → fetching all trades")

            # --- Step 2: Pagination loop ---
            while True:
                resp = await self.client.account.get_trades(
                    market_names=[self.allSymbols],
                    cursor=cursor,
                    limit=limit,
                )

                if getattr(resp, "error", None):
                    logger.error(f"[ExtendedAPI] get_trades() error: {resp.error}")
                    break

                trades = getattr(resp, "data", [])
                if not trades:
                    break

                # keep only new trades if we already have data
                if newest_timestamp > 0:
                    trades = [t for t in trades if getattr(t, "created_time", 0) > newest_timestamp]

                if not trades:
                    logger.info("[ExtendedAPI] No newer trades found; stopping fetch.")
                    break

                all_trades.extend(trades)
                logger.info(f"[ExtendedAPI] Retrieved {len(trades)} trades (total {len(all_trades)})")

                # --- pagination ---
                cursor = None
                if hasattr(resp, "pagination") and resp.pagination:
                    cursor = getattr(resp.pagination, "next", None)
                if not cursor and len(trades) == limit:
                    last = trades[-1]
                    cursor = getattr(last, "id", None)

                if not cursor:
                    break

                await asyncio.sleep(0.2)

            # --- Step 3: Save result ---
            if not all_trades:
                logger.info("[ExtendedAPI] No new trades to save.")
                return

            trade_dicts = [t.__dict__ for t in all_trades]
            fieldnames = (
                existing_fieldnames
                or sorted(set().union(*(d.keys() for d in trade_dicts)))
            )

            file_exists = os.path.exists(filename)
            mode = "a" if file_exists else "w"

            with open(filename, mode, newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                if not file_exists:
                    writer.writeheader()
                writer.writerows(trade_dicts)

            logger.info(f"[ExtendedAPI] ✅ Saved {len(all_trades)} new trades → {filename}")

        except Exception as e:
            logger.error(f"[ExtendedAPI] getTrades() failed: {e}")

    def split_trades_by_symbol(self):
        input_file  ="/root/arbSpread/backend/db_ext/raw/_trades.csv"
        output_dir  ="/root/arbSpread/backend/db_ext/raw"

        # --- Step 1: Read all trades and group by symbol ---
        grouped_rows = defaultdict(list)
        fieldnames = None

        os.makedirs(output_dir, exist_ok=True)
        with open(input_file, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            for row in reader:
                symbol = row.get("market") or row.get("symbol")
                if not symbol:
                    continue
                grouped_rows[symbol].append(row)

        # --- Step 2: Write each group to its own file ---
        for symbol, rows in grouped_rows.items():
            safe_symbol = symbol.replace("/", "-").replace(":", "-")
            out_path = os.path.join(output_dir, f"{safe_symbol}.csv")

            file_exists = os.path.exists(out_path)
            with open(out_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
            
    async def getFundingPayment(self, start_time=None, end_time=None, cursor=None, limit=10000):
        filename            = "/root/arbSpread/backend/db_ext/raw/_fundings.csv"
        os.makedirs(os.path.dirname(filename), exist_ok=True)

        all_fundings = []
        newest_timestamp = 0

        # --- Helper: seconds -> Asia/Jakarta readable ---
        from datetime import datetime, timedelta

        def _normalize_epoch_seconds(raw):
            """
            Accepts epoch in seconds/ms/us/ns or string/float.
            Returns integer seconds or None if out-of-range.
            """
            if raw is None:
                return None
            s = str(raw).strip()
            if s == "":
                return None
            try:
                v = int(float(s))
            except Exception:
                return None

            # Reduce to seconds based on magnitude
            if v > 10**18:           # ns -> s
                v //= 10**9
            elif v > 10**15:         # µs -> s
                v //= 10**6
            elif v > 10**12:         # ms -> s
                v //= 10**3
            # else: already seconds

            # sanity: accept 1970..2200
            # 2200-01-01 00:00:00 UTC = 7258118400
            if v < 0 or v > 7258118400:
                return None
            return v

        def to_readable_jkt(raw_epoch):
            secs = _normalize_epoch_seconds(raw_epoch)
            if secs is None:
                return ""
            return (datetime.utcfromtimestamp(secs) + timedelta(hours=7)).strftime("%Y-%m-%d %H:%M:%S")

        # --- Step 1: Determine latest timestamp from CSV ---
        if os.path.exists(filename):
            with open(filename, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # prefer paidTime if present; else created_time/timestamp
                    ts = row.get("paidTime") or row.get("timestamp") or row.get("created_time")
                    if ts and str(ts).isdigit():
                        newest_timestamp = max(newest_timestamp, int(ts))
            logger.info(f"[ExtendedAPI] Found existing funding file → newest timestamp: {newest_timestamp}")
        else:
            logger.info("[ExtendedAPI] No existing funding file found → full fetch.")

        # --- Step 2: Pagination loop ---
        while True:
            url = self.client.account._get_url(
                "/user/funding/history",
                query={
                    "market": [self.allSymbols],
                    "fromTime": newest_timestamp if newest_timestamp > 0 else 0,
                    "cursor": cursor,
                    "limit": limit,
                },
            )

            session = await self.client.account.get_session()
            res = await send_get_request(
                session,
                url,
                list,
                api_key=self.client.account._get_api_key(),
            )

            data = getattr(res, "data", []) or []
            if not data:
                logger.info("[ExtendedAPI] No more funding data; stopping pagination.")
                break

            all_fundings.extend(data)
            logger.info(f"[ExtendedAPI] Retrieved {len(data)} fundings (total {len(all_fundings)})")

            # --- Pagination handler ---
            cursor = None
            if hasattr(res, "pagination") and res.pagination:
                cursor = getattr(res.pagination, "next", None)
            if not cursor and len(data) == limit:
                cursor = data[-1].get("id") or data[-1].get("paidTime") or data[-1].get("timestamp")

            if not cursor:
                break

            await asyncio.sleep(0.1)

        if not all_fundings:
            logger.info("[ExtendedAPI] No new funding records to append.")
            return

        # --- Step 3: Merge and deduplicate ---
        combined_map = {}
        for f in all_fundings:
            key = f.get("id") or f.get("paidTime") or f.get("timestamp")
            if key:
                combined_map[key] = f

        # Convert back to list
        combined = list(combined_map.values())

        # Add readable_paidTime (Asia/Jakarta) — based on paidTime (seconds)
        for item in combined:
            paid = item.get("paidTime")
            item["readable_paidTime"] = to_readable_jkt(paid) if paid else ""

        # Sort by paidTime desc, fallback to timestamp
        def sort_key(x):
            v = x.get("paidTime") or x.get("timestamp") or 0
            try:
                return int(v)
            except Exception:
                return 0

        combined.sort(key=sort_key, reverse=True)

        # --- Step 4: Save back ---
        # ensure our new column is included
        fieldnames = sorted(set(["readable_paidTime"]).union(*(d.keys() for d in combined)))
        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(combined)

        logger.info(f"[ExtendedAPI] ✅ Saved {len(combined)} total fundings (sorted desc) → {filename}")
