
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

from x10.perpetual.accounts import StarkPerpetualAccount
from x10.perpetual.configuration import MAINNET_CONFIG
from x10.perpetual.orders import OrderSide as ExtendedOrderSide
from x10.perpetual.trading_client import PerpetualTradingClient
from x10.perpetual.stream_client import PerpetualStreamClient
from x10.utils.http import send_get_request

logger                          = logging.getLogger("helper_exended")
logger.setLevel                 (logging.INFO)
load_dotenv                     ()

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


    # async def getFundingPayment(self, start_time=None, end_time=None, cursor=None, limit=None):
    #     url = self.client.account._get_url(
    #         "/user/funding/history",
    #         query={
    #             "market": [self.allSymbols],
    #             "fromTime": 0,  # or set dynamically if needed
    #             "limit": 10000,
    #             "cursor":cursor?? need this handler                
    #         },
    #     )

    #     # Create folder if missing
    #     os.makedirs("database", exist_ok=True)
    #     filename = "database/fundings_ext.csv"

    #     # --- Fetch data ---
    #     session = await self.client.account.get_session()
    #     res = await send_get_request(
    #         session,
    #         url,
    #         list,  # return raw list of dicts
    #         api_key=self.client.account._get_api_key(),
    #     )

    #     data = res.data or []

    #     if not data:
    #         logger.warning("⚠️ No funding data returned.")
    #         return

    #     # --- Save to CSV ---
    #     # Normalize fieldnames from the first item
    #     fieldnames = list(data[0].keys())

    #     write_header = not os.path.exists(filename)

    #     with open(filename, "w", newline="", encoding="utf-8") as f:
    #         writer = csv.DictWriter(f, fieldnames=fieldnames)
    #         writer.writeheader()
    #         writer.writerows(data)

    #     logger.info(f"✅ Saved {len(data)} funding records → {filename}")


    async def getFundingPayment(self, start_time=None, end_time=None, cursor=None, limit=10000):
        """
        Fetch all funding history with pagination and save to CSV incrementally.
        """
        filename = "database/fundings_ext.csv"
        os.makedirs(os.path.dirname(filename), exist_ok=True)

        all_fundings = []
        newest_timestamp = 0

        # --- Step 1: Determine latest timestamp from CSV ---
        if os.path.exists(filename):
            with open(filename, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    ts = row.get("timestamp") or row.get("created_time")
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
                cursor = data[-1].get("id") or data[-1].get("timestamp")

            if not cursor:
                break

            await asyncio.sleep(0.1)

        if not all_fundings:
            logger.info("[ExtendedAPI] No new funding records to append.")
            return

        # --- Step 3: Merge and deduplicate ---
        combined_map = {}
        for f in all_fundings:
            key = f.get("id") or f.get("timestamp")
            if key:
                combined_map[key] = f

        # Convert back to list, sort by timestamp desc
        combined = list(combined_map.values())
        combined.sort(key=lambda x: int(x.get("timestamp", 0)), reverse=True)

        # --- Step 4: Save back ---
        fieldnames = sorted(set().union(*(d.keys() for d in combined)))
        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(combined)

        logger.info(f"[ExtendedAPI] ✅ Saved {len(combined)} total fundings (sorted desc) → {filename}")



        
    async def getTrades(self):
        try:
            filename = "database/trades_ext.csv"
            limit = 300
            cursor = None
            all_trades = []

            # --- Step 1: Determine latest timestamp ---
            newest_timestamp = 0
            existing_fieldnames = None
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

                await asyncio.sleep(0.1)


            # --- Step 3: Save result ---
            if not all_trades:
                logger.info("[ExtendedAPI] No new trades to save.")
                return

            trade_dicts = [t.__dict__ for t in all_trades]

            # ✅ Merge with existing data (if file exists)
            if os.path.exists(filename):
                with open(filename, newline="", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    existing = list(reader)
                trade_dicts.extend(existing)

            # ✅ Sort all by created_time descending
            trade_dicts.sort(key=lambda x: int(x.get("created_time", 0)), reverse=True)

            # ✅ Write everything back (overwrite)
            fieldnames = sorted(set().union(*(d.keys() for d in trade_dicts)))
            with open(filename, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(trade_dicts)

            logger.info(f"[ExtendedAPI] ✅ Saved {len(trade_dicts)} total trades (sorted desc) → {filename}")



        except Exception as e:
            logger.error(f"[ExtendedAPI] getTrades() failed: {e}")


    def mergeTrades(self):
        input_file  = "database/trades_ext.csv"
        output_file = "database/trades_merged_ext.csv"

        trades_by_market = defaultdict(list)

        # Read trades
        with open(input_file, newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                row["price"] = float(row["price"])
                row["qty"] = float(row["qty"])
                row["value"] = float(row["value"])
                row["fee"] = float(row["fee"]) if row["fee"] else 0.0

                # ✅ created_time is timestamp (ms → datetime)
                if row["created_time"].isdigit():
                    ts = int(row["created_time"])
                    if ts > 1e12:  # milliseconds
                        row["created_time"] = datetime.fromtimestamp(ts / 1000)
                    else:  # seconds
                        row["created_time"] = datetime.fromtimestamp(ts)
                else:
                    # fallback if it's already a string (YYYY-MM-DD HH:MM:SS)
                    row["created_time"] = datetime.strptime(row["created_time"], "%Y-%m-%d %H:%M:%S")

                trades_by_market[row["market"]].append(row)

        merged = []

        for market, trades in trades_by_market.items():
            # Sort by time ascending
            trades.sort(key=lambda x: x["created_time"])

            running_qty = 0.0
            entry_trade_id = None
            entry_time = None
            entry_side = None

            qty_opened = 0.0
            entry_price_total = 0.0
            qty_closed = 0.0
            exit_price_total = 0.0
            total_fee = 0.0  # 🧾 New accumulator

            for t in trades:
                side = t["side"].upper()
                qty = float(t["qty"])
                price = float(t["price"])
                fee = float(t["fee"])
                trade_id = t["id"]
                time = t["created_time"]

                total_fee += fee  # 🧾 accumulate fee for all trades

                # If no running position, start new one
                if abs(running_qty) < 1e-12:
                    qty_opened = qty_closed = entry_price_total = exit_price_total = 0.0
                    total_fee = 0.0  # reset when starting a new position
                    total_fee += fee
                    entry_side = side
                    entry_trade_id = trade_id
                    entry_time = time

                # Same direction as entry -> open
                if side == entry_side:
                    qty_opened += qty
                    entry_price_total += price * qty
                    running_qty += qty if entry_side == "BUY" else -qty
                else:
                    # Opposite direction -> close
                    qty_closed += qty
                    exit_price_total += price * qty
                    running_qty += qty if side == "BUY" and entry_side == "SELL" else -qty

                # Position closed (flat)
                if abs(running_qty) < 1e-8 and entry_trade_id:
                    avg_entry_price = (entry_price_total / qty_opened) if qty_opened > 0 else 0.0
                    avg_exit_price = (exit_price_total / qty_closed) if qty_closed > 0 else 0.0

                    pnl = 0.0
                    if qty_closed > 0:
                        if entry_side == "BUY":
                            pnl = (avg_exit_price - avg_entry_price) * qty_closed
                        else:
                            pnl = (avg_entry_price - avg_exit_price) * qty_closed

                    # Deduct fee from PnL
                    net_pnl = pnl - total_fee

                    merged.append({
                        "market": market,
                        "side": entry_side,
                        "entry_time": entry_time.strftime("%Y-%m-%d %H:%M:%S"),
                        "exit_time": time.strftime("%Y-%m-%d %H:%M:%S"),
                        "qty_opened": round(qty_opened, 8),
                        "qty_closed": round(qty_closed, 8),
                        "avg_entry_price": round(avg_entry_price, 8),
                        "avg_exit_price": round(avg_exit_price, 8),
                        "pnl": round(pnl, 8),
                        "net_pnl": round(net_pnl, 8),  # 🧾 New column
                        "fee_total": round(total_fee, 8),  # 🧾 New column
                        "entry_trade_id": entry_trade_id,
                        "exit_trade_id": trade_id,
                        "status": "CLOSED",
                    })

                    # reset for next position
                    running_qty = 0.0
                    entry_trade_id = entry_time = entry_side = None
                    qty_opened = qty_closed = entry_price_total = exit_price_total = total_fee = 0.0

            # Handle open positions
            if entry_trade_id:
                avg_entry_price = (entry_price_total / qty_opened) if qty_opened > 0 else 0.0
                merged.append({
                    "market": market,
                    "side": entry_side,
                    "entry_time": entry_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "exit_time": "",
                    "qty_opened": round(qty_opened, 8),
                    "qty_closed": round(qty_closed, 8),
                    "avg_entry_price": round(avg_entry_price, 8),
                    "avg_exit_price": "" if qty_closed == 0 else round((exit_price_total / qty_closed), 8),
                    "pnl": "" if qty_closed == 0 else round((( (exit_price_total/qty_closed) - avg_entry_price ) * qty_closed) if entry_side == "BUY" else (((avg_entry_price - (exit_price_total/qty_closed)) * qty_closed)), 8),
                    "net_pnl": "",  # still open
                    "fee_total": round(total_fee, 8),
                    "entry_trade_id": entry_trade_id,
                    "exit_trade_id": "",
                    "status": "OPEN",
                })

        # Sort by entry_time desc
        merged.sort(key=lambda x: x["entry_time"] or "0000-00-00 00:00:00", reverse=True)

        # Write output
        with open(output_file, "w", newline="") as f:
            fieldnames = [
                "market", "side", "entry_time", "exit_time",
                "qty_opened", "qty_closed",
                "avg_entry_price", "avg_exit_price", "pnl", "net_pnl", "fee_total",
                "entry_trade_id", "exit_trade_id", "status"
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(merged)

        print(f"✅ Merged {len(merged)} trades saved to {output_file}")


    def calculateDailyPnL(self):
        input_file      = "database/trades_merged_ext.csv"
        output_file     = "database/trades_daily_pnl_ext.csv"

        if not os.path.exists(input_file):
            print("❌ trades_ext.csv not found.")
            return

        daily_pnl               = defaultdict(float)
        daily_volume            = defaultdict(float)

        with open(input_file, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if not row["net_pnl"] or not row["exit_time"]:
                    continue

                try:
                    date_str    = row["exit_time"].split(" ")[0]  # YYYY-MM-DD
                    pnl         = float(row["net_pnl"])
                    
                    entryvolume = float(row.get("qty_opened", 0)) * float(row.get("avg_entry_price", 0))  # fallback to 0 if missing
                    exitvolume  = float(row.get("qty_closed", 0)) * float(row.get("avg_exit_price", 0))  # fallback to 0 if missing

                    daily_pnl[date_str]     += pnl
                    daily_volume[date_str]  += entryvolume + exitvolume

                except Exception as e:
                    print(f"⚠️ Skipping row due to error: {e}")

        # Sort by date ascending
        sorted_daily        = sorted(daily_pnl.items(), key=lambda x: x[0], reverse=True)

        with open(output_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["date", "daily_net_pnl", "daily_volume_usd"])
            for date, pnl in sorted_daily:
                writer.writerow([date, round(pnl, 2), round(daily_volume[date], 2)])

        print(f"✅ Daily PnL summary saved to {output_file}")
