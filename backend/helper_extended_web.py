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
        
    # async def getTrades(self):
    #     try:
    #         resp                    = await self.client.account.get_trades(market_names=self.allSymbols)
    #         logger.info(f"Fetched {len(resp.data)} trades")
    #         await self.saveTradesToCsv(resp.data)

    #     except Exception as e:
    #         logger.error(f"⚠️ Error getTrades: {e}")
        

    async def getTrades(self):
        """
        Fetch latest position history (instead of trades).
        If trades_ext.csv exists, append only newer positions.
        """
        try:
            resp = await self.client.account.get_positions_history(
                market_names=self.allSymbols,
                limit=200
            )
            if not resp or not resp.data:
                logger.info("No positions found.")
                return

            logger.info(f"Fetched {len(resp.data)} positions.")
            await self.savePositionsToCsv(resp.data)

        except Exception as e:
            logger.error(f"⚠️ Error getTrades (positions): {e}")
            traceback.print_exc()

    async def init_getTrades(self, page_size: int = 300, max_retries: int = 5):
        try:
            filename        = "trades_ext.csv"
            fieldnames      = [
                "id", "account_id", "market", "side", "leverage",
                "size", "open_price", "exit_price", "exit_type",
                "realised_pnl", "created_time", "closed_time", "created_at", "closed_at"
            ]

            with open(filename, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()

            total_fetched   = 0
            cursor          = None
            all_positions   = []

            logger.info("🚀 Starting full Extended positions sync...")

            while True:
                for attempt in range(max_retries):
                    try:
                        resp                = await self.client.account.get_positions_history(
                            market_names    = self.allSymbols,
                            cursor          = cursor,
                            limit           = page_size
                        )
                        break  # ✅ success
                    except Exception as e:
                        wait_time           = 2 ** attempt
                        logger.warning      (f"⚠️ Request failed ({type(e).__name__}: {e}). Retrying in {wait_time}s...")
                        await asyncio.sleep(wait_time)
                else:
                    logger.error("❌ Max retries reached. Aborting pagination.")
                    break

                if not resp or not resp.data:
                    logger.info("✅ No more positions found.")
                    break

                positions       = resp.data
                total_fetched   += len(positions)
                all_positions.extend(positions)

                logger.info(f"📦 Fetched {len(positions)} positions (total {total_fetched}).")

                # Handle pagination
                next_cursor = None
                if hasattr(resp, "pagination") and resp.pagination:
                    next_cursor = getattr(resp.pagination, "cursor", None)
                elif isinstance(resp, dict):
                    next_cursor = resp.get("pagination", {}).get("cursor")

                if not next_cursor:
                    break
                cursor          = next_cursor

                await asyncio.sleep(1)

            # Write all positions to CSV
            with open(filename, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for p in all_positions:
                    created_readable    = (datetime.fromtimestamp(p.created_time / 1000).strftime("%Y-%m-%d %H:%M:%S"))
                    closed_readable     = (datetime.fromtimestamp(p.closed_time / 1000).strftime("%Y-%m-%d %H:%M:%S") if isinstance(p.closed_time, (int, float)) and p.closed_time > 0 else "")
                    writer.writerow({
                        "id"            : p.id,
                        "account_id"    : p.account_id,
                        "market"        : p.market,
                        "side"          : p.side,
                        "leverage"      : p.leverage,
                        "size"          : p.max_position_size,
                        "open_price"    : p.open_price,
                        "exit_price"    : p.exit_price,
                        "exit_type"     : getattr(p, "exit_type", ""),
                        "realised_pnl"  : p.realised_pnl,
                        "created_time"  : p.created_time,
                        "closed_time"   : p.closed_time,
                        "created_at"    : created_readable,
                        "closed_at"     : closed_readable
                    })

            logger.info(f"✅ Saved {total_fetched} total positions to {filename}")
            print(f"✅ Saved {total_fetched} total positions to {filename}")

        except Exception as e:
            logger.error(f"⚠️ Fatal error in init_getTrades: {e}", exc_info=True)
            print(f"⚠️ Fatal error in init_getTrades: {e}")


    async def savePositionsToCsv(self, positions):
        try:
            filename            = "trades_ext.csv"
            fieldnames          = [
                "id", "account_id", "market", "side", "leverage",
                "size", "open_price", "exit_price", "exit_type",
                "realised_pnl", "created_time", "closed_time", "created_at", "closed_at"
            ]

            existing_rows       = []
            newest_timestamp    = 0
            if os.path.exists(filename):
                with open(filename, "r", newline="", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    existing_rows = list(reader)
                    if existing_rows:
                        newest_timestamp = max(
                            int(row["created_time"]) for row in existing_rows if row["created_time"].isdigit()
                        )

            new_positions = [p for p in positions if p.created_time > newest_timestamp]
            if not new_positions:
                logger.info("No new positions to save.")
                return

            new_rows = []
            for p in new_positions:
                created_readable = datetime.fromtimestamp(p.created_time / 1000).strftime("%Y-%m-%d %H:%M:%S")
                closed_readable = (
                    datetime.fromtimestamp(p.closed_time / 1000).strftime("%Y-%m-%d %H:%M:%S")
                    if isinstance(p.closed_time, (int, float)) and p.closed_time > 0 else ""
                )
                new_rows.append({
                        "id"            : p.id,
                        "account_id"    : p.account_id,
                        "market"        : p.market,
                        "side"          : p.side,
                        "leverage"      : p.leverage,
                        "size"          : p.max_position_size,
                        "open_price"    : p.open_price,
                        "exit_price"    : p.exit_price,
                        "exit_type"     : getattr(p, "exit_type", ""),
                        "realised_pnl"  : p.realised_pnl,
                        "created_time"  : p.created_time,
                        "closed_time"   : p.closed_time,
                        "created_at"    : created_readable,
                        "closed_at"     : closed_readable
                    })

            all_rows                    = existing_rows + new_rows
            all_rows.sort(key=lambda x: int(x["created_time"]), reverse=True)

            with open(filename, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(all_rows)

            logger.info(f"✅ Added {len(new_positions)} new positions to {filename}.")

        except Exception as e:
            logger.error(f"⚠️ Error saving positions to CSV: {e}")









    async def saveTradesToCsv(self, trades):
        try:
            filename = "trades_ext.csv"
            fieldnames = [
                "id", "account_id", "market", "order_id", "side", "price",
                "qty", "value", "fee", "is_taker", "trade_type",
                "created_time", "created_at"
            ]

            # Step 1: Load existing trades
            existing_rows = []
            newest_timestamp = 0
            if os.path.exists(filename):
                with open(filename, mode="r", newline="", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    existing_rows = list(reader)
                    if existing_rows:
                        # Find newest created_time from existing rows
                        newest_timestamp = max(int(row["created_time"]) for row in existing_rows if row["created_time"].isdigit())

            # Step 2: Keep only trades newer than newest_timestamp
            new_trades = [t for t in trades if t.created_time > newest_timestamp]

            if not new_trades:
                logging.info("No new trades to save (all older than newest on file).")
                return

            # Step 3: Convert new trades to dicts (with readable timestamp)
            new_rows = []
            for t in new_trades:
                readable_time = datetime.fromtimestamp(t.created_time / 1000).strftime("%Y-%m-%d %H:%M:%S")
                new_rows.append({
                    "id": t.id,
                    "account_id": t.account_id,
                    "market": t.market,
                    "order_id": t.order_id,
                    "side": t.side,
                    "price": str(t.price),
                    "qty": str(t.qty),
                    "value": str(t.value),
                    "fee": str(t.fee),
                    "is_taker": t.is_taker,
                    "trade_type": t.trade_type,
                    "created_time": t.created_time,
                    "created_at": readable_time
                })

            # Step 4: Combine and sort by created_time desc
            all_rows = existing_rows + new_rows
            all_rows.sort(key=lambda x: int(x["created_time"]), reverse=True)

            # Step 5: Rewrite file
            with open(filename, mode="w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(all_rows)

            logging.info(f"✅ Added {len(new_trades)} new trades after {newest_timestamp}, sorted by created_time desc")

        except Exception as e:
            logging.error(f"⚠️ Error saving to CSV: {e}")

    def mergeTrades(self):
        input_file  = "trades_ext.csv"
        output_file = "trades_merged_ext.csv"

        trades_by_market = defaultdict(list)

        # Read trades
        with open(input_file, newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                row["price"] = float(row["price"])
                row["qty"] = float(row["qty"])
                row["value"] = float(row["value"])
                row["fee"] = float(row["fee"]) if row["fee"] else 0.0
                row["created_time"] = datetime.strptime(row["created_at"], "%Y-%m-%d %H:%M:%S")
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
        input_file      = "trades_ext.csv"
        output_file     = "trades_daily_pnl_ext.csv"

        if not os.path.exists(input_file):
            print("❌ trades_ext.csv not found.")
            return

        daily_pnl               = defaultdict(float)
        daily_volume            = defaultdict(float)

        with open(input_file, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Skip rows without realised_pnl or closed_at (open trades)
                if not row["realised_pnl"] or not row["closed_at"]:
                    continue

                try:
                    date_str    = row["closed_at"].split(" ")[0]  # YYYY-MM-DD
                    pnl         = float(row["realised_pnl"])
                    
                    entryvolume = float(row.get("size", 0)) * float(row.get("open_price", 0))  # fallback to 0 if missing
                    exitvolume  = float(row.get("size", 0)) * float(row.get("exit_price", 0))  # fallback to 0 if missing

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
