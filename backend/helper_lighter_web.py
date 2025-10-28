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
            "private_key"       : os.getenv("PRIVATE_KEY"),
            "account_index"     : int(os.getenv("ACCOUNT_INDEX")),
            "api_key_index"     : int(os.getenv("API_KEY_INDEX")),
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

    async def getTrades(self, limit: int = 100):
        try:
            # ✅ Get (and refresh) auth token automatically
            auth = await self._get_auth_token()
            if not auth:
                return

            # 3️⃣ Prepare request params
            params = {
                "auth": auth,
                "account_index": self.config["account_index"],
                "sort_by": "timestamp",
                "sort_dir": "desc",
                "limit": limit,
            }

            url = f"{self.config['base_url']}/api/v1/trades"
            headers = {
                "accept": "application/json",
                "authorization": auth
            }

            # 4️⃣ Send GET request
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, params=params) as resp:
                    if resp.status != 200:
                        logger.error(f"Failed to fetch trades: {resp.status}")
                        return
                    data = await resp.json()

            fetched_trades = data.get("trades", [])

            # 5️⃣ Load existing trades and IDs
            filename = "trades_lig.csv"
            existing_trades = [] # Store existing trades to combine with new ones
            existing_ids = set()
            if os.path.exists(filename):
                with open(filename, "r", newline="", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        existing_ids.add(row["trade_id"])
                        existing_trades.append(row) # Keep existing trades

            # 6️⃣ Prepare new trades (only the ones not already recorded)
            new_trades = []
            new_trades_count = 0
            for t in fetched_trades:
                trade_id_str = str(t["trade_id"]) 
                if trade_id_str in existing_ids:
                    continue

                readable_time = datetime.fromtimestamp(
                    t["timestamp"] / 1000
                ).strftime("%Y-%m-%d %H:%M:%S")

                market_id = str(t["market_id"])
                symbol = self.market_map.get(market_id, f"UNKNOWN_MARKET_{market_id}")

                new_trade = {
                    "symbol": symbol,
                    "trade_id": trade_id_str, # Use string for consistency
                    "tx_hash": t["tx_hash"],
                    "market_id": market_id,
                    "side": "SELL" if t["ask_account_id"]==self.config["account_index"] else "BUY",
                    "size": t["size"],
                    "price": t["price"],
                    "usd_amount": t["usd_amount"],
                    "block_height": t["block_height"],
                    "timestamp": readable_time
                }
                new_trades.append(new_trade)
                new_trades_count += 1
                
            if not new_trades:
                logger.info("No new trades to add, CSV will be rewritten with existing data (if any).")
                if not existing_trades:
                    return

            # Combine all trades: existing + newly fetched and formatted
            all_trades = new_trades + existing_trades
            
            # 7️⃣ Sort all trades newest → oldest (essential for file integrity)
            # Sorting by timestamp (the formatted string) ensures order.
            all_trades.sort(key=lambda x: x["timestamp"], reverse=True)


            # 8️⃣ Write (rewrite/overwrite the file)
            fieldnames = [
                "symbol", "trade_id", "tx_hash", "market_id",
                "side", "size", "price", "usd_amount",
                "block_height", "timestamp"
            ]
            
            # Use "w" mode to overwrite the file
            with open(filename, "w", newline="", encoding="utf-8") as f: 
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                
                # Always write the header since we are overwriting
                writer.writeheader() 
                
                # Write the complete, sorted list of all unique trades
                writer.writerows(all_trades)

            # NOTE: The original logger.info used 'symbol', which might be from the last trade.
            # It's better to use a more generic message or the count.
            logger.info(f"✅ Rewrote {filename} with {len(all_trades)} unique trades (added {new_trades_count} new).")

        except Exception as e:
            logger.error(f"⚠️ Error in getTrades: {e}")
            
    def mergeTrades(self):
        INPUT_FILE = "trades_lig.csv"
        OUTPUT_FILE = "trades_merged_lig.csv"

        trades_by_symbol = defaultdict(list)

        # Step 1: Load all trades
        with open(INPUT_FILE, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                row["size"] = float(row["size"])
                row["price"] = float(row["price"])
                row["usd_amount"] = float(row["usd_amount"])
                row["block_height"] = int(row["block_height"])
                row["timestamp"] = datetime.strptime(row["timestamp"], "%Y-%m-%d %H:%M:%S")
                trades_by_symbol[row["symbol"]].append(row)
        
        merged_trades = []

        # Step 2: Process each symbol
        for symbol, trades in trades_by_symbol.items():
            trades.sort(key=lambda x: x["timestamp"])
            cumulative_qty = 0
            running_trade = None

            for trade in trades:
                size = trade["size"] if trade["side"].upper() == "BUY" else -trade["size"]

                # Open a new position
                if cumulative_qty == 0 and size != 0:
                    running_trade = {
                        "symbol": symbol,
                        "side": "BUY" if size > 0 else "SELL",
                        "start_time": trade["timestamp"],
                        "end_time": trade["timestamp"],
                        "qty_opened": abs(size),
                        "qty_closed": 0.0,
                        "avg_entry_price": trade["price"],
                        "avg_exit_price": 0.0,
                        "entry_value": trade["usd_amount"],
                        "exit_value": 0.0,
                        "start_trade_id": trade["trade_id"],
                        "end_trade_id": trade["trade_id"],
                        "status": "OPEN",
                        "pnl_usd": 0.0
                    }

                elif running_trade:
                    running_trade["end_time"] = trade["timestamp"]
                    prev_opened = running_trade["qty_opened"]
                    prev_closed = running_trade["qty_closed"]

                    # Add to position (same direction)
                    if (size > 0 and running_trade["side"] == "BUY") or (size < 0 and running_trade["side"] == "SELL"):
                        running_trade["qty_opened"] += abs(size)
                        total_opened_now = running_trade["qty_opened"]
                        running_trade["avg_entry_price"] = (
                            (running_trade["avg_entry_price"] * prev_opened + trade["price"] * abs(size))
                            / total_opened_now
                        )
                        running_trade["entry_value"] += trade["usd_amount"]

                    # Reduce position (opposite direction)
                    else:
                        running_trade["qty_closed"] += abs(size)
                        total_closed_now = running_trade["qty_closed"]
                        running_trade["avg_exit_price"] = (
                            (running_trade["avg_exit_price"] * prev_closed + trade["price"] * abs(size))
                            / total_closed_now
                        )
                        running_trade["exit_value"] += trade["usd_amount"]

                    running_trade["end_trade_id"] = trade["trade_id"]

                cumulative_qty += size

                # Close when fully flat
                if abs(cumulative_qty) < 1e-8 and running_trade:
                    cumulative_qty = 0
                    running_trade["status"] = "CLOSED"
                    merged_trades.append(running_trade)
                    running_trade = None

            # Save remaining open trade (no exit yet)
            if running_trade:
                if abs(running_trade["qty_opened"] - running_trade["qty_closed"]) < 1e-8:
                    running_trade["status"] = "CLOSED"
                else:
                    running_trade["status"] = "OPEN"
                    running_trade["end_time"] = ""   # 🟢 leave blank for open trades
                merged_trades.append(running_trade)

        # Step 3: Calculate PnL
        for mt in merged_trades:
            if mt["status"] == "CLOSED" and mt["qty_closed"] > 0:
                if mt["side"] == "BUY":
                    mt["pnl_usd"] = (mt["avg_exit_price"] - mt["avg_entry_price"]) * mt["qty_closed"]
                else:
                    mt["pnl_usd"] = (mt["avg_entry_price"] - mt["avg_exit_price"]) * mt["qty_closed"]
            else:
                mt["pnl_usd"] = 0.0

        # Step 4: Sort (newest first)
        merged_trades.sort(key=lambda x: x["start_time"] if isinstance(x["start_time"], datetime) else datetime.min, reverse=True)

        # Step 5: Write output
        fieldnames = [
            "symbol", "side", "start_time", "end_time",
            "qty_opened", "qty_closed", "avg_entry_price", "avg_exit_price",
            "entry_value", "exit_value", "pnl_usd",
            "start_trade_id", "end_trade_id", "status"
        ]
        with open(OUTPUT_FILE, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for mt in merged_trades:
                mt["start_time"] = mt["start_time"].strftime("%Y-%m-%d %H:%M:%S") if isinstance(mt["start_time"], datetime) else mt["start_time"]
                if isinstance(mt["end_time"], datetime):
                    mt["end_time"] = mt["end_time"].strftime("%Y-%m-%d %H:%M:%S")
                writer.writerow(mt)

        print(f"✅ Merged trades saved to {OUTPUT_FILE}")
        print(f"Total positions: {len(merged_trades)} (including running ones).")

    def calculateDailyPnL(self):
        """
        Summarize daily PnL (USD) and daily volume from merged trades.
        Only CLOSED trades with valid end_time are counted.
        Output: trades_daily_pnl_lig.csv
        """
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
                if not row["end_time"] or row["status"].upper() != "CLOSED":
                    continue
                try:
                    date_str = row["end_time"].split(" ")[0]  # YYYY-MM-DD
                    pnl = float(row["pnl_usd"])
                    entryvolume = float(row.get("entry_value", 0))  # fallback to 0 if missing
                    exitvolume  = float(row.get("exit_value", 0))  # fallback to 0 if missing

                    daily_pnl[date_str] += pnl
                    daily_volume[date_str] += entryvolume + exitvolume
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

    async def init_getTrades(self, page_size: int = 100, max_retries: int = 5):
        """
        Fetch all historical trades for the current account using pagination.
        Retries automatically on any aiohttp/network error.
        """
        try:
            filename = "trades_lig.csv"

            fieldnames = [
                "symbol", "trade_id", "tx_hash", "market_id",
                "side", "size", "price", "usd_amount",
                "block_height", "timestamp"
            ]


            # Create new CSV with header if missing
            if not os.path.exists(filename):
                with open(filename, "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                logger.info("🆕 trades_lig.csv not found — starting full position sync.")
            else:
                logger.info("📂 trades_lig.csv found — stopping init.")
                return


            auth = await self._get_auth_token()
            if not auth:
                logger.error("❌ Auth token unavailable.")
                return

            url = f"{self.config['base_url']}/api/v1/trades"
            headers = {
                "accept": "application/json",
                "authorization": auth
            }

            params = {
                "account_index": self.config["account_index"],
                "sort_by": "timestamp",
                "sort_dir": "desc",
                "limit": page_size,
            }

            with open(filename, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()

            logger.info("🚀 Starting full trade sync from Lighter...")

            cursor = None
            total_fetched = 0

            async with aiohttp.ClientSession() as session:
                while True:
                    if cursor:
                        params["cursor"] = cursor

                    # --- retry loop for any failure ---
                    for attempt in range(max_retries):
                        try:
                            async with session.get(url, headers=headers, params=params, timeout=30) as resp:
                                data = await resp.json()
                                break  # ✅ success, exit retry loop
                        except Exception as e:
                            wait_time = 2 ** attempt
                            logger.warning(f"⚠️ Request failed ({type(e).__name__}: {e}). Retrying in {wait_time}s...")
                            await asyncio.sleep(wait_time)
                    else:
                        logger.error("❌ Max retries reached. Stopping pagination.")
                        break
                    # -----------------------------------

                    trades = data.get("trades", [])
                    next_cursor = data.get("next_cursor")

                    if not trades:
                        logger.info("✅ No more trades found.")
                        break

                    # Write each batch incrementally
                    with open(filename, "a", newline="", encoding="utf-8") as f:
                        writer = csv.DictWriter(f, fieldnames=fieldnames)
                        for t in trades:
                            trade_id_str = str(t["trade_id"])
                            readable_time = datetime.fromtimestamp(t["timestamp"] / 1000).strftime("%Y-%m-%d %H:%M:%S")
                            market_id = str(t["market_id"])
                            symbol = self.market_map.get(market_id, f"UNKNOWN_MARKET_{market_id}")
                            side = "SELL" if t["ask_account_id"] == self.config["account_index"] else "BUY"

                            writer.writerow({
                                "symbol": symbol,
                                "trade_id": trade_id_str,
                                "tx_hash": t["tx_hash"],
                                "market_id": market_id,
                                "side": side,
                                "size": t["size"],
                                "price": t["price"],
                                "usd_amount": t["usd_amount"],
                                "block_height": t["block_height"],
                                "timestamp": readable_time,
                            })

                    total_fetched += len(trades)
                    logger.info(f"📦 Fetched {len(trades)} trades (total {total_fetched}).")

                    if not next_cursor:
                        break
                    cursor = next_cursor

                    # small cooldown between pages
                    await asyncio.sleep(1)

            logger.info(f"✅ Saved {total_fetched} total trades to {filename}")
            print(f"✅ Saved {total_fetched} total trades to {filename}")

        except Exception as e:
            logger.error(f"⚠️ Fatal error in init_getTrades: {e}", exc_info=True)
            print(f"⚠️ Fatal error in init_getTrades: {e}")
