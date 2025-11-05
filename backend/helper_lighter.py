import json
import os
import asyncio
import aiohttp
import logging
import lighter
import time
from lighter import WsClient
from decimal import Decimal
from dotenv import load_dotenv
from helpers import HELPERS
load_dotenv()

logger                          = logging.getLogger("helper_lighter")
logger.setLevel                 (logging.INFO)
load_dotenv                     ()

class LighterAPI:
    def __init__(self, symbol: str):
        self.client             = None
        self.ws_client          = None
        self.config             = {
            "base_url"          : "https://mainnet.zklighter.elliot.ai",
            "private_key"       : os.getenv("LIGHTER_API_PRIVATE_KEY"),
            "account_index"     : int(os.getenv("LIGHTER_ACCOUNT_INDEX")),
            "api_key_index"     : int(os.getenv("LIGHTER_API_KEY_INDEX")),
            "slippage"          : float(os.getenv("ALLOWED_SLIPPAGE")) / 100,
        }
        self.pair               = {
            "symbol"            : symbol,
            "market_id"         : None,
            "size_decimals"     : None,
            "price_decimals"    : None,
            "min_size"          : None,
            "min_value"         : None,
        }
        self.ob                 = {
            "bidPrice"          : 0.0,
            "askPrice"          : 0.0,
            "bidSize"           : 0.0,
            "askSize"           : 0.0,
        }
        self.accountData        = {
            "qty"               : 0.0,
            "entry_price"       : 0.0,
            "all_inv_value"     : 0.0,
        }
        self.wsCallback         = None
        self.invValue           = None
        self.currFundRate       = None
        self._wsFundingTask     = None

    async def init(self):
        self.client             = lighter.SignerClient(
            url                 = self.config["base_url"],
            private_key         = self.config["private_key"],
            account_index       = self.config["account_index"],
            api_key_index       = self.config["api_key_index"],
        )


    async def initPair(self):
        symbol                  = self.pair["symbol"]
        url                     = f"{self.config["base_url"]}/api/v1/orderBookDetails"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    raise Exception(f"Failed to fetch market metadata: {resp.status}")
                data            = await resp.json()

        details                 = data.get("order_book_details", [])
        match                   = next((d for d in details if d["symbol"].upper() == symbol.upper()), None)
        if not match:
            raise Exception(f"Symbol {symbol} not found in orderBookDetails")

        self.pair = {
            "symbol"                    : self.pair["symbol"],
            "market_id"                 : int(match["market_id"]),
            "size_decimals"             : int(match["size_decimals"]),
            "price_decimals"            : int(match["price_decimals"]),
            "min_size"                  : float(match["min_base_amount"]),
            "min_value"                 : float(match["min_quote_amount"]),   
        }
                

    async def startWsFunding(self):
        market_id           = self.pair["market_id"]
        if self._wsFundingTask and not self._wsFundingTask.done():
            logger.info("wsFunding already running; skipping new start.")
            return

        logger.info("[Funding WS] startWsFunding() called")

        async def _run():
            url             = "wss://mainnet.zklighter.elliot.ai/stream"
            sub_msg         = {"type": "subscribe","channel": f"market_stats/{market_id}"}

            while True:
                try:
                    async with aiohttp.ClientSession() as session:
                        async with session.ws_connect(url, heartbeat=30) as ws:
                            await ws.send_str(json.dumps(sub_msg))
                            logger.info(f"[Funding WS] Subscribed to market_stats for market_id={market_id}")
                            async for msg in ws:
                                if msg.type == aiohttp.WSMsgType.TEXT:
                                    try:
                                        data            = json.loads(msg.data)
                                        message_type    = data.get("type")
                                        if message_type == "ping":
                                            await ws.send_str(json.dumps({"type": "pong"}))
                                            continue
                                        if message_type in ["update/market_stats", "market_stats"]:
                                            mstats      = data.get("market_stats") or {}
                                            fr          = mstats.get("current_funding_rate") or mstats.get("funding_rate")
                                            if fr is not None:
                                                try:
                                                    self.currFundRate = float(fr)
                                                except (TypeError, ValueError):
                                                    pass
                                    except Exception as e:
                                        logger.error(f"[Funding WS] parse error: {e}")
                                elif msg.type == aiohttp.WSMsgType.ERROR:
                                    raise RuntimeError(f"WebSocket error: {ws.exception()}")
                            logger.warning("[Funding WS] socket closed; reconnecting...")

                except Exception as e:
                    self.currFundRate = None
                    logger.error(f"[Funding WS] disconnected: {e} — retrying in 1s")
                    await asyncio.sleep(1)

        self._wsFundingTask = asyncio.create_task(_run())



    async def startWs(self, wsCallback):
        self.wsCallback                         = wsCallback
        async def run_ws():
            while True:
                try:
                    self.ws_client              = WsClient(
                        order_book_ids          = [self.pair["market_id"]],
                        on_order_book_update    = self._handle_orderbook_update,
                        account_ids             = [self.config["account_index"]],
                        on_account_update       = self._handle_account_update
                    )
                    # logger.info('✅ WS Lighter Connected')
                    await self.ws_client.run_async()

                except Exception as e:
                    # logger.error(f"⚠️ WS Lighter Disconnected")
                    self.ob = {
                        "bidPrice": 0.0,
                        "askPrice": 0.0,
                        "bidSize" : 0.0,
                        "askSize" : 0.0,
                    }
        asyncio.create_task(run_ws())
        
    def _handle_orderbook_update(self, market_id, order_book):
        try:
            if isinstance(order_book, dict) and order_book.get("type") == "ping":
                return

            # Sort by price
            order_book["bids"].sort(key=lambda x: float(x["price"]), reverse=True)
            order_book["asks"].sort(key=lambda x: float(x["price"]))

            # --- find first valid bid ---
            bid = next((b for b in order_book["bids"] if float(b["size"]) > 0), None)
            ask = next((a for a in order_book["asks"] if float(a["size"]) > 0), None)

            if not bid or not ask:
                logger.warning(f"No valid bid/ask found for {market_id}")
                return

            self.ob = {
                "bidPrice": float(bid["price"]),
                "askPrice": float(ask["price"]),
                "bidSize": float(bid["size"]),
                "askSize": float(ask["size"]),
            }

            self.wsCallback('l_ob')

        except Exception as e:
            logger.error(f"Lighter handler error: {e}")

    def _handle_account_update(self, account_id, account):
        try:
            if isinstance(account, dict) and account.get("type") == "ping":
                return
            if isinstance(account, str):
                account     = json.loads(account)
                
            positions       = account.get("positions")
            all_inv_value   = 0.0
            for pos in positions.values():
                try:
                    position_value  = float(pos.get("position_value", "0"))
                    all_inv_value   += position_value
                except Exception as e:
                    logger.warning(f"Error parsing position for {pos.get('symbol', '?')}: {e}")

            self.invValue           = all_inv_value
            self.wsCallback("l_acc")

        except Exception as e:
            logger.error(f"Lighter handler error: {e}")

            
    async def placeMarketOrder(self, side: str, order_qty: float, isReduceOnly, max_retries=10, delay=0.5):
        market_index            = self.pair["market_id"]
        size_decimals           = self.pair["size_decimals"]
        price_decimals          = self.pair["price_decimals"]
        ob                      = self.ob
        if not ob or not ob.get("bidPrice") or not ob.get("askPrice"):
            logger.warning     ("No orderbook data yet, cannot place market order")
            return None

        if side.upper() == "BUY":
            best_ask            = float(ob["askPrice"])
            price               = best_ask * (1 + self.config["slippage"])
            is_ask              = False
        elif side.upper() == "SELL":
            best_bid            = float(ob["bidPrice"])
            price               = best_bid * (1 - self.config["slippage"])
            is_ask              = True
        else:
            logger.error("Invalid side, must be BUY or SELL")
            return None
        
        fix_size                = HELPERS.lighterFmtDecimal(order_qty, size_decimals)
        fix_price               = HELPERS.lighterFmtDecimal(price    , price_decimals)

        return_msg              = f"• PlacingMarketOrder ⭢ [{is_ask}, {fix_size}, {fix_price}]" + '\n'   
        logger.info             ( f"• PlacingMarketOrder ⭢ [{is_ask}, {fix_size}, {fix_price}]")

        # ✅ RETRY LOOP
        for attempt in range(1, max_retries + 1):
            try:
                start_time = time.perf_counter()
                tx, tx_hash, err = await self.client.create_order(
                    market_index        = market_index,
                    client_order_index  = int(asyncio.get_event_loop().time() * 1000),
                    base_amount         = fix_size,
                    price               = fix_price,
                    is_ask              = is_ask,
                    order_type          = lighter.SignerClient.ORDER_TYPE_LIMIT,
                    time_in_force       = lighter.SignerClient.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
                    reduce_only         = 1 if isReduceOnly else 0,
                    trigger_price       = 0,
                )
                
                if err:
                    raise Exception(err)
                else:
                    logger.info(tx_hash)

                end_time                = time.perf_counter()
                latency_ms              = (end_time - start_time) * 1000
                return_msg              += f"• {side} Placed | ⏱ Latency: {latency_ms:.2f} ms" + '\n'
                logger.info             (  f"• {side} Placed | ⏱ Latency: {latency_ms:.2f} ms")
                return return_msg

            except Exception as e:
                logger.error(f"❌ Attempt {attempt}/{max_retries} failed: {e}")

                if attempt < max_retries:
                    await asyncio.sleep(delay)
                else:
                    logger.error("❌ Final failure after all retries")
                    return return_msg + "• FAILED after all retries"


    async def loadPos(self, max_retries=1000, retry_delay=1):
        symbol = self.pair["symbol"]
        account_index = self.config["account_index"]
        url = f"{self.config['base_url']}/api/v1/account?by=index&value={account_index}"

        for attempt in range(1, max_retries + 1):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, headers={"accept": "application/json"}) as resp:
                        if resp.status != 200:
                            logger.warning(f"⚠️ Attempt {attempt}/{max_retries} failed (HTTP {resp.status})")
                            raise Exception(f"Bad HTTP status: {resp.status}")
                        
                        data = await resp.json(content_type=None)  # ← tolerate wrong/missing headers

                # ✅ Check API internal code
                if data.get("code") != 200:
                    logger.warning(f"⚠️ API returned code {data.get('code')} on attempt {attempt}")
                    raise Exception("API returned error code")

                # ✅ Check that accounts exist
                accounts = data.get("accounts", [])
                if not accounts:
                    logger.warning(f"⚠️ No accounts found (attempt {attempt})")
                    raise Exception("Empty accounts in response")

                positions = accounts[0].get("positions", [])
                if not positions:
                    logger.warning(f"⚠️ No positions found (attempt {attempt})")
                    raise Exception("Empty positions in response")

                # ✅ Try to find the current symbol
                current_pos = next((p for p in positions if p["symbol"].upper() == symbol.upper()), None)
                if current_pos:
                    qty = float(current_pos.get("position", "0") or 0) * int(current_pos.get("sign", 1))
                    entry_price = float(current_pos.get("avg_entry_price", "0") or 0)
                    self.accountData = {"qty": qty, "entry_price": entry_price}
                    return self.accountData
                else:
                    logger.warning(f"⚠️ No position found for symbol {symbol} (attempt {attempt})")
                    raise Exception("Symbol not found in positions")

            except Exception as e:
                logger.info(f"Retry {attempt}/{max_retries} — {e}")
                if attempt < max_retries:
                    await asyncio.sleep(retry_delay)
                    continue
                else:
                    logger.error(f"❌ Failed to fetch position for {symbol} after {max_retries} attempts")
                    self.accountData = {"qty": 0, "entry_price": 0}
                    return self.accountData

    # async def loadPos(self):
    #     try:
    #         symbol          = self.pair["symbol"]
    #         account_index   = self.config["account_index"]
    #         url             = f"{self.config['base_url']}/api/v1/account?by=index&value={account_index}"

    #         async with aiohttp.ClientSession() as session:
    #             async with session.get(url, headers={"accept": "application/json"}) as resp:
    #                 if resp.status != 200:
    #                     logger.info(f"⚠️ Failed to loadPos")
    #                     return None
    #                 data = await resp.json()
    #                 logger.info(data)

    #         accounts                = data.get("accounts", [])
    #         if not accounts:
    #             logger.warning("No accounts found in response")
    #             return None

    #         positions               = accounts[0].get("positions", [])
    #         if not positions:
    #             logger.info("No positions found in response")
    #             return

    #         current_pos             = next((p for p in positions if p["symbol"].upper() == symbol.upper()), None)
    #         if current_pos:
    #             qty                 = float(current_pos.get("position", "0")) * int(current_pos.get("sign", 1))
    #             entry_price         = float(current_pos.get("avg_entry_price", "0"))
    #             self.accountData    = {"qty" : qty, "entry_price" : entry_price}
    #         else:
    #             logger.info(f"No position found for symbol {symbol}")
    #             self.accountData    = {"qty": 0, "entry_price": 0}
            
    #         return self.accountData

    #     except Exception as e:
    #         logger.info(f"⚠️ Error fetching position for {symbol}: {str(e)}")
    #         return None


    async def placeOrder(self, side: str, price: float, order_qty: float, max_retries=10, delay=0.5):
        market_index            = self.pair["market_id"]
        size_decimals           = self.pair["size_decimals"]
        price_decimals          = self.pair["price_decimals"]

        if side.upper() == "BUY":
            is_ask              = False
        elif side.upper() == "SELL":
            is_ask              = True
        else:
            logger.error("Invalid side, must be BUY or SELL")
            return None
        
        fix_size                = HELPERS.lighterFmtDecimal(order_qty, size_decimals)
        fix_price               = HELPERS.lighterFmtDecimal(price    , price_decimals)

        return_msg              = f"• PlacingMarketOrder ⭢ [{is_ask}, {fix_size}, {fix_price}]" + '\n'   
        logger.info             ( f"• PlacingMarketOrder ⭢ [{is_ask}, {fix_size}, {fix_price}]")

        # ✅ RETRY LOOP
        for attempt in range(1, max_retries + 1):
            try:
                start_time = time.perf_counter()
                tx, tx_hash, err = await self.client.create_order(
                    market_index        = market_index,
                    client_order_index  = int(asyncio.get_event_loop().time() * 1000),
                    base_amount         = fix_size,
                    price               = fix_price,
                    is_ask              = is_ask,
                    order_type          = lighter.SignerClient.ORDER_TYPE_LIMIT,
                    time_in_force       = lighter.SignerClient.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
                    trigger_price       = 0,
                )
                
                if err:
                    raise Exception(err)
                else:
                    logger.info(tx_hash)

                end_time                = time.perf_counter()
                latency_ms              = (end_time - start_time) * 1000
                return_msg              += f"• {side} Placed | ⏱ Latency: {latency_ms:.2f} ms" + '\n'
                logger.info             (  f"• {side} Placed | ⏱ Latency: {latency_ms:.2f} ms")
                return return_msg

            except Exception as e:
                logger.error(f"❌ Attempt {attempt}/{max_retries} failed: {e}")

                if attempt < max_retries:
                    await asyncio.sleep(delay)
                else:
                    logger.error("❌ Final failure after all retries")
                    return return_msg + "• FAILED after all retries"
    

    async def cancelOrders(self):
        market_index            = self.pair["market_id"]
        try:
            start_time          = time.perf_counter()
            tx, tx_hash, err    = await self.client.cancel_order(
                market_index        = market_index,
                order_index        = 11111111111,
            )
            if err:
                raise Exception(err)
            else:
                logger.info(tx_hash)

            end_time                = time.perf_counter()
            latency_ms              = (end_time - start_time) * 1000
            logger.info             ( f"• All Orders Cancelled | ⏱ Latency: {latency_ms:.2f} ms")

        except Exception as e:
            logger.error(f"❌ Cancel Orders failed: {e}")