import time
import traceback
import os
import asyncio
import logging
import aiohttp
from dotenv import load_dotenv
from decimal import Decimal
from helpers import HELPERS

from x10.perpetual.accounts import StarkPerpetualAccount
from x10.perpetual.configuration import MAINNET_CONFIG
from x10.perpetual.orders import OrderSide as ExtendedOrderSide
from x10.perpetual.trading_client import PerpetualTradingClient
from x10.perpetual.stream_client import PerpetualStreamClient
from x10.perpetual.simple_client.simple_trading_client import BlockingTradingClient

logger                          = logging.getLogger("helper_exended")
logger.setLevel                 (logging.INFO)
load_dotenv                     ()

class ExtendedAPI:
    def __init__(self, symbol: str):
        self.client             = None
        self.simpleClient       = None
        self.ws_client          = None
        self.starkPerpAcc       = None 
        self.config             = {
            "vault_id"          : int(os.getenv("EXTENDED_VAULT_ID")),
            "private_key"       : os.getenv("EXTENDED_PRIVATE_KEY"),
            "public_key"        : os.getenv("EXTENDED_PUBLIC_KEY"),
            "api_key"           : os.getenv("EXTENDED_API_KEY"),
            "slippage"          : float(os.getenv("ALLOWED_SLIPPAGE")) / 100,
        }
        self.pair               = {
            "symbol"            : symbol + "-USD",
            "min_size"          : None,
            "min_size_change"   : None,
            "min_price_change"  : None,
            "asset_precision"   : None,
        }            
        self.ob                 = {}
        self.ob                 = {
            "bidPrice"          : 0.0,
            "askPrice"          : 0.0,
            "bidSize"           : 0.0,
            "askSize"           : 0.0,
        }
        self.accountData        = {
            "qty"               : 0.0,
            "entry_price"       : 0.0,
        }
        self.wsCallback         = None
        self.allSymbols         = []
        self.currFundRate      = None

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
        self.simpleClient       = BlockingTradingClient(
            MAINNET_CONFIG,
            starkPerpAcc
        )
        
    async def initPair(self):
        url                     = f"https://api.starknet.extended.exchange/api/v1/info/markets?market={self.pair["symbol"]}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                data            = await resp.json()

        if data.get("status") != "OK":
            raise Exception(f"Failed to fetch market info: {data}")

        market_info             = data["data"][0]
        trading_cfg             = market_info["tradingConfig"]

        self.pair               = {
            "symbol"            : self.pair["symbol"],
            "min_size"          : float(trading_cfg["minOrderSize"]),
            "min_size_change"   : float(trading_cfg["minOrderSizeChange"]),
            "min_price_change"  : float(trading_cfg["minPriceChange"]),
            "asset_precision"   : int  (market_info["assetPrecision"])
        }

        logger.info(f"Extended initPair Done, Pair Config\n{self.pair}")
            
    async def startWs(self, wsCallback):
        self.wsCallback                     = wsCallback

        async def subscribeOrderbook():
            while True:
                try:
                    async with self.ws_client.subscribe_to_orderbooks(self.pair["symbol"], depth=1) as stream:
                        while True:
                            msg             = await stream.recv()
                            self._handle_orderbook_update(msg.data)
                except Exception as e:
                    self.ob = { "bidPrice": 0.0, "askPrice": 0.0, "bidSize": 0.0, "askSize": 0.0 }
                    
        async def subscribeFunding():
            while True:
                try:
                    async with self.ws_client.subscribe_to_funding_rates(self.pair["symbol"]) as stream:
                        while True:
                            msg             = await stream.recv()
                            self._handle_funding_update(msg.data)
                except Exception as e:
                    self.currFundRate = None

        async def run_ws():
            await asyncio.gather(subscribeOrderbook(),subscribeFunding())

        asyncio.create_task(run_ws())

    def _handle_funding_update(self, msg):
        try:
            fr = float(msg.funding_rate)
            self.currFundRate = fr
        except Exception as e:
            logger.error(f"⚠️ Error handling Extended funding update: {e}") 

    def _handle_orderbook_update(self, msg):
        try:
            self.ob = {
                "bidPrice": float(msg.bid[0].price) if msg.bid else 0.0,
                "askPrice": float(msg.ask[0].price) if msg.ask else 0.0,
                "bidSize" : float(msg.bid[0].qty)   if msg.bid else 0.0,
                "askSize" : float(msg.ask[0].qty)   if msg.ask else 0.0,
            }

            self.wsCallback('e_ob')

        except Exception as e:
            logger.error(f"⚠️ Error handling Extended OB update: {e}")

    async def placeMarketOrder(self, side: str, qty: float, isReduceOnly, max_retries=10, delay=0.5):
        ob = self.ob
        if not ob or not ob.get("bidPrice") or not ob.get("askPrice"):
            logger.warning("No orderbook data yet, cannot place market order")
            return None

        # ✅ Determine price & format values
        if side.upper() == "BUY":
            raw_price = ob["askPrice"] * (1 + self.config["slippage"])
            side_enum = ExtendedOrderSide.BUY
        elif side.upper() == "SELL":
            raw_price = ob["bidPrice"] * (1 - self.config["slippage"])
            side_enum = ExtendedOrderSide.SELL
        else:
            logger.error("Invalid side, must be BUY or SELL")
            return None

        price           = HELPERS.extGetAllowedNum(raw_price, self.pair["min_price_change"])
        price           = Decimal(str(price))
        fixQty          = HELPERS.extendedFmtDecimal(qty, self.pair["asset_precision"])
        fixQty          = Decimal(str(fixQty))

        return_msg      = f"• PlacingMarketOrder ⭢ [{side}, {fixQty}, {price}]"  + '\n'
        logger.info     ( f"• PlacingMarketOrder ⭢ [{side}, {fixQty}, {price}]" )

        # ✅ RETRY LOOP
        for attempt in range(1, max_retries + 1):
            try:
                start_time              = time.perf_counter()
                order                   = await self.client.place_order(
                    amount_of_synthetic = fixQty,
                    price               = price,
                    market_name         = self.pair["symbol"],
                    side                = side_enum,
                    post_only           = False,
                    reduce_only         = True if isReduceOnly else False
                )
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



    # async def placeMarketOrder(self, side: str, qty: float, isReduceOnly):
    #     try:
    #         if side == "BUY":
    #             price                   = self.ob["askPrice"] * (1 + self.config["slippage"])
    #             price                   = HELPERS.extGetAllowedNum(price, self.pair["min_price_change"])
    #             price                   = Decimal(str(price))
    #             fixQty                  = HELPERS.extendedFmtDecimal(qty, self.pair["asset_precision"])
    #             fixQty                  = Decimal(str(fixQty))
    #             logger.info(f"🔃 PlacingMarketOrder Args[side, qty, worstPrice] ⭢ [{side}, {fixQty}, {price}]")
    #             start_time              = time.perf_counter()
    #             order                   = await self.client.place_order(
    #                 amount_of_synthetic = fixQty,
    #                 price               = price,
    #                 market_name         = self.pair["symbol"],
    #                 side                = ExtendedOrderSide.BUY,
    #                 post_only           = False,
    #                 reduce_only         = True if isReduceOnly else False
    #             )
    #             end_time                = time.perf_counter()
    #             latency_ms              = (end_time - start_time) * 1000
    #             logger.info(f"✅ Buy Placed | ⏱ Latency: {latency_ms:.2f} ms")

    #         elif side == "SELL":
    #             price                   = self.ob["bidPrice"] * (1 - self.config["slippage"])
    #             price                   = HELPERS.extGetAllowedNum(price, self.pair["min_price_change"])
    #             price                   = Decimal(str(price))
    #             fixQty                  = HELPERS.extendedFmtDecimal(qty, self.pair["asset_precision"])
    #             fixQty                  = Decimal(str(fixQty))
    #             logger.info(f"🔃 PlacingMarketOrder Args[side, qty, worstPrice] ⭢ [{side}, {fixQty}, {price}]")
    #             start_time              = time.perf_counter()
    #             order                   = await self.client.place_order(
    #                 amount_of_synthetic = fixQty,
    #                 price               = price,
    #                 market_name         = self.pair["symbol"],
    #                 side                = ExtendedOrderSide.SELL,
    #                 post_only           = False,
    #                 reduce_only         = True if isReduceOnly else False
    #             )
    #             end_time                = time.perf_counter()
    #             latency_ms              = (end_time - start_time) * 1000
    #             logger.info(f"✅ Buy Placed | ⏱ Latency: {latency_ms:.2f} ms")

    #         return order

    #     except Exception as e:
    #         HELPERS.record_error(f'Extended MarketOrder Error:{e}')
    #         return None


    async def placeOrder(self, side: str, price: float, qty: float, max_retries=10, delay=0.5):
        if side.upper() == "BUY":
            side_enum = ExtendedOrderSide.BUY
        elif side.upper() == "SELL":
            side_enum = ExtendedOrderSide.SELL
        else:
            logger.error("Invalid side, must be BUY or SELL")
            return None

        price           = HELPERS.extGetAllowedNum(price, self.pair["min_price_change"])
        price           = Decimal(str(price))
        fixQty          = HELPERS.extendedFmtDecimal(qty, self.pair["asset_precision"])
        fixQty          = Decimal(str(fixQty))

        return_msg      = f"• placingOrder ⭢ [{side}, {price}, {fixQty}, {price}]"  + '\n'
        logger.info     ( f"• placingOrder ⭢ [{side}, {price}, {fixQty}, {price}]" )

        # ✅ RETRY LOOP
        for attempt in range(1, max_retries + 1):
            try:
                start_time              = time.perf_counter()
                order                   = await self.simpleClient.create_and_place_order(
                    amount_of_synthetic = fixQty,
                    price               = price,
                    market_name         = self.pair["symbol"],
                    side                = side_enum,
                    post_only           = True,
                )
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
        try:
            symbol                  = self.pair["symbol"]
            start_time              = time.perf_counter()
            logger.info             (f"• CancelOrderByMarketId ⭢ {symbol}")
            resp                    = await self.simpleClient.mass_cancel(markets=[symbol])
            end_time                = time.perf_counter()
            latency_ms              = (end_time - start_time) * 1000
            logger.info             (f"• CancelOrderByMarketId Done, ⏱ Latency: {latency_ms:.2f} ms")
            return resp
        except Exception as e:
            HELPERS.record_error(f'Extended CancelOrder Error:{e}')
            return None
        



    async def loadPos(self):
        try:
            symbol                  = self.pair["symbol"]
            resp                    = await self.client.account.get_positions(market_names=[symbol])
            positions               = resp.data  

            if not positions:
                self.accountData    = {
                    "qty"           : 0.0,
                    "entry_price"   : 0.0,
                }
                return

            pos                     = positions[0]
            size                    = float(pos.size or 0.0)
            avg_price               = float(pos.open_price or 0.0)
            side                    = pos.side.upper() if pos.side else ""
            qty                     = size if side == "LONG" else -size

            self.accountData        = {
                "qty"               : qty,
                "entry_price"       : avg_price,
            }
        except Exception as e:
            HELPERS.record_error(f'Extended LoadPos Error:{e}')
            return None

