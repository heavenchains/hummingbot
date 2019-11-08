#!/usr/bin/env python

import aiohttp
import asyncio
import logging
import pandas as pd
from typing import Dict, List, Optional
import time
from hummingbot.logger import HummingbotLogger
from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.market.tex.tex_order_book import TEXOrderBook
from hummingbot.core.data_type.order_book_message import TEXOrderBookMessage
from hummingbot.core.data_type.order_book_tracker_entry import OrderBookTrackerEntry
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource

REST_URL = "http://127.0.0.1:3001"
MARKETS_URL = f"{REST_URL}/market/fetchMarkets/RINKEBY"
TICKERS_URL = "http://localhost:5001/tickers/tickers"


class TEXAPIOrderBookDataSource(OrderBookTrackerDataSource):
    def __init__(self, symbols: Optional[List[str]] = None):
        super().__init__()
        self._symbols: Optional[List[str]] = symbols
        self._get_tracking_pair_done_event: asyncio.Event = asyncio.Event()

    _baobds_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._baobds_logger is None:
            cls._baobds_logger = logging.getLogger(__name__)
        return cls._baobds_logger

    @classmethod
    async def get_active_exchange_markets(cls) -> pd.DataFrame:
        """
        Returned data frame should have symbol as index and include usd volume, baseAsset and quoteAsset
        """
        async with aiohttp.ClientSession() as client:
            market_response, ticker_response = await safe_gather(client.get(MARKETS_URL), client.get(TICKERS_URL))
            market_response: aiohttp.ClientResponse = market_response
            ticker_response: aiohttp.ClientResponse = ticker_response

            if market_response.status != 200:
                raise IOError(f"Error fetching active TEX markets. HTTP status is {market_response.status}.")
            if ticker_response.status != 200:
                raise IOError(f"Error fetching active TEX Ticker. HTTP status is {ticker_response.status}.")

            ticker_data = await ticker_response.json()
            market_data = await market_response.json()

            market_data: Dict[str, any] = market_data["markets"]
            print(ticker_data, market_data)
            ticker_data: List[Dict[str, any]] = [
                {**ticker_item, **market_data[ticker_item["marketId"]]}
                for ticker_item in ticker_data["tickers"]
                if ticker_item["marketId"] in market_data
            ]
            all_markets: pd.DataFrame = pd.DataFrame.from_records(data=ticker_data, index="marketId")
            usd_volume: List[float] = []
            for row in all_markets.itertuples():
                quote_volume: float = float(row.volume)
                usd_volume.append(quote_volume)
            all_markets["USDVolume"] = usd_volume
            return all_markets.sort_values("USDVolume", ascending=False)

    @property
    def order_book_class(self) -> TEXOrderBook:
        return TEXOrderBook

    async def get_trading_pairs(self) -> List[str]:
        if not self._symbols:
            try:
                active_markets: pd.DataFrame = await self.get_active_exchange_markets()
                trading_pairs: List[str] = active_markets.index.tolist()
                self._symbols = trading_pairs
                print(self._symbols)
            except Exception:
                self._symbols = []
                self.logger().network(
                    f"Error getting active exchange information.",
                    exc_info=True,
                    app_warning_msg=f"Error getting active exchange information. Check network connection.",
                )
        return self._symbols

    async def get_tracking_pairs(self) -> Dict[str, OrderBookTrackerEntry]:
        # Get the currently active markets
        async with aiohttp.ClientSession() as client:
            trading_pairs: List[str] = await self.get_trading_pairs()
            retval: Dict[str, OrderBookTrackerEntry] = {}
            number_of_pairs: int = len(trading_pairs)
            for index, trading_pair in enumerate(trading_pairs):
                try:
                    snapshot: Dict[str, any] = await self.get_snapshot(client, trading_pair)

                    snapshot_timestamp: float = time.time()
                    snapshot_msg: TEXOrderBookMessage = TEXOrderBook.snapshot_message_from_exchange(
                        snapshot, snapshot_timestamp, {"marketId": trading_pair}
                    )
                    tex_order_book: OrderBook = self.order_book_create_function()
                    print(snapshot_msg)
                    tex_order_book.apply_snapshot(snapshot_msg.bids, snapshot_msg.asks, snapshot_msg.update_id)

                    retval[trading_pair] = OrderBookTrackerEntry(trading_pair, snapshot_timestamp, tex_order_book)

                    self.logger().info(
                        f"Initialized order book for {trading_pair}. " f"{index+1}/{number_of_pairs} completed."
                    )
                    await asyncio.sleep(1.0)
                except Exception:
                    self.logger().error(f"Error initializing order book for {trading_pair}.", exc_info=True)
                    await asyncio.sleep(5.0)
            return retval

    async def get_snapshot(self, client: aiohttp.ClientSession, trading_pair: str) -> Dict[str, any]:
        retry: int = 3
        print(trading_pair)
        print(f"{REST_URL}/market/fetchOrderBook/RINKEBY/{trading_pair}")
        while retry > 0:
            try:
                async with client.get(f"{REST_URL}/market/fetchOrderBook/RINKEBY/{trading_pair}") as response:
                    response: aiohttp.ClientResponse = response
                    if response.status != 200:
                        raise IOError(
                            f"Error fetching Tex market snapshot for {trading_pair}. "
                            f"HTTP status is {response.status}."
                        )
                    data: Dict[str, any] = await response.json()
                    return data["orders"]
            except Exception:
                self.logger().warning(f"Error requesting order book snapshot. Retrying {retry} more times.")
                await asyncio.sleep(10)
                retry -= 1
                if retry == 0:
                    raise

    async def listen_for_trades(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        # Not implemented yet
        pass

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        # Tex's API do not provide order book diffs yet. (i.e only snapshots)
        pass

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                trading_pairs: List[str] = await self.get_trading_pairs()
                async with aiohttp.ClientSession() as client:
                    for trading_pair in trading_pairs:
                        try:
                            snapshot: Dict[str, any] = await self.get_snapshot(client, trading_pair)
                            snapshot_timestamp: float = time.time()
                            snapshot_msg: TEXOrderBookMessage = TEXOrderBook.snapshot_message_from_exchange(
                                snapshot, snapshot_timestamp, {"marketId": trading_pair}
                            )
                            output.put_nowait(snapshot_msg)
                            self.logger().debug(f"Saved order book snapshot for {trading_pair} at {snapshot_timestamp}")
                            await asyncio.sleep(5.0)
                        except asyncio.CancelledError:
                            raise
                        except IOError:
                            self.logger().network(
                                f"Error getting snapshot for {trading_pair}.",
                                exc_info=True,
                                app_warning_msg=f"Error getting snapshot for {trading_pair}. Check network connection.",
                            )
                            await asyncio.sleep(5.0)
                        except Exception:
                            self.logger().error(f"Error processing snapshot for {trading_pair}.", exc_info=True)
                            await asyncio.sleep(5.0)
                    await asyncio.sleep(5.0)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    f"Unexpected error listening for order book snapshot.",
                    exc_info=True,
                    app_warning_msg=f"Unexpected error listening for order book snapshot. Check network connection.",
                )
                await asyncio.sleep(5.0)
