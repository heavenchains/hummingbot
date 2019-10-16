#!/usr/bin/env python

import aiohttp
import asyncio
import logging
import pandas as pd
from typing import (
    Any,
    AsyncIterable,
    Dict,
    List,
    Optional
)
import time
import ujson
import websockets
from websockets.exceptions import ConnectionClosed

# from hummingbot.core.data_type.order_book import OrderBook
# from hummingbot.logger import HummingbotLogger
# from hummingbot.core.utils import async_ttl_cache
# from hummingbot.market.idex.idex_active_order_tracker import IDEXActiveOrderTracker
# from hummingbot.market.idex.idex_order_book import IDEXOrderBook
# from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
# from hummingbot.core.data_type.order_book_tracker_entry import (
#     IDEXOrderBookTrackerEntry,
#     OrderBookTrackerEntry
# )
# from hummingbot.core.data_type.order_book_message import IDEXOrderBookMessage


TEX_REST_URL = "https://rinkeby.liquidity.network"

class TEXAPIOrderBookDataSource():

    # _iaobds_logger: Optional[logging.Logger] = None
    
    # @classmethod
    # def logger(cls) -> HummingbotLogger:
    #     if cls._iaobds_logger is None:
    #         cls._iaobds_logger = logging.getLogger(__name__)
    #     return cls._iaobds_logger

    def __init__(self, symbols: Optional[List[str]] = None):
        super().__init__()
        self._symbols: Optional[List[str]] = symbols
        self._get_tracking_pair_done_event: asyncio.Event = asyncio.Event()
  
    @classmethod
    async def get_active_exchange_markets(cls) -> pd.DataFrame:
        """
        Return all hub's available pairs
        """

        pairs= ["fLQD-fETH","fLQD-fCO2","fCO2-fETH","fFCO-fETH","fLQD-fFCO","fFCO-fCO2"]
        Tokens= [{
                "tokenAddress": "0x66b26B6CeA8557D6d209B33A30D69C11B0993a3a",
                "name": "Ethereum",
                "shortName": "ETH"
                }, {
                "tokenAddress": "0xA9F86DD014C001Acd72d5b25831f94FaCfb48717",
                "name": "LQD",
                "shortName": "LQD"
                }, {
                "tokenAddress": "0x773104aA7fF27Abc94e251392a45661fcb4CB302",
                "name": "CarbonCredits",
                "shortName": "CO2"
                }, {
                "tokenAddress": "0xa5022f14E82C18b78B137460333F48a9841Be44e",
                "name": "FedirCoin",
                "shortName": "FCO"
                }]
               
        data = []
        for trading_pair in pairs:
             if "-" in trading_pair:
                  quote_asset = trading_pair.split("-")[1][1:4]
                  base_asset = trading_pair.split("-")[0][1:4]
                  quote_address= ''
                  base_address= ''
                  for token in Tokens:
                        if quote_asset == token["shortName"]:
                            quote_address = token["tokenAddress"]
                        if  base_asset == token["shortName"]:
                            base_address = token["tokenAddress"]
                  data.append({
                            "market": trading_pair,
                            "baseAsset": base_asset,
                            "quoteAsset": quote_asset,
                            "baseAddress": base_address,
                            "quoteAddress": quote_address
                            })  
        return data    


    async def get_trading_pairs(self)-> List[str]:
        if self._symbols is None:
            pairList = []
            active_markets = await self.get_active_exchange_markets()   
            for pairs in active_markets:
                pairList.append(pairs["market"])
            trading_pairs: List[str] = pairList
            self._symbols = trading_pairs
        else:
            trading_pairs: List[str] = self._symbols
        return trading_pairs  


    async def get_snapshot(self, client: aiohttp.ClientSession, trading_pair: str) -> Dict[str, Any]:
        active_markets = await self.get_active_exchange_markets()  
        base_address = ''
        quote_address = ''
        for pair in active_markets:
            if trading_pair == pair["market"]:
                base_address = pair["baseAddress"]
                quote_address = pair["quoteAddress"]

        
       
        async with client.get(f"{TEX_REST_URL}/audit/swaps/{base_address}/{quote_address}") as response:
            print(base_address, quote_address)
            response: aiohttp.ClientResponse = response
            if response.status != 200:
                raise IOError(f"Error fetching TEX market snapshot for {trading_pair}. "
                                f"HTTP status is {response.status}.")
            print(response)
            orders = await response.json()
            return {"orders": orders}

        

async def main():
    async with aiohttp.ClientSession() as client:
        x = await TEXAPIOrderBookDataSource().get_snapshot(client, 'fLQD-fETH')
        print(x)               

asyncio.run(main())