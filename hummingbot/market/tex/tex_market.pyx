import aiohttp
from async_timeout import timeout
import asyncio
from cachetools import TTLCache
from collections import (
    deque,
    OrderedDict
)
from decimal import Decimal
from libc.stdint cimport int64_t
import logging
import time
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Tuple,
)
from web3 import Web3

from hummingbot.logger import HummingbotLogger
from hummingbot.core.data_type.cancellation_result import CancellationResult
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.data_type.order_book cimport OrderBook
from hummingbot.core.data_type.order_book_tracker import OrderBookTrackerDataSourceType
from hummingbot.core.event.events import (
    MarketEvent,
    BuyOrderCompletedEvent,
    SellOrderCompletedEvent,
    OrderFilledEvent,
    OrderCancelledEvent,
    MarketOrderFailureEvent,
    BuyOrderCreatedEvent,
    SellOrderCreatedEvent,
    TradeType,
    OrderType,
    TradeFee
)
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.utils.async_call_scheduler import AsyncCallScheduler
from hummingbot.core.utils.async_utils import (
    safe_ensure_future,
    safe_gather,
)
from hummingbot.market.market_base cimport MarketBase
from hummingbot.market.market_base import s_decimal_NaN
from hummingbot.wallet.ethereum.web3_wallet import Web3Wallet
from hummingbot.market.tex.tex_api_order_book_data_source import TEXAPIOrderBookDataSource
from hummingbot.market.tex.tex_order_book_tracker import TEXOrderBookTracker
from hummingbot.market.tex.tex_in_flight_order cimport TEXInFlightOrder

tex_logger = None
s_decimal_0 = Decimal(0)
NETWORK= 'RINKEBY'

cdef class TEXMarketTransactionTracker(TransactionTracker):
    cdef:
        TEXMarket _owner

    def __init__(self, owner: TEXMarket):
        super().__init__()
        self._owner = owner

    cdef c_did_timeout_tx(self, str tx_id):
        TransactionTracker.c_did_timeout_tx(self, tx_id)
        self._owner.c_did_timeout_tx(tx_id)

cdef class TEXMarket(MarketBase):
    MARKET_RECEIVED_ASSET_EVENT_TAG = MarketEvent.ReceivedAsset.value
    MARKET_BUY_ORDER_COMPLETED_EVENT_TAG = MarketEvent.BuyOrderCompleted.value
    MARKET_SELL_ORDER_COMPLETED_EVENT_TAG = MarketEvent.SellOrderCompleted.value
    MARKET_WITHDRAW_ASSET_EVENT_TAG = MarketEvent.WithdrawAsset.value
    MARKET_ORDER_CANCELLED_EVENT_TAG = MarketEvent.OrderCancelled.value
    MARKET_ORDER_FILLED_EVENT_TAG = MarketEvent.OrderFilled.value
    MARKET_ORDER_FAILURE_EVENT_TAG = MarketEvent.OrderFailure.value
    MARKET_BUY_ORDER_CREATED_EVENT_TAG = MarketEvent.BuyOrderCreated.value
    MARKET_SELL_ORDER_CREATED_EVENT_TAG = MarketEvent.SellOrderCreated.value

    TEX_REST_ENDPOINT = "http://127.0.0.1:3001"
    API_CALL_TIMEOUT =200.0
    TRADE_API_CALL_TIMEOUT = 60
    UPDATE_HOURLY_INTERVAL = 60 * 60
    UPDATE_ORDER_TRACKING_INTERVAL = 10
    UPDATE_BALANCES_INTERVAL = 5
    ORDER_EXPIRY_TIME = 15 * 60.0
    CANCEL_EXPIRY_TIME = 60.0
    MINIMUM_LIMIT_ORDER_LIFESPAN = 15

    MINIMUM_MAKER_ORDER_SIZE_ETH = Decimal("0.15")
    MINIMUM_TAKER_ORDER_SIZE_ETH = Decimal("0.05")

    @classmethod
    def logger(cls) -> HummingbotLogger:
        global tex_logger
        if tex_logger is None:
            tex_logger = logging.getLogger(__name__)
        return tex_logger

    def __init__(self,
                 wallet: Web3Wallet,
                 ethereum_rpc_url: str,
                 poll_interval: float = 5.0,
                 order_book_tracker_data_source_type: OrderBookTrackerDataSourceType =
                 OrderBookTrackerDataSourceType.EXCHANGE_API,
                 symbols: Optional[List[str]] = None,
                 trading_required: bool = True):
        super().__init__()

        self._order_book_tracker = TEXOrderBookTracker(data_source_type=order_book_tracker_data_source_type, symbols=symbols)
        self._trading_required = trading_required
        self._ev_loop = asyncio.get_event_loop()
        self._poll_notifier = asyncio.Event()
        self._last_timestamp = 0
        self.symbol = 'fLQD-fETH'
        self._last_update_balances_timestamp = 0
        self._last_update_order_timestamp = 0
        self._last_update_asset_info_timestamp = 0
        self._last_update_contract_address_timestamp = 0
        self._poll_interval = poll_interval
        self._in_flight_orders = {}
        self._in_flight_cancels = OrderedDict()
        self._order_expiry_queue = deque()
        self._order_expiry_set = set()
        self._w3 = Web3(Web3.HTTPProvider(ethereum_rpc_url))
        self._tx_tracker = TEXMarketTransactionTracker(self)
        self._status_polling_task = None
        self._order_tracker_task = None
        self._wallet = wallet
        self._account_balances = {}
        self._account_available_balances = {}
        self._shared_client = None
        self._api_response_records = TTLCache(60000, ttl=600.0)
        self._assets_info = {}
        self._contract_address = None
        self._async_scheduler = AsyncCallScheduler(call_interval=1.0)
        self._last_nonce = 0

    @property
    def status_dict(self):
        return {
            "account_balance": len(self._account_balances) > 0 if self._trading_required else True,
            "order_books_initialized": self._order_book_tracker.ready,
        }

    @property
    def ready(self) -> bool:
        return all(self.status_dict.values())

    @property
    def name(self) -> str:
        return "tex"

    @property
    def order_books(self) -> Dict[str, OrderBook]:
        return self._order_book_tracker.order_books

    @property
    def wallet(self) -> Web3Wallet:
        return self._wallet

    @property
    def in_flight_orders(self) -> Dict[str, TEXInFlightOrder]:
        return self._in_flight_orders

    @property
    def limit_orders(self) -> List[LimitOrder]:
        cdef:
            list retval = []
            TEXInFlightOrder typed_in_flight_order
            set expiring_order_ids = set([order_id for _, order_id in self._order_expiry_queue])

        for in_flight_order in self._in_flight_orders.values():
            typed_in_flight_order = in_flight_order
            if ((typed_in_flight_order.order_type is not OrderType.LIMIT) or
                    typed_in_flight_order.is_done):
                continue
            if typed_in_flight_order.client_order_id in expiring_order_ids:
                continue
            retval.append(typed_in_flight_order.to_limit_order())

        return retval

    @property
    def expiring_orders(self) -> List[LimitOrder]:
        return [self._in_flight_orders[order_id].to_limit_order() for _, order_id in self._order_expiry_queue]

    @property
    def tracking_states(self) -> Dict[str, any]:
        return {
            key: value.to_json() for key, value in self._in_flight_orders.items()
        }

    def restore_tracking_states(self, saved_states: Dict[str, any]):
        self._in_flight_orders.update({
            key: TEXInFlightOrder.from_json(value) for key, value in saved_states.items()
        })

    async def get_active_exchange_markets(self) -> pd.DataFrame:
        return await TEXAPIOrderBookDataSource.get_active_exchange_markets()

    async def _status_polling_loop(self):
        print('starting status polling')
        while True:
            try:
                print('creating poll notifier')
                self._poll_notifier = asyncio.Event()
                print('waiting poll notifier')
                await self._poll_notifier.wait()
                print('gathering')
                await self._update_balances()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(f"TEX Status Polling Loop Error: {e}")
                self.logger().network(
                    "Unexpected error while fetching account and status updates.",
                    exc_info=True,
                    app_warning_msg=f"Failed to fetch account updates on TEX. Check network connection."
                )

    async def _register_wallet(self):
        print('registering wallet....')
        privateKey = self.wallet.private_key
        url = f"{self.TEX_REST_ENDPOINT}/account/import"
        register_response = await self._api_request("post", url=url, json={"privateKey": privateKey, "hubName": 'RINKEBY', "pair": self.symbol})
        print('registered wallet....', register_response)

    async def _update_balances(self):
        cdef:
            double current_timestamp = self._current_timestamp
        if current_timestamp - self._last_update_balances_timestamp > self.UPDATE_BALANCES_INTERVAL or len(self._account_balances) > 0:
            available_balances, total_balances = await self.get_tex_balances()
            self._account_available_balances = available_balances
            self._account_balances = total_balances
            self._last_update_balances_timestamp = current_timestamp

    async def get_tex_balances(self) -> Tuple[Dict[str, Decimal], Dict[str, Decimal]]:
        publicKey = self.wallet.address
        url = f"{self.TEX_REST_ENDPOINT}/account/fetchCommitChainBalance"
        response_data = await self._api_request("post", url=url, json={"publicKey": publicKey, "hubName": 'RINKEBY', "pair": self.symbol})
        available_balances = response_data["balance"]
        total_balances = response_data["balance"]
        print('available_balances', available_balances)
        print('total_balances', total_balances)
        return available_balances, total_balances

    cdef str c_buy(self, str symbol, object amount, object order_type=OrderType.LIMIT, object price=s_decimal_NaN, dict kwargs={}):
        order_id = safe_ensure_future(self.execute_buy(symbol, amount, price, order_type))
        print(order_id)
        return order_id

    async def execute_buy(self, symbol: str, amount: Decimal, price: Decimal, order_type: OrderType) -> str:
        cdef:
            object q_amt = self.c_quantize_order_amount(symbol, amount)
            object q_price = (self.c_quantize_order_price(symbol, price)
                              if order_type is OrderType.LIMIT
                              else s_decimal_0)

        publicKey = self.wallet.address
        url = f"{self.TEX_REST_ENDPOINT}/trading/createOrder"
        buy_response = await self._api_request("post", url=url, json={"publicKey": publicKey, "pair": symbol, "trade_type": "buy", "hubName": "RINKEBY", "price": q_price, "amount": q_amt})
        print(buy_response)
        return buy_response

    cdef str c_sell(self, str symbol, object amount, object order_type=OrderType.LIMIT, object price=s_decimal_NaN, dict kwargs={}):
        order_id = safe_ensure_future(self.execute_sell(symbol, amount, price, order_type))
        print(order_id)
        return order_id

    async def execute_sell(self, symbol: str, amount: Decimal, price: Decimal, order_type: OrderType) -> str:
        cdef:
            object q_amt = self.c_quantize_order_amount(symbol, amount)
            object q_price = (self.c_quantize_order_price(symbol, price)
                              if order_type is OrderType.LIMIT
                              else s_decimal_0)
        publicKey = self.wallet.address
        url = f"{self.TEX_REST_ENDPOINT}/trading/createOrder"
        sell_response = await self._api_request("post", url=url, json={"publicKey": publicKey, "pair": symbol, "trade_type": "sell", "hubName": "RINKEBY", "price": q_price, "amount": q_amt})
        print(sell_response)
        return sell_response

    async def c_cancel(self, str symbol, str client_order_id):
        publicKey = self.wallet.address
        url = f"{self.TEX_REST_ENDPOINT}/trading/cancelOrder"
        cancel_response = await self._api_request("post", url=url, json={"publicKey": publicKey, "txId": client_order_id})
        print(cancel_response)

    async def cancel_all(self, timeout_seconds: float) -> List[CancellationResult]:
        incomplete_orders = [o for o in self.in_flight_orders.values() if not o.is_done and o.client_order_id not in self._order_expiry_set]
        client_order_ids = [o.client_order_id for o in incomplete_orders]
        order_id_set = set(client_order_ids)
        tasks = [self.cancel_order(i) for i in client_order_ids]
        successful_cancellations = []

        try:
            async with timeout(timeout_seconds):
                cancellation_results = await safe_gather(*tasks, return_exceptions=True)
                for cid, cr in zip(client_order_ids, cancellation_results):
                    if isinstance(cr, Exception):
                        continue
                    if isinstance(cr, dict) and cr.get("success") == 1:
                        order_id_set.remove(cid)
                        successful_cancellations.append(CancellationResult(cid, True))
        except Exception as e:
            self.logger().network(
                f"Unexpected error cancelling orders.",
                exc_info=True,
                app_warning_msg=f"Failed to cancel orders on TEX. Check Ethereum wallet and network connection."
            )

        failed_cancellations = [CancellationResult(oid, False) for oid in order_id_set]
        return successful_cancellations + failed_cancellations

    cdef OrderBook c_get_order_book(self, str symbol):
        cdef:
            dict order_books = self._order_book_tracker.order_books

        if symbol not in order_books:
            raise ValueError(f"No order book exists for '{symbol}'.")
        return order_books[symbol]

    async def start_network(self):
        if self._order_tracker_task is not None:
            self._stop_network()
        await self._register_wallet()
        await self._update_balances()
        self._order_tracker_task = safe_ensure_future(self._order_book_tracker.start())
        self._status_polling_task = safe_ensure_future(self._status_polling_loop())

    def _stop_network(self):
        if self._order_tracker_task is not None:
            self._order_tracker_task.cancel()
            self._status_polling_task.cancel()
        self._order_tracker_task = self._status_polling_task = None

    async def stop_network(self):
        self._stop_network()
        if self._shared_client is not None:
            await self._shared_client.close()
            self._shared_client = None

    async def check_network(self) -> NetworkStatus:
        if self._wallet.network_status is not NetworkStatus.CONNECTED:
            return NetworkStatus.NOT_CONNECTED
        url = f"{self.TEX_REST_ENDPOINT}/market/connect/RINKEBY"
        try:
            await self._api_request("get", url)
        except asyncio.CancelledError:
            raise
        except Exception:
            return NetworkStatus.NOT_CONNECTED
        return NetworkStatus.CONNECTED

    cdef c_tick(self, double timestamp):
        cdef:
            int64_t last_tick = <int64_t>(self._last_timestamp / self._poll_interval)
            int64_t current_tick = <int64_t>(timestamp / self._poll_interval)

        self._tx_tracker.c_tick(timestamp)
        MarketBase.c_tick(self, timestamp)
        if current_tick > last_tick:
            if not self._poll_notifier.is_set():
                self._poll_notifier.set()
        self._last_timestamp = timestamp

    async def _http_client(self) -> aiohttp.ClientSession:
        if self._shared_client is None:
            self._shared_client = aiohttp.ClientSession()
        return self._shared_client

    async def _api_request(self,
                           http_method: str,
                           url: str,
                           data: Optional[Dict[str, Any]] = None,
                           params: Optional[Dict[str, Any]] = None,
                           headers: Optional[Dict[str, str]] = None,
                           json: Any = None) -> Dict[str, Any]:
        client = await self._http_client()
        async with client.request(http_method,
                                  url=url,
                                  timeout=self.API_CALL_TIMEOUT,
                                  data=data,
                                  params=params,
                                  json=json) as response:
            data = await response.json()
            if response.status != 200:
                raise IOError(f"Error fetching data from {url}. HTTP status is {response.status} - {data}")
            # Keep an auto-expired record of the response and the request URL for debugging and logging purpose.
            self._api_response_records[url] = response
            return data

    cdef object c_get_fee(self,
                          str base_currency,
                          str quote_currency,
                          object order_type,
                          object order_side,
                          object amount,
                          object price):

        if order_type is OrderType.LIMIT:
            return TradeFee(percent=Decimal("0.00"))

    cdef object c_get_order_price_quantum(self, str symbol, object price):
        cdef:
            quote_asset_decimals = 18
        decimals_quantum = Decimal(f"1e-{quote_asset_decimals}")
        return decimals_quantum

    cdef object c_get_order_size_quantum(self, str symbol, object amount):
        cdef:
            base_asset_decimals = 18
        decimals_quantum = Decimal(f"1e-{base_asset_decimals}")
        return decimals_quantum

    def quantize_order_amount(self, symbol: str, amount: Decimal, price: Decimal = s_decimal_NaN) -> Decimal:
        return self.c_quantize_order_amount(symbol, amount, price)

    cdef object c_quantize_order_amount(self, str symbol, object amount, object price=s_decimal_0):
        quantized_amount = MarketBase.c_quantize_order_amount(self, symbol, amount)
        actual_price = Decimal(price or self.get_price(symbol, True))
        amount_quote = quantized_amount * actual_price
        return quantized_amount
