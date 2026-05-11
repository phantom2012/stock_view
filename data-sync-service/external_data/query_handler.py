import pandas as pd
from typing import Optional

from .goldminer_query import GoldminerQuery
from .tushare_query import TushareQuery

QUERY_API_TYPE = "goldminer"


class ExternalDataQueryHandler:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self._goldminer = GoldminerQuery()
        self._tushare = TushareQuery()
        self._initialized = True

    def get_daily_data(self, symbol: str, start_date: str, end_date: str, fields: Optional[str] = None) -> Optional[pd.DataFrame]:
        return self._goldminer.get_daily_data(symbol, start_date, end_date, fields)

    def get_minute_data(self, symbol: str, trade_date: str, start_time: str, end_time: str) -> Optional[pd.DataFrame]:
        return self._goldminer.get_minute_data(symbol, trade_date, start_time, end_time)

    def get_minute_data_batch(self, symbols: list, trade_date: str, start_time: str, end_time: str, batch_size: int = 50) -> Optional[pd.DataFrame]:
        return self._goldminer.get_minute_data_batch(symbols, trade_date, start_time, end_time, batch_size)

    def get_tick_data(self, symbol: str, trade_date: str, start_time: str, end_time: str) -> Optional[pd.DataFrame]:
        return self._goldminer.get_tick_data(symbol, trade_date, start_time, end_time)

    def get_tick_data_batch(self, symbols: list, trade_date: str, start_time: str, end_time: str, batch_size: int = 50) -> Optional[pd.DataFrame]:
        return self._goldminer.get_tick_data_batch(symbols, trade_date, start_time, end_time, batch_size)

    def get_money_flow_data(self, symbol: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        return self._goldminer.get_money_flow_data(symbol, start_date, end_date)

    def get_auction_data(self, symbol: str, trade_date: str) -> Optional[pd.DataFrame]:
        if QUERY_API_TYPE == "tushare":
            return self._tushare.get_auction_data(symbol, trade_date)
        return self._goldminer.get_auction_data(symbol, trade_date)

    def get_instruments(self, list_status: str = None) -> Optional[pd.DataFrame]:
        return self._tushare.get_instruments(list_status)

    def get_daily_basic_data(self, symbol: Optional[str] = None, trade_date: Optional[str] = None) -> Optional[dict]:
        return self._tushare.get_daily_basic_data(symbol, trade_date)


_query_handler = None


def get_query_handler() -> ExternalDataQueryHandler:
    global _query_handler
    if _query_handler is None:
        _query_handler = ExternalDataQueryHandler()
    return _query_handler
