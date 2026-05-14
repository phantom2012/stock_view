import logging
import pandas as pd
from typing import Optional
import time
import akshare as ak

from .rate_limiter import RateLimiter

logger = logging.getLogger(__name__)

GOLD_MINER_API_TOKEN = "2e664976b46df6a0903672349c30226ac68e7bf3"

RATE_LIMIT_CONFIG = {
    'get_daily_data': 2000,
    'get_minute_data': 1000,
    'get_tick_data': 1000,
    'get_instruments': 120,
    'get_money_flow_data': 120,
}


class GoldminerQuery:
    def __init__(self):
        self._api_token_set = False
        self._rate_limiters = {}
        for api_name, max_requests in RATE_LIMIT_CONFIG.items():
            self._rate_limiters[api_name] = RateLimiter(max_requests=max_requests)
        self._init_token()

    def _init_token(self):
        from gm.api import set_token
        set_token(GOLD_MINER_API_TOKEN)
        self._api_token_set = True

    def get_daily_data(self, symbol: str, start_date: str, end_date: str, fields: Optional[str] = None) -> Optional[pd.DataFrame]:
        # logger.info(f"get_daily_data - symbol={symbol}, start={start_date}, end={end_date}")
        self._rate_limiters['get_daily_data'].wait_and_acquire('get_daily_data')
        from gm.api import history, ADJUST_PREV
        return history(
            symbol=symbol, frequency='1d',
            start_time=f"{start_date} 00:00:00", end_time=f"{end_date} 23:59:59",
            fields=fields or 'symbol,eob,open,close,high,low,volume,amount,pre_close',
            adjust=ADJUST_PREV, df=True
        )

    def get_minute_data(self, symbol: str, trade_date: str, start_time: str, end_time: str) -> Optional[pd.DataFrame]:
        logger.info(f"get_minute_data - symbol={symbol}, date={trade_date}, time={start_time}-{end_time}")
        self._rate_limiters['get_minute_data'].wait_and_acquire('get_minute_data')
        from gm.api import history
        return history(
            symbol=symbol, frequency='60s',
            start_time=f"{trade_date} {start_time}", end_time=f"{trade_date} {end_time}",
            fields='symbol,eob,open,close,high,low,volume,amount', df=True
        )

    def get_minute_data_batch(self, symbols: list, trade_date: str, start_time: str, end_time: str, batch_size: int = 50) -> Optional[pd.DataFrame]:
        print(f"[GoldminerQuery] get_minute_data_batch - {len(symbols)}只股票, date={trade_date}, time={start_time}-{end_time}")
        self._rate_limiters['get_minute_data'].wait_and_acquire('get_minute_data')

        from gm.api import history

        all_data = []
        for i in range(0, len(symbols), batch_size):
            batch_symbols = symbols[i:i + batch_size]
            print(f"  处理批次 [{i+1}-{min(i+batch_size, len(symbols))}/{len(symbols)}]")

            try:
                data = history(
                    symbol=batch_symbols, frequency='60s',
                    start_time=f"{trade_date} {start_time}", end_time=f"{trade_date} {end_time}",
                    fields='symbol,eob,open,close,high,low,volume,amount', df=True
                )
                if data is not None and not data.empty:
                    all_data.append(data)
            except Exception as e:
                print(f"  批次查询失败: {e}")

        if all_data:
            result = pd.concat(all_data, ignore_index=True)
            print(f"[GoldminerQuery] get_minute_data_batch 完成, 共{len(result)}条数据")
            return result
        return None

    def get_tick_data(self, symbol: str, trade_date: str, start_time: str, end_time: str) -> Optional[pd.DataFrame]:
        logger.info(f"get_tick_data - symbol={symbol}, date={trade_date}")
        self._rate_limiters['get_tick_data'].wait_and_acquire('get_tick_data')
        from gm.api import history
        return history(
            symbol=symbol, frequency='tick',
            start_time=f"{trade_date} {start_time}", end_time=f"{trade_date} {end_time}",
            fields='symbol,price,volume,cum_amount,cum_volume,created_at', df=True
        )

    def get_tick_data_batch(self, symbols: list, trade_date: str, start_time: str, end_time: str, batch_size: int = 50) -> Optional[pd.DataFrame]:
        print(f"[GoldminerQuery] get_tick_data_batch - {len(symbols)}只股票, date={trade_date}, time={start_time}-{end_time}")
        self._rate_limiters['get_tick_data'].wait_and_acquire('get_tick_data')

        from gm.api import history

        all_data = []
        for i in range(0, len(symbols), batch_size):
            batch_symbols = symbols[i:i + batch_size]
            print(f"  处理批次 [{i+1}-{min(i+batch_size, len(symbols))}/{len(symbols)}]")

            try:
                data = history(
                    symbol=batch_symbols, frequency='tick',
                    start_time=f"{trade_date} {start_time}", end_time=f"{trade_date} {end_time}",
                    fields='symbol,price,volume,cum_amount,cum_volume,created_at', df=True
                )
                if data is not None and not data.empty:
                    all_data.append(data)
            except Exception as e:
                print(f"  批次查询失败: {e}")

        if all_data:
            result = pd.concat(all_data, ignore_index=True)
            print(f"[GoldminerQuery] get_tick_data_batch 完成, 共{len(result)}条数据")
            return result
        return None

    def get_auction_data(self, symbol: str, trade_date: str) -> Optional[pd.DataFrame]:
        logger.info(f"get_auction_data - symbol={symbol}, date={trade_date}")
        self._rate_limiters['get_daily_data'].wait_and_acquire('get_auction_data')
        try:
            start_time = time.time()
            from gm.api import history
            data = history(
                symbol=symbol, frequency='1d',
                start_time=trade_date + ' 09:15:00', end_time=trade_date + ' 09:25:00',
                fields='open,close,high,low,volume,amount,pre_close,eob', df=True
            )
            logger.info(f"get_auction_data 耗时: {time.time()-start_time:.3f}s")
            return data
        except Exception as e:
            print(f"[GoldminerQuery] get_auction_data 失败: {e}")
            import traceback; traceback.print_exc()
            return None

    def get_money_flow_data(self, symbol: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        logger.info(f"get_money_flow_data - symbol={symbol}, {start_date}~{end_date}")
        self._rate_limiters['get_money_flow_data'].wait_and_acquire('get_money_flow_data')
        try:
            start_time = time.time()
            code = self._symbol_to_akshare_code(symbol)
            df = ak.stock_individual_fund_flow(stock=code, market=self._get_market(code))
            if df is not None and not df.empty:
                df = df.rename(columns={
                    '日期': 'trade_date',
                    '收盘价': 'close',
                    '涨跌幅': 'pct_change',
                    '主力净流入-净额': 'net_amount',
                    '主力净流入-净占比': 'net_amount_rate',
                    '超大单净流入-净额': 'buy_elg_amount',
                    '超大单净流入-净占比': 'buy_elg_amount_rate',
                    '大单净流入-净额': 'buy_lg_amount',
                    '大单净流入-净占比': 'buy_lg_amount_rate',
                    '中单净流入-净额': 'buy_md_amount',
                    '中单净流入-净占比': 'buy_md_amount_rate',
                    '小单净流入-净额': 'buy_sm_amount',
                    '小单净流入-净占比': 'buy_sm_amount_rate',
                })
                df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y-%m-%d')
                df['symbol'] = symbol
                df['name'] = ''
                df['net_d5_amount'] = 0
                date_mask = (df['trade_date'] >= start_date) & (df['trade_date'] <= end_date)
                df = df[date_mask].copy()
                logger.info(f"get_money_flow_data 耗时: {time.time()-start_time:.3f}s, {len(df)}条")
                return df
            logger.info(f"akshare未返回资金流向数据: {code}")
            return None
        except Exception as e:
            print(f"[GoldminerQuery] get_money_flow_data 失败: {e}")
            import traceback; traceback.print_exc()
            return None

    def _symbol_to_akshare_code(self, symbol: str) -> str:
        if '.' not in symbol:
            return symbol
        _, code = symbol.split('.')
        return code

    def _get_market(self, code: str) -> str:
        if code.startswith('6'):
            return 'sh'
        return 'sz'
