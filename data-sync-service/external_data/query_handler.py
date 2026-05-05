"""
外部数据查询请求器
从 backend/external_data/ext_data_query_handle.py 迁移
根据 QUERY_API_TYPE 配置决定竞价数据使用掘金(goldminer)还是Tushare接口
其他数据（日线、分钟、Tick）统一使用掘金接口
"""
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional
import time
import threading

# 外部数据接口类型 （tushare, goldminer）
QUERY_API_TYPE = "tushare"

# Tushare API Token
TUSHARE_API_TOKEN = "Zku47OUVCydb1095ShpVSzn4u7pea7bFvgLNoCjIENA"
TUSHARE_PROXY_URL = "http://47.109.59.144:8989/dataapi"

# 接口限流配置
RATE_LIMIT_CONFIG = {
    'get_auction_data': 120,
    'get_daily_data': 1000,
    'get_minute_data': 1000,
    'get_tick_data': 1000,
    'get_instruments': 120,
    'get_money_flow_data': 120,
    'get_daily_basic_data': 120,
}


class RateLimiter:
    """限流器：基于滑动窗口算法"""

    def __init__(self, max_requests: int, window_seconds: int = 60):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.requests = []
        self.lock = threading.Lock()

    def acquire(self) -> bool:
        with self.lock:
            now = time.time()
            self.requests = [t for t in self.requests if now - t < self.window_seconds]
            if len(self.requests) < self.max_requests:
                self.requests.append(now)
                return True
            return False

    def wait_and_acquire(self) -> None:
        wait_count = 0
        while True:
            if self.acquire():
                if wait_count > 0:
                    print(f"[RateLimiter] 等待完成（等待了{wait_count}次）")
                return
            with self.lock:
                if self.requests:
                    oldest_request = min(self.requests)
                    wait_time = self.window_seconds - (time.time() - oldest_request)
                    if wait_time > 0:
                        wait_time += 0.1
                        if wait_count == 0:
                            print(f"[RateLimiter] 触发限流，等待{wait_time:.2f}秒")
                        time.sleep(wait_time)
                        wait_count += 1
                    else:
                        time.sleep(0.01)
                        wait_count += 1
                else:
                    time.sleep(0.01)
                    wait_count += 1


class ExternalDataQueryHandler:
    """外部数据查询请求器"""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self._api_token_set = False
        self._tushare_pro = None

        # 初始化限流器
        self._rate_limiters = {}
        for api_name, max_requests in RATE_LIMIT_CONFIG.items():
            self._rate_limiters[api_name] = RateLimiter(max_requests=max_requests)

        # 初始化掘金API Token
        from gm.api import set_token
        GOLD_MINER_API_TOKEN = "2e664976b46df6a0903672349c30226ac68e7bf3"
        set_token(GOLD_MINER_API_TOKEN)
        self._api_token_set = True

        # 初始化Tushare
        if QUERY_API_TYPE == "tushare":
            import tushare as ts
            self._tushare_pro = ts.pro_api(TUSHARE_API_TOKEN)
            self._tushare_pro._DataApi__http_url = TUSHARE_PROXY_URL

        self._initialized = True

    def get_daily_data(self, symbol: str, start_date: str, end_date: str, fields: Optional[str] = None) -> Optional[pd.DataFrame]:
        """获取日线数据（掘金接口）"""
        print(f"[ExternalData] get_daily_data - symbol={symbol}, start={start_date}, end={end_date}")
        self._rate_limiters['get_daily_data'].wait_and_acquire()
        from gm.api import history, ADJUST_PREV
        return history(
            symbol=symbol, frequency='1d',
            start_time=f"{start_date} 00:00:00", end_time=f"{end_date} 23:59:59",
            fields=fields or 'symbol,eob,open,close,high,low,volume,amount,pre_close',
            adjust=ADJUST_PREV, df=True
        )

    def get_minute_data(self, symbol: str, trade_date: str, start_time: str, end_time: str) -> Optional[pd.DataFrame]:
        """获取分钟数据（掘金接口）"""
        print(f"[ExternalData] get_minute_data - symbol={symbol}, date={trade_date}, time={start_time}-{end_time}")
        self._rate_limiters['get_minute_data'].wait_and_acquire()
        from gm.api import history
        return history(
            symbol=symbol, frequency='60s',
            start_time=f"{trade_date} {start_time}", end_time=f"{trade_date} {end_time}",
            fields='symbol,eob,open,close,high,low,volume,amount', df=True
        )

    def get_minute_data_batch(self, symbols: list, trade_date: str, start_time: str, end_time: str, batch_size: int = 50) -> Optional[pd.DataFrame]:
        """批量获取多只股票的分钟数据（掘金接口）

        Args:
            symbols: 股票代码列表，格式如 ['SHSE.600000', 'SZSE.000001']
            trade_date: 交易日期，格式 'YYYY-MM-DD'
            start_time: 开始时间，格式 'HH:MM:SS'
            end_time: 结束时间，格式 'HH:MM:SS'
            batch_size: 每批查询的股票数量，默认50（掘金接口限制）

        Returns:
            合并后的DataFrame，包含所有股票的分钟数据
        """
        print(f"[ExternalData] get_minute_data_batch - {len(symbols)}只股票, date={trade_date}, time={start_time}-{end_time}")
        self._rate_limiters['get_minute_data'].wait_and_acquire()

        from gm.api import history

        all_data = []
        # 分段处理，每批最多50只股票
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
            print(f"[ExternalData] get_minute_data_batch 完成, 共{len(result)}条数据")
            return result
        return None

    def get_tick_data(self, symbol: str, trade_date: str, start_time: str, end_time: str) -> Optional[pd.DataFrame]:
        """获取Tick数据（掘金接口）

        Note:
            早盘9:15~9:30只能返回09:25和09:30的快照数据
            掘金接口限制：仅能获取最近5个交易日的Tick数据
        """
        print(f"[ExternalData] get_tick_data - symbol={symbol}, date={trade_date}")
        self._rate_limiters['get_tick_data'].wait_and_acquire()
        from gm.api import history
        return history(
            symbol=symbol, frequency='tick',
            start_time=f"{trade_date} {start_time}", end_time=f"{trade_date} {end_time}",
            fields='symbol,price,volume,cum_amount,cum_volume,created_at', df=True
        )

    def get_tick_data_batch(self, symbols: list, trade_date: str, start_time: str, end_time: str, batch_size: int = 50) -> Optional[pd.DataFrame]:
        """批量获取多只股票的Tick数据（掘金接口）

        Args:
            symbols: 股票代码列表，格式如 ['SHSE.600000', 'SZSE.000001']
            trade_date: 交易日期，格式 'YYYY-MM-DD'
            start_time: 开始时间，格式 'HH:MM:SS'
            end_time: 结束时间，格式 'HH:MM:SS'
            batch_size: 每批查询的股票数量，默认50（掘金接口限制）

        Returns:
            合并后的DataFrame，包含所有股票的Tick数据

        Note:
            早盘9:15~9:30只能返回09:25和09:30的快照数据
            掘金接口限制：仅能获取最近5个交易日的Tick数据
        """
        print(f"[ExternalData] get_tick_data_batch - {len(symbols)}只股票, date={trade_date}, time={start_time}-{end_time}")
        self._rate_limiters['get_tick_data'].wait_and_acquire()

        from gm.api import history

        all_data = []
        # 分段处理，每批最多50只股票
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
            print(f"[ExternalData] get_tick_data_batch 完成, 共{len(result)}条数据")
            return result
        return None

    def get_auction_data(self, symbol: str, trade_date: str) -> Optional[pd.DataFrame]:
        """获取竞价数据（根据配置切换数据源）"""
        print(f"[ExternalData] get_auction_data - symbol={symbol}, date={trade_date}")
        self._rate_limiters['get_auction_data'].wait_and_acquire()
        try:
            start_time = time.time()
            if QUERY_API_TYPE == "tushare" and self._tushare_pro:
                ts_code = self._symbol_to_tushare(symbol)
                date_str = trade_date.replace('-', '')
                df = self._tushare_pro.stk_auction(ts_code=ts_code, trade_date=date_str)
                if df is not None and not df.empty:
                    df['symbol'] = df['ts_code'].apply(self._tushare_to_symbol)
                    df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y-%m-%d')
                    print(f"[ExternalData] Tushare竞价数据耗时: {time.time()-start_time:.3f}s")
                    return df
            from gm.api import history
            data = history(
                symbol=symbol, frequency='1d',
                start_time=trade_date + ' 09:15:00', end_time=trade_date + ' 09:25:00',
                fields='open,close,high,low,volume,amount,pre_close,eob', df=True
            )
            print(f"[ExternalData] get_auction_data 耗时: {time.time()-start_time:.3f}s")
            return data
        except Exception as e:
            print(f"[ExternalData] get_auction_data 失败: {e}")
            import traceback; traceback.print_exc()
            return None

    def get_instruments(self, list_status: str = None) -> Optional[pd.DataFrame]:
        """获取股票基本信息列表

        Args:
            list_status: 上市状态 L=上市 D=退市 P=暂停上市, None表示所有状态

        Note:
            Tushare stock_basic 接口特性：
            - 查询 list_status='L' 或 '' 时，返回的 list_status 字段为空字符串，而非 'L'
            - 查询 list_status='D' 时，返回的 list_status 字段为 'D'
            - 查询 list_status='' (空字符串) 不包含退市股票
            因此需要分别查询 L/D/P 三种状态，并显式设置 list_status 值
        """
        print(f"[ExternalData] get_instruments, list_status={list_status}")
        self._rate_limiters['get_instruments'].wait_and_acquire()
        try:
            start_time = time.time()
            if QUERY_API_TYPE == "tushare" and self._tushare_pro:
                # None 表示查询所有状态，需分别调用 L/D/P 三个接口
                # 因为 Tushare 传空字符串 '' 不包含退市股票
                if list_status is None:
                    status_list = ['L', 'D', 'P']
                else:
                    status_list = [list_status]

                all_result_list = []
                for status in status_list:
                    df = self._tushare_pro.stock_basic(exchange='', list_status=status,
                        fields='ts_code,symbol,name,area,industry,market,list_date,delist_date,list_status')
                    if df is not None and not df.empty:
                        for _, row in df.iterrows():
                            ts_code = row.get('ts_code', '')
                            if ts_code.endswith('.SH'):
                                symbol = f"SHSE.{ts_code.replace('.SH', '')}"
                            elif ts_code.endswith('.SZ'):
                                symbol = f"SZSE.{ts_code.replace('.SZ', '')}"
                            else:
                                continue
                            all_result_list.append({
                                'symbol': symbol, 'sec_name': row.get('name', '未知'),
                                'exchange': 'SHSE' if '.SH' in ts_code else 'SZSE',
                                'sec_id': row.get('symbol', ''),
                                'industry': row.get('industry', ''),
                                'area': row.get('area', ''),
                                'market': row.get('market', ''),
                                'list_date': row.get('list_date', ''),
                                'delist_date': row.get('delist_date', ''),
                                # 显式设置 list_status，因为 Tushare 对上市股票返回空字符串
                                'list_status': status,
                            })

                if all_result_list:
                    result_df = pd.DataFrame(all_result_list)
                    print(f"[ExternalData] get_instruments 耗时: {time.time()-start_time:.3f}s, {len(result_df)}条")
                    return result_df
            return None
        except Exception as e:
            print(f"[ExternalData] get_instruments 失败: {e}")
            import traceback; traceback.print_exc()
            return None

    def get_money_flow_data(self, symbol: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """获取资金流向数据（Tushare moneyflow_dc接口）"""
        print(f"[ExternalData] get_money_flow_data - symbol={symbol}, {start_date}~{end_date}")
        self._rate_limiters['get_money_flow_data'].wait_and_acquire()
        try:
            start_time = time.time()
            if self._tushare_pro:
                ts_code = self._symbol_to_tushare(symbol)
                df = self._tushare_pro.moneyflow_dc(
                    ts_code=ts_code,
                    start_date=start_date.replace('-', ''),
                    end_date=end_date.replace('-', '')
                )
                if df is not None and not df.empty:
                    df['symbol'] = df['ts_code'].apply(self._tushare_to_symbol)
                    df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y-%m-%d')
                    print(f"[ExternalData] Tushare资金流向耗时: {time.time()-start_time:.3f}s")
                    return df
                print(f"[ExternalData] Tushare未返回资金流向数据: {ts_code}")
                return None
            print(f"[ExternalData] Tushare API未初始化")
            return None
        except Exception as e:
            print(f"[ExternalData] get_money_flow_data 失败: {e}")
            import traceback; traceback.print_exc()
            return None

    def get_daily_basic_data(self, symbol: Optional[str] = None, trade_date: Optional[str] = None) -> Optional[dict]:
        """获取每日基本面指标数据（Tushare daily_basic接口）"""
        is_batch = symbol is None
        print(f"[ExternalData] get_daily_basic_data - symbol={symbol}, date={trade_date}, batch={is_batch}")
        self._rate_limiters['get_daily_basic_data'].wait_and_acquire()
        try:
            start_time = time.time()
            if self._tushare_pro:
                fields = 'ts_code,trade_date,close,turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,total_share,float_share,free_share,total_mv,circ_mv'
                kwargs = {'fields': fields}
                if is_batch:
                    if trade_date:
                        kwargs['trade_date'] = trade_date.replace('-', '')
                else:
                    ts_code = self._symbol_to_tushare(symbol)
                    kwargs['ts_code'] = ts_code
                    if trade_date:
                        kwargs['trade_date'] = trade_date.replace('-', '')
                df = self._tushare_pro.daily_basic(**kwargs)
                if df is not None and not df.empty:
                    if is_batch:
                        result = {}
                        for _, row in df.iterrows():
                            data = row.to_dict()
                            data['trade_date'] = str(data['trade_date'])
                            result[data['ts_code']] = data
                        print(f"[ExternalData] 批量查询全市场: {len(result)}只, 耗时: {time.time()-start_time:.3f}s")
                        return result
                    else:
                        data = df.iloc[0].to_dict()
                        data['trade_date'] = str(data['trade_date'])
                        print(f"[ExternalData] 单只查询耗时: {time.time()-start_time:.3f}s")
                        return data
                print(f"[ExternalData] Tushare未返回每日基本面数据")
                return None
            print(f"[ExternalData] Tushare API未初始化")
            return None
        except Exception as e:
            print(f"[ExternalData] get_daily_basic_data 失败: {e}")
            import traceback; traceback.print_exc()
            return None

    def _symbol_to_tushare(self, symbol: str) -> str:
        if '.' not in symbol:
            return symbol
        exchange, code = symbol.split('.')
        if exchange == 'SHSE':
            return f"{code}.SH"
        elif exchange == 'SZSE':
            return f"{code}.SZ"
        return symbol

    def _tushare_to_symbol(self, ts_code: str) -> str:
        if '.' not in ts_code:
            return ts_code
        code, exchange = ts_code.split('.')
        if exchange == 'SH':
            return f"SHSE.{code}"
        elif exchange == 'SZ':
            return f"SZSE.{code}"
        return ts_code


_query_handler = None


def get_query_handler() -> ExternalDataQueryHandler:
    global _query_handler
    if _query_handler is None:
        _query_handler = ExternalDataQueryHandler()
    return _query_handler
