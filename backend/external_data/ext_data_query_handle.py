"""
外部数据查询请求器
根据 QUERY_API_TYPE 配置决定竞价数据使用掘金(goldminer)还是Tushare接口
其他数据（日线、分钟、Tick）统一使用掘金接口
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Optional
import time
import threading

# 外部数据接口类型 （tushare, goldminer）
# 注意：只有竞价数据会使用此配置切换数据源
# 其他数据（日线、分钟、Tick）统一使用掘金接口
QUERY_API_TYPE = "tushare"

# Tushare API Token
TUSHARE_API_TOKEN = "aeb08b4b67a00b77b8c8041b8e183e9c07c350fbe31691ede2913291"
# Tushare 代理地址
TUSHARE_PROXY_URL = "http://tsy.xiaodefa.cn"

# 接口限流配置（每分钟最大请求次数）
RATE_LIMIT_CONFIG = {
    'get_auction_data': 120,   # 竞价数据接口：120次/分钟
    'get_daily_data': 1000,     # 日线数据接口：500次/分钟
    'get_minute_data': 1000,    # 分钟数据接口：500次/分钟
    'get_tick_data': 1000,      # Tick数据接口：500次/分钟
    'get_instruments': 120,    # 股票基本信息接口：500次/分钟
    'get_money_flow_data': 120, # 资金流向数据接口：120次/分钟
}


class RateLimiter:
    """
    限流器：基于滑动窗口算法，限制每分钟内的请求次数
    采用阻塞等待机制，超过限流时自动等待直到有可用配额
    """

    def __init__(self, max_requests: int, window_seconds: int = 60):
        """
        初始化限流器

        Args:
            max_requests: 窗口期内最大请求次数
            window_seconds: 窗口期大小（秒），默认60秒（1分钟）
        """
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.requests = []  # 记录请求时间戳
        self.lock = threading.Lock()  # 线程锁

    def acquire(self) -> bool:
        """
        尝试获取请求许可（非阻塞）

        Returns:
            bool: True表示允许请求，False表示需要等待
        """
        with self.lock:
            now = time.time()
            # 移除窗口期外的请求记录
            self.requests = [t for t in self.requests if now - t < self.window_seconds]

            if len(self.requests) < self.max_requests:
                self.requests.append(now)
                return True
            else:
                return False

    def wait_and_acquire(self) -> None:
        """
        阻塞等待并获取请求许可
        如果当前窗口期内请求次数已达上限，则阻塞等待直到窗口期重置
        此方法会一直阻塞直到成功获取请求许可
        """
        wait_count = 0
        while True:
            if self.acquire():
                if wait_count > 0:
                    print(f"[RateLimiter] 等待完成，继续执行请求（等待了{wait_count}次）")
                return

            # 计算需要等待的时间：找到最早的请求时间，等待到它超出窗口期
            with self.lock:
                if self.requests:
                    oldest_request = min(self.requests)
                    wait_time = self.window_seconds - (time.time() - oldest_request)
                    if wait_time > 0:
                        # 额外等待0.1秒确保完全超出窗口期
                        wait_time += 0.1
                        if wait_count == 0:
                            print(f"[RateLimiter] 触发限流，需要等待{wait_time:.2f}秒（当前窗口期已有{len(self.requests)}次请求，上限{self.max_requests}次）")
                        else:
                            print(f"[RateLimiter] 继续等待{wait_time:.2f}秒...")
                        time.sleep(wait_time)
                        wait_count += 1
                    else:
                        # 窗口期已过，重试
                        time.sleep(0.01)
                        wait_count += 1
                else:
                    # 理论上不会到这里，因为acquire()返回False说明requests不为空
                    time.sleep(0.01)
                    wait_count += 1


class ExternalDataQueryHandler:
    """
    外部数据查询请求器
    竞价数据根据配置切换数据源，其他数据统一使用掘金接口
    """

    _instance = None

    def __new__(cls):
        """单例模式"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        """初始化请求器"""
        if self._initialized:
            return

        self._api_token_set = False
        self._tushare_pro = None

        # 初始化各接口的限流器
        self._rate_limiters = {}
        for api_name, max_requests in RATE_LIMIT_CONFIG.items():
            self._rate_limiters[api_name] = RateLimiter(max_requests=max_requests)

        # 初始化掘金API Token
        from gm.api import set_token
        GOLD_MINER_API_TOKEN = "2e664976b46df6a0903672349c30226ac68e7bf3"
        set_token(GOLD_MINER_API_TOKEN)
        self._api_token_set = True

        # 如果配置使用tushare，初始化tushare
        if QUERY_API_TYPE == "tushare":
            import tushare as ts
            self._tushare_pro = ts.pro_api(TUSHARE_API_TOKEN)
            self._tushare_pro._DataApi__http_url = TUSHARE_PROXY_URL

        self._initialized = True

    def get_daily_data(self, symbol: str, start_date: str, end_date: str, fields: Optional[str] = None) -> Optional[pd.DataFrame]:
        """
        获取日线数据（统一使用掘金接口）

        Args:
            symbol: 股票代码 (掘金格式: SHSE.600487)
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            fields: 要返回的字段列表

        Returns:
            DataFrame，格式与掘金history接口返回一致
        """
        print(f"[ExternalData] 调用 get_daily_data - symbol={symbol}, start_date={start_date}, end_date={end_date}, fields={fields}")

        # 限流检查（阻塞等待直到有可用配额）
        self._rate_limiters['get_daily_data'].wait_and_acquire()

        from gm.api import history, ADJUST_PREV

        start_time = f"{start_date} 00:00:00"
        end_time = f"{end_date} 23:59:59"

        return history(
            symbol=symbol,
            frequency='1d',
            start_time=start_time,
            end_time=end_time,
            fields=fields or 'symbol,eob,open,close,high,low,volume,amount,pre_close',
            adjust=ADJUST_PREV,
            df=True
        )

    def get_minute_data(self, symbol: str, trade_date: str, start_time: str, end_time: str) -> Optional[pd.DataFrame]:
        """
        获取分钟数据（统一使用掘金接口）

        Args:
            symbol: 股票代码 (掘金格式: SHSE.600487)
            trade_date: 交易日期 (YYYY-MM-DD)
            start_time: 开始时间 (HH:MM:SS)
            end_time: 结束时间 (HH:MM:SS)

        Returns:
            DataFrame，格式与掘金history接口返回一致
        """
        print(f"[ExternalData] 调用 get_minute_data - symbol={symbol}, trade_date={trade_date}, start_time={start_time}, end_time={end_time}")

        # 限流检查（阻塞等待直到有可用配额）
        self._rate_limiters['get_minute_data'].wait_and_acquire()

        from gm.api import history

        full_start = f"{trade_date} {start_time}"
        full_end = f"{trade_date} {end_time}"

        return history(
            symbol=symbol,
            frequency='60s',
            start_time=full_start,
            end_time=full_end,
            fields='symbol,eob,open,close,high,low,volume,amount',
            df=True
        )

    def get_tick_data(self, symbol: str, trade_date: str, start_time: str, end_time: str) -> Optional[pd.DataFrame]:
        """
        获取Tick数据（统一使用掘金接口）

        Args:
            symbol: 股票代码 (掘金格式: SHSE.600487)
            trade_date: 交易日期 (YYYY-MM-DD)
            start_time: 开始时间 (HH:MM:SS)
            end_time: 结束时间 (HH:MM:SS)

        Returns:
            DataFrame，格式与掘金history接口返回一致
        """
        print(f"[ExternalData] 调用 get_tick_data - symbol={symbol}, trade_date={trade_date}, start_time={start_time}, end_time={end_time}")

        # 限流检查（阻塞等待直到有可用配额）
        self._rate_limiters['get_tick_data'].wait_and_acquire()

        from gm.api import history

        full_start = f"{trade_date} {start_time}"
        full_end = f"{trade_date} {end_time}"

        return history(
            symbol=symbol,
            frequency='tick',
            start_time=full_start,
            end_time=full_end,
            fields='symbol,price,volume,cum_amount,cum_volume,created_at',
            df=True
        )

    def get_auction_data(self, symbol: str, trade_date: str) -> Optional[pd.DataFrame]:
        """
        获取竞价数据（根据配置决定使用掘金还是Tushare接口）

        Args:
            symbol: 股票代码 (掘金格式: SHSE.600487)
            trade_date: 交易日期 (YYYY-MM-DD)

        Returns:
            DataFrame，包含竞价成交信息
        """
        print(f"[ExternalData] 调用 get_auction_data - symbol={symbol}, trade_date={trade_date}")

        # 限流检查（阻塞等待直到有可用配额）
        self._rate_limiters['get_auction_data'].wait_and_acquire()

        try:
            import time
            start_time = time.time()

            if QUERY_API_TYPE == "tushare" and self._tushare_pro:
                ts_code = self._symbol_to_tushare(symbol)
                date_str = trade_date.replace('-', '')

                # Tushare的stk_auction接口（参考tushare_data_get.py mode=1）
                df = self._tushare_pro.stk_auction(
                    ts_code=ts_code,
                    trade_date=date_str
                )

                if df is not None and not df.empty:
                    # 转换为统一格式
                    df['symbol'] = df['ts_code'].apply(self._tushare_to_symbol)
                    df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y-%m-%d')
                    duration = time.time() - start_time
                    print(f"[ExternalData] Tushare获取竞价数据耗时: {duration:.3f}s")
                    return df

            # 默认使用掘金API
            from gm.api import history
            data = history(
                symbol=symbol,
                frequency='1d',
                start_time=trade_date + ' 09:15:00',
                end_time=trade_date + ' 09:25:00',
                fields='open,close,high,low,volume,amount,pre_close,eob',
                df=True
            )

            duration = time.time() - start_time
            print(f"[ExternalData] get_auction_data 耗时: {duration:.3f}s")

            return data
        except Exception as e:
            print(f"[ExternalData] get_auction_data 失败: {e}")
            import traceback
            traceback.print_exc()
            return None

    def get_instruments(self) -> Optional[pd.DataFrame]:
        """
        获取股票基本信息列表（根据配置决定使用掘金还是Tushare接口）

        Returns:
            DataFrame，包含股票基本信息（symbol, sec_name, exchange等）
        """
        print(f"[ExternalData] 调用 get_instruments")

        # 限流检查（阻塞等待直到有可用配额）
        self._rate_limiters['get_instruments'].wait_and_acquire()

        try:
            import time
            start_time = time.time()

            if QUERY_API_TYPE == "tushare" and self._tushare_pro:
                # 使用Tushare获取股票列表
                df = self._tushare_pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,market,list_date')

                if df is not None and not df.empty:
                    # 转换为统一格式
                    result_list = []
                    for idx, row in df.iterrows():
                        ts_code = row.get('ts_code', '')
                        # 转换为掘金格式
                        if ts_code.endswith('.SH'):
                            symbol = f"SHSE.{ts_code.replace('.SH', '')}"
                        elif ts_code.endswith('.SZ'):
                            symbol = f"SZSE.{ts_code.replace('.SZ', '')}"
                        else:
                            continue

                        result_list.append({
                            'symbol': symbol,
                            'sec_name': row.get('name', '未知'),
                            'exchange': 'SHSE' if '.SH' in ts_code else 'SZSE',
                            'sec_id': row.get('symbol', ''),
                            'industry': row.get('industry', ''),
                            'area': row.get('area', ''),
                            'market': row.get('market', ''),
                            'list_date': row.get('list_date', ''),
                        })

                    result_df = pd.DataFrame(result_list)
                    duration = time.time() - start_time
                    print(f"[ExternalData] get_instruments 耗时: {duration:.3f}s, 获取 {len(result_df)} 条数据")
                    return result_df

            # 默认使用掘金API
            from gm.api import get_instruments
            instruments = get_instruments(exchanges=['SHSE', 'SZSE'], sec_types=[1], df=True)

            duration = time.time() - start_time
            if instruments is not None and not instruments.empty:
                print(f"[ExternalData] get_instruments 耗时: {duration:.3f}s, 获取 {len(instruments)} 条数据")

            return instruments
        except Exception as e:
            print(f"[ExternalData] get_instruments 失败: {e}")
            import traceback
            traceback.print_exc()
            return None

    def _symbol_to_tushare(self, symbol: str) -> str:
        """
        将掘金格式的symbol转换为Tushare格式
        SHSE.600487 -> 600487.SH
        SZSE.000001 -> 000001.SZ
        """
        if '.' not in symbol:
            return symbol

        exchange, code = symbol.split('.')
        if exchange == 'SHSE':
            return f"{code}.SH"
        elif exchange == 'SZSE':
            return f"{code}.SZ"
        return symbol

    def _tushare_to_symbol(self, ts_code: str) -> str:
        """
        将Tushare格式的代码转换为掘金格式
        600487.SH -> SHSE.600487
        000001.SZ -> SZSE.000001
        """
        if '.' not in ts_code:
            return ts_code

        code, exchange = ts_code.split('.')
        if exchange == 'SH':
            return f"SHSE.{code}"
        elif exchange == 'SZ':
            return f"SZSE.{code}"
        return ts_code

    def get_money_flow_data(self, symbol: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """
        获取资金流向数据（使用Tushare的moneyflow_dc接口）

        Args:
            symbol: 股票代码 (掘金格式: SHSE.600487)
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)

        Returns:
            DataFrame，包含资金流向信息
        """
        print(f"[ExternalData] 调用 get_money_flow_data - symbol={symbol}, start_date={start_date}, end_date={end_date}")

        # 限流检查（阻塞等待直到有可用配额）
        self._rate_limiters['get_money_flow_data'].wait_and_acquire()

        try:
            import time
            start_time = time.time()

            if self._tushare_pro:
                ts_code = self._symbol_to_tushare(symbol)
                start_date_str = start_date.replace('-', '')
                end_date_str = end_date.replace('-', '')

                # 使用Tushare的moneyflow_dc接口获取资金流向数据
                df = self._tushare_pro.moneyflow_dc(
                    ts_code=ts_code,
                    start_date=start_date_str,
                    end_date=end_date_str
                )

                if df is not None and not df.empty:
                    # 转换为统一格式
                    df['symbol'] = df['ts_code'].apply(self._tushare_to_symbol)
                    df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y-%m-%d')
                    duration = time.time() - start_time
                    print(f"[ExternalData] Tushare获取资金流向数据耗时: {duration:.3f}s")
                    return df
                else:
                    print(f"[ExternalData] Tushare未获取到资金流向数据: {ts_code}")
                    return None
            else:
                print(f"[ExternalData] Tushare API未初始化")
                return None

        except Exception as e:
            print(f"[ExternalData] get_money_flow_data 失败: {e}")
            import traceback
            traceback.print_exc()
            return None


# 创建全局单例实例
_query_handler = None

def get_query_handler() -> ExternalDataQueryHandler:
    """
    获取外部数据查询请求器单例实例
    """
    global _query_handler
    if _query_handler is None:
        _query_handler = ExternalDataQueryHandler()
    return _query_handler
