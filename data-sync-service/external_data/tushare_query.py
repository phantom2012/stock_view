import pandas as pd
from typing import Optional
import time

from .rate_limiter import RateLimiter

TUSHARE_API_TOKEN = "Zku47OUVCydb1095ShpVSzn4u7pea7bFvgLNoCjIENA"
TUSHARE_PROXY_URL = "http://47.109.59.144:8989/dataapi"

RATE_LIMIT_CONFIG = {
    'get_auction_data': 120,
    'get_instruments': 120,
    'get_daily_basic_data': 120,
}


class TushareQuery:
    def __init__(self):
        self._tushare_pro = None
        self._rate_limiters = {}
        for api_name, max_requests in RATE_LIMIT_CONFIG.items():
            self._rate_limiters[api_name] = RateLimiter(max_requests=max_requests)
        self._init_tushare()

    def _init_tushare(self):
        import tushare as ts
        self._tushare_pro = ts.pro_api(TUSHARE_API_TOKEN)
        self._tushare_pro._DataApi__http_url = TUSHARE_PROXY_URL

    def get_auction_data(self, symbol: str, trade_date: str) -> Optional[pd.DataFrame]:
        print(f"[TushareQuery] get_auction_data - symbol={symbol}, date={trade_date}")
        self._rate_limiters['get_auction_data'].wait_and_acquire('get_auction_data')
        try:
            start_time = time.time()
            if self._tushare_pro:
                ts_code = self._symbol_to_tushare(symbol)
                date_str = trade_date.replace('-', '')
                df = self._tushare_pro.stk_auction(ts_code=ts_code, trade_date=date_str)
                if df is not None and not df.empty:
                    df['symbol'] = df['ts_code'].apply(self._tushare_to_symbol)
                    df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y-%m-%d')
                    print(f"[TushareQuery] Tushare竞价数据耗时: {time.time()-start_time:.3f}s")
                    return df
                print(f"[TushareQuery] Tushare未返回竞价数据: {ts_code}")
                return None
            print(f"[TushareQuery] Tushare API未初始化")
            return None
        except Exception as e:
            print(f"[TushareQuery] get_auction_data 失败: {e}")
            import traceback; traceback.print_exc()
            return None

    def get_instruments(self, list_status: str = None) -> Optional[pd.DataFrame]:
        print(f"[TushareQuery] get_instruments, list_status={list_status}")
        self._rate_limiters['get_instruments'].wait_and_acquire('get_instruments')
        try:
            start_time = time.time()
            if self._tushare_pro:
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
                                'list_status': status,
                            })

                if all_result_list:
                    result_df = pd.DataFrame(all_result_list)
                    print(f"[TushareQuery] get_instruments 耗时: {time.time()-start_time:.3f}s, {len(result_df)}条")
                    return result_df
            return None
        except Exception as e:
            print(f"[TushareQuery] get_instruments 失败: {e}")
            import traceback; traceback.print_exc()
            return None

    def get_daily_basic_data(self, symbol: Optional[str] = None, trade_date: Optional[str] = None) -> Optional[dict]:
        is_batch = symbol is None
        print(f"[TushareQuery] get_daily_basic_data - symbol={symbol}, date={trade_date}, batch={is_batch}")
        self._rate_limiters['get_daily_basic_data'].wait_and_acquire('get_daily_basic_data')
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
                        print(f"[TushareQuery] 批量查询全市场: {len(result)}只, 耗时: {time.time()-start_time:.3f}s")
                        return result
                    else:
                        data = df.iloc[0].to_dict()
                        data['trade_date'] = str(data['trade_date'])
                        print(f"[TushareQuery] 单只查询耗时: {time.time()-start_time:.3f}s")
                        return data
                print(f"[TushareQuery] Tushare未返回每日基本面数据")
                return None
            print(f"[TushareQuery] Tushare API未初始化")
            return None
        except Exception as e:
            print(f"[TushareQuery] get_daily_basic_data 失败: {e}")
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
