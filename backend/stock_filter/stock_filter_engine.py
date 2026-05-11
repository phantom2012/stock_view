import logging
from datetime import datetime
from typing import List, Optional

from models import StockDetail
from models.filter_params import FilterParams
from shared.trade_date_util import TradeDateUtil
from stock_cache import get_stock_cache
from common.stock_code_convert import to_pure_code
from .stock_analyzer import StockAnalyzer

logger = logging.getLogger(__name__)


class StockFilterEngine:
    """
    筛选引擎层
    负责批量遍历股票列表，调用 StockAnalyzer 进行单股分析，组装最终结果
    不涉及股票池获取、结果保存、数据同步等编排逻辑
    """

    def __init__(self):
        self.analyzer = StockAnalyzer()
        self.cache = get_stock_cache()
        self.trade_date_util = TradeDateUtil()

    def filter_stocks(self, symbols: List[str], trade_date: datetime,
                      params: FilterParams) -> List[StockDetail]:
        """
        综合筛选股票 - 批量遍历并组装结果

        Args:
            symbols: 股票代码列表（掘金格式，如 ['SHSE.600000', ...]）
            trade_date: 交易日期
            params: 筛选参数对象

        Returns:
            筛选结果列表（StockDetail对象列表）
        """
        results = []

        for symbol in symbols:
            stock_code = to_pure_code(symbol)

            # 1. 检查是否为10cm股票
            if not self.analyzer.check_is_10cm(symbol):
                continue

            # 2. 检查性能条件
            performance = self.analyzer.check_performance(symbol, trade_date, params)
            if not performance.is_pass:
                continue

            # 3. 尾盘超预期条件检查
            auction_data = None
            if params.weipan_exceed > 0:
                auction_data = self.analyzer.check_tail_auction_condition(symbol, trade_date)
                if not auction_data:
                    continue

            # 4. 获取当日涨幅
            today_gain = self.analyzer.get_stock_day_gain(symbol, trade_date)

            # 5. 获取次日涨幅（只有当次日不是今天时才获取）
            next_day_rise = None
            if trade_date.date() < datetime.now().date():
                next_trade_date_str = self.trade_date_util.get_next_trade_date(trade_date)
                if next_trade_date_str:
                    next_trading_day = datetime.strptime(next_trade_date_str, '%Y-%m-%d')
                    next_day_rise = self.analyzer.get_stock_day_gain(symbol, next_trading_day)

            # 6. 计算升浪形态得分
            rising_wave_score = 0
            if params.rising_wave == 1:
                rising_wave_score = self.analyzer.calculate_rising_wave_score(
                    symbol, trade_date, params.interval_days
                )
                if rising_wave_score <= 0:
                    continue


            # 7. 获取昨均价、昨收盘价、昨涨幅
            pre_avg_price = 0
            pre_close_price = 0
            pre_price_gain = 0
            try:
                prev_trade_data = self.cache.get_previous_trade_data(symbol, trade_date)
                if prev_trade_data:
                    pre_close_price = prev_trade_data.get('pre_close_price', 0)
                    pre_avg_price = prev_trade_data.get('pre_avg_price', 0)
                    pre_price_gain = prev_trade_data.get('pre_price_gain', 0)
            except Exception as e:
                logger.error(f"Error getting previous trade data for {symbol}: {e}")

            # 8. 获取股票名称
            stock_name = self.cache.get_stock_name(symbol)

            # 如果名称仍为未知，尝试通过批量接口获取
            if stock_name == '未知':
                try:
                    names_map = self.cache.fetch_stock_names_bulk([stock_code])
                    stock_name = names_map.get(stock_code, '未知')
                except Exception:
                    pass

            # 9. 获取竞价数据以获取volume_ratio
            volume_ratio = 0
            try:
                auction_data_full = self.cache.get_auction_data(symbol, trade_date)
                volume_ratio = auction_data_full.get('volume_ratio', 0)
            except Exception as e:
                logger.error(f"[StockFilterEngine] Error getting auction data for {symbol}: {e}")

            # 10. 获取当日开盘价、收盘价和次日数据
            open_price = 0.0
            close_price = 0.0
            next_open_price = 0.0
            next_close_price = 0.0
            try:
                day_data = self.cache.get_stock_day_data(symbol, trade_date, force_refresh=False)
                if day_data is not None and not day_data.empty:
                    row = day_data.iloc[0]
                    open_price = row.get('open', 0.0)
                    close_price = row.get('close', 0.0)

                # 获取次日开盘价和收盘价
                if trade_date.date() < datetime.now().date():
                    next_trade_date_str = self.trade_date_util.get_next_trade_date(trade_date)
                    if next_trade_date_str:
                        next_trading_day = datetime.strptime(next_trade_date_str, '%Y-%m-%d')
                        next_day_data = self.cache.get_stock_day_data(symbol, next_trading_day, force_refresh=False)
                        if next_day_data is not None and not next_day_data.empty:
                            next_row = next_day_data.iloc[0]
                            next_open_price = next_row.get('open', 0.0)
                            next_close_price = next_row.get('close', 0.0)
            except Exception as e:
                logger.error(f"[StockFilterEngine] Error getting day data for {symbol}: {e}")

            # 11. 构建结果
            result = StockDetail.create(
                symbol=symbol,
                code=stock_code,
                stock_name=stock_name,
                auction_data=auction_data,
                open_volume_ratio=volume_ratio,
                interval_max_rise=performance.interval_max_rise,
                max_day_rise=performance.max_day_rise,
                today_gain=today_gain if today_gain is not None else 0.0,
                next_day_rise=next_day_rise if next_day_rise is not None else 0.0,
                trade_date=trade_date.strftime('%Y-%m-%d'),
                rising_wave_score=rising_wave_score,
                weipan_exceed=params.weipan_exceed,
                zaopan_exceed=params.zaopan_exceed,
                rising_wave=params.rising_wave,
                pre_avg_price=pre_avg_price,
                pre_close_price=pre_close_price,
                pre_price_gain=pre_price_gain,
                open_price=open_price,
                close_price=close_price,
                next_open_price=next_open_price,
                next_close_price=next_close_price
            )

            results.append(result)

        return results
