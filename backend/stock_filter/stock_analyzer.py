import logging
from datetime import datetime
from typing import Optional, Dict, Any

from models import StockPerformance, StockInfo, get_session_ro
from models.filter_params import FilterParams
from stock_cache import get_stock_cache
from .stock_wave_analyzer import StockWaveAnalyzer

logger = logging.getLogger(__name__)


class StockAnalyzer:
    """
    单股分析器
    提供个股级别的纯算法分析，不涉及批量遍历和结果组装
    负责：盘前竞价条件检查、股票属性判断、得分计算等个股维度的处理
    K线形态分析委托给 StockWaveAnalyzer
    """

    def __init__(self):
        self.cache = get_stock_cache()
        self.wave_analyzer = StockWaveAnalyzer(self.cache)

    # ==================== 核心性能检查 ====================

    def check_performance(self, symbol: str, trade_date: datetime,
                          params: FilterParams) -> StockPerformance:
        """
        检查股票近N个交易日的表现

        Args:
            symbol: 股票代码（掘金格式）
            trade_date: 交易日期
            params: 筛选参数对象

        Returns:
            StockPerformance对象
        """
        try:
            # 从缓存获取日K线数据（多获取1天数据用于计算）
            data = self.cache.get_history_data(
                symbol, days=params.interval_days + 1,
                trade_date=trade_date, force_refresh=False
            )

            if data is not None and len(data) >= 2:
                # 取最后interval_days条数据用于计算
                data = data.tail(params.interval_days)

                # 1. 计算股价相对于近期最高价的比例
                price_ratio = self.wave_analyzer.calculate_price_ratio(data)

                # 如果设置了股价比例阈值，检查是否满足
                if params.prev_high_price_rate > 0 and price_ratio < params.prev_high_price_rate:
                    return StockPerformance(
                        is_pass=False, interval_max_rise=0,
                        max_day_rise=0, prev_high_price_rate=round(price_ratio, 2)
                    )

                # 2. 计算区间最大涨幅（首尾收盘价）
                interval_max_rise_value = self.wave_analyzer.calculate_period_gain(data)

                # 3. 计算日内最大涨幅
                max_day_rise = self.wave_analyzer.calculate_max_day_rise(data, params.recent_days)

                # 保留小数点后2位
                interval_max_rise_value = round(interval_max_rise_value, 2)
                max_day_rise = round(max_day_rise, 2)
                price_ratio = round(price_ratio, 2)

                # 检查条件：区间涨幅和日内涨幅都需要大于阈值
                is_pass = (abs(interval_max_rise_value) >= params.interval_max_rise
                           and max_day_rise >= params.recent_max_day_rise)
                return StockPerformance(
                    is_pass=is_pass, interval_max_rise=interval_max_rise_value,
                    max_day_rise=max_day_rise, prev_high_price_rate=price_ratio
                )

            return StockPerformance(
                is_pass=False, interval_max_rise=0,
                max_day_rise=0, prev_high_price_rate=0
            )
        except Exception as e:
            logger.error(f"[StockAnalyzer] Error checking performance for {symbol}: {e}")
            return StockPerformance(
                is_pass=False, interval_max_rise=0,
                max_day_rise=0, prev_high_price_rate=0
            )

    # ==================== 升浪形态（委托给 StockWaveAnalyzer） ====================

    def calculate_rising_wave_score(self, symbol: str, trade_date: datetime,
                                    recent_days: int = 10) -> float:
        return self.wave_analyzer.calculate_rising_wave_score(symbol, trade_date, recent_days)

    # ==================== 竞价条件检查 ====================

    def check_tail_auction_condition(self, symbol: str, trade_date: datetime) -> Optional[Dict[str, Any]]:
        """
        检查尾盘竞价条件（获取14:57-15:00的尾盘数据，要求价格上涨）

        Args:
            symbol: 股票代码（掘金格式）
            trade_date: 交易日期

        Returns:
            竞价数据字典，如果不符合条件返回None
        """
        try:
            tail_data = self.cache.get_tail_auction_data(symbol, trade_date)

            if tail_data:
                auction_start = tail_data['open_price']
                auction_end = tail_data['close_price']

                # 竞价结束价 >= 竞价开始价才符合条件
                if auction_end >= auction_start:
                    return tail_data

            return None
        except Exception as e:
            logger.error(f"[StockAnalyzer] Error checking tail auction for {symbol}: {e}")
            return None

    # ==================== 涨幅获取 ====================

    def get_stock_day_gain(self, symbol: str, trade_date: datetime) -> Optional[float]:
        """
        获取股票指定日期的涨幅

        Args:
            symbol: 股票代码（掘金格式）
            trade_date: 交易日期

        Returns:
            涨幅百分比，如果无法计算则返回 None
        """
        try:
            data = self.cache.get_stock_day_data(symbol, trade_date, force_refresh=False)

            if data is not None and not data.empty:
                row = data.iloc[0]
                today_close = row['close']
                prev_close = row['pre_close']

                if prev_close is not None and prev_close != 0:
                    gain = round((today_close - prev_close) / prev_close * 100, 2)
                    return gain

            return None
        except Exception as e:
            logger.error(f"[StockAnalyzer] Error getting day gain for {symbol} on {trade_date}: {e}")
            return None

    # ==================== 股票属性判断 ====================

    def check_is_main_board(self, symbol: str) -> bool:
        """
        检查是否是主板股票
        主板股票定义：排除科创板、创业板、北交所后的股票

        Args:
            symbol: 股票代码

        Returns:
            bool: 是否是主板股票
        """
        stock_code = symbol.split('.')[-1] if '.' in symbol else symbol

        # 排除科创板 (688)
        if stock_code.startswith('688'):
            return False

        # 排除创业板 (300, 301)
        if stock_code.startswith('300') or stock_code.startswith('301'):
            return False

        # 排除北交所 (8, 4, 92开头)
        if stock_code.startswith('8') or stock_code.startswith('4') or stock_code.startswith('92'):
            return False

        # 剩下的就是主板股票（包括60、00、002、003等）
        return True

    def check_is_10cm(self, symbol: str) -> bool:
        """
        检查是否为10cm涨跌幅股票（主板非ST股票）
        过滤条件：
        1. 必须是主板股票（60或00开头）
        2. 排除ST、*ST股票
        3. 排除退市股票

        Args:
            symbol: 股票代码

        Returns:
            bool: 是否符合10cm条件
        """
        # 1. 首先检查是否是主板股票
        if not self.check_is_main_board(symbol):
            return False

        # 2. 检查是否退市
        if not self._check_delisted(symbol):
            return False

        # 3. 获取股票名称并检查 ST
        try:
            stock_name = self._fetch_stock_name(symbol)

            if stock_name == '未知':
                logger.warning(f"[StockAnalyzer] 警告: 股票 {symbol} 无法获取有效名称，但允许通过筛选")
            elif 'ST' in stock_name or '*ST' in stock_name:
                logger.info(f"[StockAnalyzer] 股票 {symbol} 是 ST 股票，已剔除")
                return False

        except Exception as e:
            logger.error(f"[StockAnalyzer] 检查10cm股票时出错 {symbol}: {e}，已剔除")
            return False

        return True

    # ==================== 预期得分计算 ====================

    def calculate_exp_score(self, auction_data: Dict[str, Any],
                            rising_wave_score: int = 0) -> float:
        """
        计算预期得分

        Args:
            auction_data: 竞价数据
            rising_wave_score: 升浪形态得分

        Returns:
            预期得分（保留2位小数）
        """
        begin_price = auction_data.get('begin_price', auction_data.get('open_price', 0))
        end_price = auction_data.get('end_price', auction_data.get('close_price', 0))

        if begin_price != 0:
            base_score = (end_price - begin_price) / begin_price * 10000
            exp_score = base_score + rising_wave_score
            return round(exp_score, 2)

        return 0

    # ==================== 私有辅助方法 ====================

    def _check_delisted(self, symbol: str) -> bool:
        """
        检查股票是否已退市（仅从数据库读取）

        Args:
            symbol: 股票代码

        Returns:
            bool: True=未退市(有效), False=已退市
        """
        try:
            pure_code = symbol.split('.')[-1] if '.' in symbol else symbol
            with get_session_ro() as db:
                row = db.query(StockInfo).filter(StockInfo.code == pure_code).first()

                if row is None:
                    logger.info(f"[StockAnalyzer] 股票 {symbol} 在数据库中找不到，视为无效股票，已剔除")
                    return False

                list_status = row.list_status
                delist_date = row.delist_date

                if list_status == 'L':
                    return True

                if delist_date is not None and delist_date.strip():
                    try:
                        delisted_dt = datetime.strptime(delist_date[:10], '%Y-%m-%d')
                        if delisted_dt < datetime.now():
                            logger.info(f"[StockAnalyzer] 股票 {symbol} 已退市，日期: {delist_date}，已剔除")
                            return False
                    except Exception:
                        pass

                if list_status != 'L':
                    logger.info(f"[StockAnalyzer] 股票 {symbol} 状态异常: {list_status}，已剔除")
                    return False

                return True
        except Exception as e:
            logger.error(f"[StockAnalyzer] 查询股票 {symbol} 退市状态失败: {e}")
            return True

    def _fetch_stock_name(self, symbol: str) -> str:
        """
        获取股票名称（仅从数据库读取）

        Args:
            symbol: 股票代码

        Returns:
            str: 股票名称，获取失败返回'未知'
        """
        stock_name = self.cache.get_stock_name(symbol)

        if stock_name == '未知':
            logger.warning(f"[StockAnalyzer] 股票 {symbol} 在数据库中也找不到，视为无效股票")

        return stock_name
