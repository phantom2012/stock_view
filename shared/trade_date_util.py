"""
交易日查询工具类（单例模式）
从 trade_calendar 表查询交易日，替代 Baostock 在线查询

初始化逻辑：
1. 第一次创建实例时，检查 trade_calendar 表中是否有当天日期的数据
2. 如果没有，则通过 Baostock 查询最近3天的交易日数据
3. 将查询结果更新到 trade_calendar 表
4. 缓存最近90个交易日列表，后续查询直接从缓存获取
"""
import logging
from datetime import datetime, timedelta
from typing import Optional, List

from .db import get_session, get_session_ro, TradeCalendar

logger = logging.getLogger(__name__)

CACHE_DAYS = 90


class TradeDateUtil:
    """
    交易日查询工具类（单例模式）
    从 trade_calendar 表查询交易日数据，支持缓存机制
    """

    _instance = None
    _initialized = False

    def __new__(cls, *args, **kwargs):
        """单例模式实现"""
        if cls._instance is None:
            cls._instance = super(TradeDateUtil, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        """初始化交易日工具类"""
        if TradeDateUtil._initialized:
            return

        self._latest_trade_date = None
        self._cached_trade_dates = []
        self._init_trade_calendar()
        self._cache_recent_trade_dates()
        TradeDateUtil._initialized = True

    def _init_trade_calendar(self):
        """
        初始化交易日历
        检查当天日期的数据是否存在，不存在则从 Baostock 同步
        """
        today = datetime.now().strftime("%Y-%m-%d")

        try:
            with get_session_ro() as db:
                exists = db.query(TradeCalendar).filter(
                    TradeCalendar.calendar_date == today
                ).first()

                if exists:
                    logger.info(f"trade_calendar 表已存在当天日期数据: {today}")
                    return

            logger.info(f"trade_calendar 表不存在当天日期数据，从 Baostock 同步最近3天数据...")
            self._sync_from_baostock()
        except Exception as e:
            logger.error(f"初始化交易日历失败: {e}")

    def _sync_from_baostock(self):
        """
        从 Baostock 查询最近3天的交易日数据并更新到数据库
        """
        try:
            import baostock as bs

            lg = bs.login()
            if lg.error_code != '0':
                logger.error(f"Baostock 登录失败: {lg.error_msg}")
                return

            try:
                end_date = datetime.now()
                start_date = end_date - timedelta(days=7)

                start_str = start_date.strftime("%Y-%m-%d")
                end_str = end_date.strftime("%Y-%m-%d")

                logger.info(f"查询 Baostock 交易日历: {start_str} 至 {end_str}")
                df = bs.query_trade_dates(start_date=start_str, end_date=end_str)
                data = df.get_data()

                if data.empty:
                    logger.warning("Baostock 返回空数据")
                    return

                with get_session() as db:
                    for _, row in data.iterrows():
                        calendar_date = row['calendar_date']
                        is_trading_day = 1 if row['is_trading_day'] == '1' else 0
                        date_obj = datetime.strptime(calendar_date, "%Y-%m-%d")

                        existing = db.query(TradeCalendar).filter(
                            TradeCalendar.calendar_date == calendar_date
                        ).first()

                        if existing:
                            existing.is_trading_day = is_trading_day
                            existing.year = date_obj.year
                            existing.month = date_obj.month
                            existing.update_time = datetime.now()
                        else:
                            new_record = TradeCalendar(
                                calendar_date=calendar_date,
                                is_trading_day=is_trading_day,
                                year=date_obj.year,
                                month=date_obj.month,
                                update_time=datetime.now()
                            )
                            db.add(new_record)

                    db.commit()
                    logger.info(f"成功同步 {len(data)} 条交易日历数据")

            finally:
                bs.logout()

        except ImportError:
            logger.error("未安装 baostock 库，请先安装: pip install baostock")
        except Exception as e:
            logger.error(f"从 Baostock 同步交易日历失败: {e}")

    def _cache_recent_trade_dates(self):
        """缓存最近90个交易日"""
        try:
            with get_session_ro() as db:
                rows = db.query(TradeCalendar).filter(
                    TradeCalendar.is_trading_day == 1
                ).order_by(TradeCalendar.calendar_date.desc()).limit(CACHE_DAYS).all()

                self._cached_trade_dates = [row.calendar_date for row in rows]
                self._cached_trade_dates.reverse()

                logger.info(f"缓存了 {len(self._cached_trade_dates)} 个交易日")
        except Exception as e:
            logger.error(f"缓存交易日失败: {e}")

    def refresh_cache(self):
        """刷新交易日缓存"""
        self._latest_trade_date = None
        self._cache_recent_trade_dates()
        logger.info("交易日缓存已刷新")

    def get_latest_trade_date(self) -> Optional[str]:
        """
        获取最近一个交易日的日期（带时间过滤）

        逻辑：
        - 如果当前北京时间 <= 15:30，则门槛日期为昨日
        - 如果当前北京时间 > 15:30，则门槛日期为今日
        - 返回 <= 门槛日期的最新一个交易日

        Returns:
            str: 最近一个交易日的日期，格式为 'YYYY-MM-DD'，如果查询失败返回 None
        """
        if self._latest_trade_date is not None:
            return self._latest_trade_date

        try:
            import pytz
            beijing_tz = pytz.timezone('Asia/Shanghai')
            now_beijing = datetime.now(beijing_tz)
        except ImportError:
            now_beijing = datetime.now()

        cutoff_time = now_beijing.replace(hour=15, minute=30, second=0, microsecond=0)

        if now_beijing > cutoff_time:
            threshold_date = now_beijing.strftime("%Y-%m-%d")
        else:
            yesterday = now_beijing - timedelta(days=1)
            threshold_date = yesterday.strftime("%Y-%m-%d")

        try:
            with get_session_ro() as db:
                row = db.query(TradeCalendar).filter(
                    TradeCalendar.is_trading_day == 1,
                    TradeCalendar.calendar_date <= threshold_date
                ).order_by(TradeCalendar.calendar_date.desc()).first()

                if row:
                    self._latest_trade_date = row.calendar_date
                    return row.calendar_date

                logger.warning(f"未找到 <= {threshold_date} 的交易日")
                return None
        except Exception as e:
            logger.error(f"查询最新交易日失败: {e}")
            return None

    def get_recent_trade_dates(self, days: int = 5, trade_date: Optional[datetime] = None) -> List[str]:
        """
        获取最近N个交易日的日期（优先从缓存获取）

        Args:
            days: 需要获取的交易日数量，默认为5
            trade_date: 可选，指定的交易日。如果不传，则使用当前时间计算门槛日期

        Returns:
            list: 最近N个交易日的日期列表，格式为 ['YYYY-MM-DD', ...]，升序排列
        """
        if trade_date is None:
            if days <= len(self._cached_trade_dates):
                return self._cached_trade_dates[-days:]

        try:
            import pytz
            beijing_tz = pytz.timezone('Asia/Shanghai')
            now_beijing = datetime.now(beijing_tz)
        except ImportError:
            now_beijing = datetime.now()

        if trade_date is None:
            cutoff_time = now_beijing.replace(hour=15, minute=30, second=0, microsecond=0)
            if now_beijing > cutoff_time:
                threshold_date = now_beijing.strftime("%Y-%m-%d")
            else:
                threshold_date = (now_beijing - timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            threshold_date = trade_date.strftime("%Y-%m-%d")

        try:
            with get_session_ro() as db:
                rows = db.query(TradeCalendar).filter(
                    TradeCalendar.is_trading_day == 1,
                    TradeCalendar.calendar_date <= threshold_date
                ).order_by(TradeCalendar.calendar_date.desc()).limit(days).all()

                result = [row.calendar_date for row in rows]
                result.reverse()

                if len(result) < days:
                    logger.warning(f"只找到 {len(result)} 个交易日，不足 {days} 个")

                return result
        except Exception as e:
            logger.error(f"查询最近交易日失败: {e}")
            return []

    def get_next_trade_date(self, trade_date: datetime) -> Optional[str]:
        """
        获取指定日期之后的下一个交易日（优先从缓存获取）

        Args:
            trade_date: 指定的日期（datetime对象）

        Returns:
            str: 下一个交易日的日期，格式为 'YYYY-MM-DD'
        """
        date_str = trade_date.strftime("%Y-%m-%d")

        try:
            idx = self._cached_trade_dates.index(date_str)
            if idx + 1 < len(self._cached_trade_dates):
                return self._cached_trade_dates[idx + 1]
        except ValueError:
            pass

        try:
            with get_session_ro() as db:
                row = db.query(TradeCalendar).filter(
                    TradeCalendar.is_trading_day == 1,
                    TradeCalendar.calendar_date > date_str
                ).order_by(TradeCalendar.calendar_date.asc()).first()

                if row:
                    return row.calendar_date

                logger.warning(f"未找到 {date_str} 之后的下一个交易日")
                return None
        except Exception as e:
            logger.error(f"查询下一个交易日失败: {e}")
            return None

    def get_previous_trade_date(self, trade_date: datetime) -> Optional[str]:
        """
        获取指定日期之前的上一个交易日（优先从缓存获取）

        Args:
            trade_date: 指定的日期（datetime对象）

        Returns:
            str: 上一个交易日的日期，格式为 'YYYY-MM-DD'
        """
        date_str = trade_date.strftime("%Y-%m-%d")

        try:
            idx = self._cached_trade_dates.index(date_str)
            if idx > 0:
                return self._cached_trade_dates[idx - 1]
        except ValueError:
            pass

        try:
            with get_session_ro() as db:
                row = db.query(TradeCalendar).filter(
                    TradeCalendar.is_trading_day == 1,
                    TradeCalendar.calendar_date < date_str
                ).order_by(TradeCalendar.calendar_date.desc()).first()

                if row:
                    return row.calendar_date

                logger.warning(f"未找到 {date_str} 之前的上一个交易日")
                return None
        except Exception as e:
            logger.error(f"查询上一个交易日失败: {e}")
            return None

    def get_month_trade_dates(self, year: int, month: int) -> List[str]:
        """
        获取指定年月的所有交易日

        Args:
            year: 年份
            month: 月份

        Returns:
            list: 该月所有交易日列表
        """
        try:
            with get_session_ro() as db:
                rows = db.query(TradeCalendar).filter(
                    TradeCalendar.year == year,
                    TradeCalendar.month == month,
                    TradeCalendar.is_trading_day == 1
                ).order_by(TradeCalendar.calendar_date.asc()).all()

                return [row.calendar_date for row in rows]
        except Exception as e:
            logger.error(f"查询{year}年{month}月交易日失败: {e}")
            return []

    def is_trading_day(self, date_str: str) -> bool:
        """
        判断指定日期是否为交易日

        Args:
            date_str: 日期字符串，格式为 'YYYY-MM-DD'

        Returns:
            bool: 是否为交易日
        """
        if date_str in self._cached_trade_dates:
            return True

        try:
            with get_session_ro() as db:
                row = db.query(TradeCalendar).filter(
                    TradeCalendar.calendar_date == date_str
                ).first()

                return row and row.is_trading_day == 1
        except Exception as e:
            logger.error(f"判断交易日失败: {e}")
            return False
