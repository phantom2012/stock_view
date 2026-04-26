import baostock as bs
import pandas as pd
from datetime import datetime, timedelta


class TradeDateUtil:
    """
    交易日查询工具类
    使用baostock库查询交易日历
    """

    def __init__(self):
        """初始化交易日工具类"""
        # 缓存最新交易日，会在第一次调用get_latest_trade_date强制查询api获取并保存到缓存
        self.latest_trade_date = None

        # 缓存各月份的交易日列表，key格式为 "YYYYMM"，如 "202604"
        self.mouths_trade_days = {}

    def _login(self):
        """登录baostock"""
        lg = bs.login()
        if lg.error_code != '0':
            print(f"登录失败: {lg.error_msg}")
            return False
        return True

    def _logout(self):
        """登出baostock"""
        bs.logout()

    def _query_and_cache_trade_dates(self, start_date: datetime, end_date: datetime):
        """
        查询日期范围内的交易日，并按月分组缓存到 mouths_trade_days

        Args:
            start_date: 查询开始日期
            end_date: 查询结束日期
        """
        if not self._login():
            return

        try:
            start_str = start_date.strftime("%Y-%m-%d")
            end_str = end_date.strftime("%Y-%m-%d")
            print(f"查询日期范围: {start_str} 至 {end_str}")

            df = bs.query_trade_dates(start_date=start_str, end_date=end_str)
            data = df.get_data()

            trade_dates = data[data['is_trading_day'] == '1']['calendar_date'].tolist()
            trade_dates.sort()

            if not trade_dates:
                print("未找到交易日")
                return

            for date in trade_dates:
                month_key = date[:6]
                if month_key not in self.mouths_trade_days:
                    self.mouths_trade_days[month_key] = []
                # 避免重复添加日期
                if date not in self.mouths_trade_days[month_key]:
                    self.mouths_trade_days[month_key].append(date)

            self.latest_trade_date = trade_dates[-1]
            print(f"缓存交易日，月份数: {len(self.mouths_trade_days)}，最新交易日: {self.latest_trade_date}")
        finally:
            self._logout()

    def get_latest_trade_date(self):
        """
        获取最近一个交易日的日期（带时间过滤）

        逻辑：
        - 查询从上个月初到今天的交易日历
        - 遍历交易日列表，按月分组缓存到 mouths_trade_days
        - 如果当前北京时间 <= 15:30，则门槛日期为昨日
        - 如果当前北京时间 > 15:30，则门槛日期为今日
        - 返回 <= 门槛日期的最新一个交易日

        Returns:
            str: 最近一个交易日的日期，格式为 'YYYY-MM-DD'，如果查询失败返回 None
        """
        if self.latest_trade_date is not None:
            return self.latest_trade_date

        import pytz
        beijing_tz = pytz.timezone('Asia/Shanghai')
        now_beijing = datetime.now(beijing_tz)

        cutoff_time = now_beijing.replace(hour=15, minute=30, second=0, microsecond=0)

        if now_beijing > cutoff_time:
            threshold_date = now_beijing.strftime("%Y-%m-%d")
            print(f"当前时间 {now_beijing.strftime('%H:%M')} > 15:30，门槛日期为今日: {threshold_date}")
        else:
            yesterday = now_beijing - timedelta(days=1)
            threshold_date = yesterday.strftime("%Y-%m-%d")
            print(f"当前时间 {now_beijing.strftime('%H:%M')} <= 15:30，门槛日期为昨日: {threshold_date}")

        if now_beijing.month == 1:
            start_date = now_beijing.replace(year=now_beijing.year - 1, month=12, day=1)
        else:
            start_date = now_beijing.replace(month=now_beijing.month - 1, day=1)

        self._query_and_cache_trade_dates(start_date, now_beijing)

        filtered_dates = []
        for month_key in sorted(self.mouths_trade_days.keys()):
            for date in self.mouths_trade_days[month_key]:
                if date <= threshold_date:
                    filtered_dates.append(date)

        if filtered_dates:
            latest_trade_date = filtered_dates[-1]
            print(f"最近一个交易日: {latest_trade_date}")
            self.latest_trade_date = latest_trade_date
            return latest_trade_date
        else:
            print("未找到符合条件的交易日")
            return None

    def _ensure_trade_dates_cache(self):
        """确保 mouths_trade_days 有足够的缓存数据"""
        if not self.mouths_trade_days:
            import pytz
            beijing_tz = pytz.timezone('Asia/Shanghai')
            now_beijing = datetime.now(beijing_tz)
            start_date = now_beijing - timedelta(days=365)
            self._query_and_cache_trade_dates(start_date, now_beijing)

    def _get_all_trade_dates_from_cache(self):
        """从缓存中获取所有交易日并排序，确保无重复"""
        all_dates = []
        seen_dates = set()
        for month_key in sorted(self.mouths_trade_days.keys()):
            for date in self.mouths_trade_days[month_key]:
                if date not in seen_dates:
                    all_dates.append(date)
                    seen_dates.add(date)
        all_dates.sort()
        return all_dates

    def get_recent_trade_dates(self, days=5):
        """
        获取最近N个交易日的日期

        Args:
            days (int): 需要获取的交易日数量，默认为5

        Returns:
            list: 最近N个交易日的日期列表，格式为 ['YYYY-MM-DD', ...]，如果查询失败返回空列表
        """
        import pytz
        beijing_tz = pytz.timezone('Asia/Shanghai')
        now_beijing = datetime.now(beijing_tz)

        cutoff_time = now_beijing.replace(hour=15, minute=30, second=0, microsecond=0)
        if now_beijing > cutoff_time:
            threshold_date = now_beijing
        else:
            threshold_date = now_beijing - timedelta(days=1)

        self._ensure_trade_dates_cache()

        all_dates = self._get_all_trade_dates_from_cache()

        filtered_dates = [date for date in all_dates if date <= threshold_date.strftime("%Y-%m-%d")]

        if len(filtered_dates) >= days:
            return filtered_dates[-days:]
        else:
            print(f"警告: 只找到 {len(filtered_dates)} 个交易日，不足 {days} 个")
            return filtered_dates

    def get_current_month_trade_dates(self):
        """
        获取本月所有交易日的日期列表，并根据当前时间过滤

        逻辑：
        - 如果当前北京时间 <= 15:30，则门槛日期为昨日
        - 如果当前北京时间 > 15:30，则门槛日期为今日
        - 剔除门槛日期后面的日期，只保留 <= 门槛日期的交易日

        Returns:
            list: 本月所有符合条件的交易日列表，格式为 ['YYYY-MM-DD', ...]，如果查询失败返回空列表
        """
        import pytz
        beijing_tz = pytz.timezone('Asia/Shanghai')
        now_beijing = datetime.now(beijing_tz)
        current_month_key = now_beijing.strftime("%Y%m")

        if current_month_key in self.mouths_trade_days:
            print("使用缓存的本月交易日数据")
            trade_dates = self.mouths_trade_days[current_month_key]
        else:
            if not self._login():
                return []

            try:
                first_day_of_month = now_beijing.replace(day=1)
                if now_beijing.month == 12:
                    first_day_of_next_month = first_day_of_month.replace(year=now_beijing.year + 1, month=1)
                else:
                    first_day_of_next_month = first_day_of_month.replace(month=now_beijing.month + 1)
                last_day_of_month = first_day_of_next_month - timedelta(days=1)

                start_date = first_day_of_month.strftime("%Y-%m-%d")
                end_date = last_day_of_month.strftime("%Y-%m-%d")

                print(f"查询本月交易日: {start_date} 至 {end_date}")

                df = bs.query_trade_dates(start_date=start_date, end_date=end_date)
                data = df.get_data()

                trade_dates = data[data['is_trading_day'] == '1']['calendar_date'].tolist()
                trade_dates.sort()

                self.mouths_trade_days[current_month_key] = trade_dates
                print(f"更新本月交易日缓存，数量: {len(trade_dates)}")
            finally:
                self._logout()

        cutoff_time = now_beijing.replace(hour=15, minute=30, second=0, microsecond=0)
        if now_beijing > cutoff_time:
            threshold_date = now_beijing.strftime("%Y-%m-%d")
            print(f"当前时间 {now_beijing.strftime('%H:%M')} > 15:30，门槛日期为今日: {threshold_date}")
        else:
            yesterday = now_beijing - timedelta(days=1)
            threshold_date = yesterday.strftime("%Y-%m-%d")
            print(f"当前时间 {now_beijing.strftime('%H:%M')} <= 15:30，门槛日期为昨日: {threshold_date}")

        filtered_dates = [date for date in trade_dates if date <= threshold_date]

        print(f"本月原始交易日数量: {len(trade_dates)}，过滤后数量: {len(filtered_dates)}")
        print(f"本月交易日: {filtered_dates}")

        return filtered_dates

    def get_month_trade_dates(self, year: int, month: int) -> list:
        """
        获取指定年月的所有交易日

        Args:
            year: 年份
            month: 月份

        Returns:
            list: 该月所有交易日列表，格式为 ['YYYY-MM-DD', ...]，如果查询失败返回空列表
        """
        month_key = f"{year:04d}{month:02d}"

        if month_key in self.mouths_trade_days:
            print(f"使用缓存的{year}年{month}月交易日数据")
            return self.mouths_trade_days[month_key]

        if not self._login():
            return []

        try:
            first_day_of_month = datetime(year, month, 1)
            if month == 12:
                first_day_of_next_month = first_day_of_month.replace(year=year + 1, month=1)
            else:
                first_day_of_next_month = first_day_of_month.replace(month=month + 1)
            last_day_of_month = first_day_of_next_month - timedelta(days=1)

            start_date = first_day_of_month.strftime("%Y-%m-%d")
            end_date = last_day_of_month.strftime("%Y-%m-%d")

            print(f"查询{year}年{month}月交易日: {start_date} 至 {end_date}")

            df = bs.query_trade_dates(start_date=start_date, end_date=end_date)
            data = df.get_data()

            trade_dates = data[data['is_trading_day'] == '1']['calendar_date'].tolist()
            trade_dates.sort()

            self.mouths_trade_days[month_key] = trade_dates
            print(f"更新{year}年{month}月交易日缓存，数量: {len(trade_dates)}")

            return trade_dates
        finally:
            self._logout()

    def get_next_trade_date(self, trade_date: datetime) -> str:
        """
        获取指定日期之后的下一个交易日

        Args:
            trade_date: 指定的日期（datetime对象）

        Returns:
            str: 下一个交易日的日期，格式为 'YYYY-MM-DD'，如果查询失败返回 None
        """
        trade_date_str = trade_date.strftime("%Y-%m-%d")

        current_year = trade_date.year
        current_month = trade_date.month

        current_month_key = f"{current_year:04d}{current_month:02d}"

        if current_month_key not in self.mouths_trade_days:
            current_month_days = self.get_month_trade_dates(current_year, current_month)
        else:
            current_month_days = self.mouths_trade_days[current_month_key]

        for date in current_month_days:
            if date > trade_date_str:
                print(f"在当月找到下一个交易日: {date}")
                return date

        next_year = current_year
        next_month = current_month + 1
        if next_month > 12:
            next_year += 1
            next_month = 1

        next_month_days = self.get_month_trade_dates(next_year, next_month)

        if next_month_days:
            next_trade_date = next_month_days[0]
            print(f"在次月找到下一个交易日: {next_trade_date}")
            return next_trade_date
        else:
            print("未找到下一个交易日")
            return None


if __name__ == "__main__":
    trade_date_util = TradeDateUtil()

    print("=" * 50)
    print("测试获取最近一个交易日:")
    latest = trade_date_util.get_latest_trade_date()
    print(f"最近一个交易日: {latest}")

    print("\n" + "=" * 50)
    print("测试获取最近10个交易日:")
    recent_dates_10 = trade_date_util.get_recent_trade_dates(10)
    print(f"最近10个交易日: {recent_dates_10}")

    print("\n" + "=" * 50)
    print("测试获取本月所有交易日:")
    month_dates = trade_date_util.get_current_month_trade_dates()
    print(f"本月所有交易日: {month_dates}")
