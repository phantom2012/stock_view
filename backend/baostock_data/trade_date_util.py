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
        # 缓存最新交易日
        self.latest_trade_date = None
        # 缓存最近的交易日列表（至少20天）
        self.recent_trade_dates_cache = []
        # 缓存本月的交易日列表
        self.current_month_trade_dates_cache = []
        # 缓存的月份标识，用于判断是否需要重新查询本月交易日
        self.cache_month = None
    
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
    
    def get_latest_trade_date(self):
        """
        获取最近一个交易日的日期（带时间过滤）
        
        逻辑：
        - 查询最近3天的交易日历
        - 如果当前北京时间 <= 15:30，则门槛日期为昨日
        - 如果当前北京时间 > 15:30，则门槛日期为今日
        - 返回 <= 门槛日期的最新一个交易日
        
        Returns:
            str: 最近一个交易日的日期，格式为 'YYYY-MM-DD'，如果查询失败返回 None
        """
        # 如果已经缓存了最新交易日，直接返回
        if self.latest_trade_date is not None:
            return self.latest_trade_date
        
        if not self._login():
            return None
        
        try:
            # 获取当前北京时间
            import pytz
            beijing_tz = pytz.timezone('Asia/Shanghai')
            now_beijing = datetime.now(beijing_tz)
            
            # 判断当前时间是否大于15:30
            cutoff_time = now_beijing.replace(hour=15, minute=30, second=0, microsecond=0)
            
            # 确定门槛日期
            if now_beijing > cutoff_time:
                # 当前时间 > 15:30，门槛日期为今日
                threshold_date = now_beijing.strftime("%Y-%m-%d")
                print(f"当前时间 {now_beijing.strftime('%H:%M')} > 15:30，门槛日期为今日: {threshold_date}")
            else:
                # 当前时间 <= 15:30，门槛日期为昨日
                yesterday = now_beijing - timedelta(days=1)
                threshold_date = yesterday.strftime("%Y-%m-%d")
                print(f"当前时间 {now_beijing.strftime('%H:%M')} <= 15:30，门槛日期为昨日: {threshold_date}")
            
            # 查询最近3天的交易日历（足够覆盖）
            today = now_beijing
            start_date = (today - timedelta(days=3)).strftime("%Y-%m-%d")
            end_date = (today + timedelta(days=1)).strftime("%Y-%m-%d")
            
            print(f"查询日期范围: {start_date} 至 {end_date}")
            
            # 查询交易日历
            df = bs.query_trade_dates(start_date=start_date, end_date=end_date)
            data = df.get_data()
            
            # 筛选出是交易日的日期
            trade_dates = data[data['is_trading_day'] == '1']['calendar_date'].tolist()
            
            # 过滤：只保留 <= 门槛日期的交易日
            filtered_dates = [date for date in trade_dates if date <= threshold_date]
            
            # 排序并获取最后一个（最近的交易日）
            filtered_dates.sort()
            
            if filtered_dates:
                latest_trade_date = filtered_dates[-1]
                print(f"最近一个交易日: {latest_trade_date}")
                # 缓存最新交易日
                self.latest_trade_date = latest_trade_date
                return latest_trade_date
            else:
                print("未找到符合条件的交易日")
                return None
        finally:
            self._logout()
    
    def get_recent_trade_dates(self, days=5):
        """
        获取最近N个交易日的日期
        
        Args:
            days (int): 需要获取的交易日数量，默认为5
            
        Returns:
            list: 最近N个交易日的日期列表，格式为 ['YYYY-MM-DD', ...]，如果查询失败返回空列表
        """
        # 检查缓存是否足够
        if len(self.recent_trade_dates_cache) >= days:
            print(f"使用缓存的交易日数据，缓存数量: {len(self.recent_trade_dates_cache)}")
            return self.recent_trade_dates_cache[-days:]
        
        # 缓存不足，需要查询更多数据
        if not self._login():
            return []
        
        try:
            # 获取今天和过去足够多的天的日期（覆盖可能的节假日）
            # 统一查询至少20天的交易日数据
            min_days_to_query = max(days, 20)
            # 考虑到周末和节假日，需要往前推算更多的天数
            lookback_days = int(min_days_to_query * 1.5) + 10
            
            import pytz
            beijing_tz = pytz.timezone('Asia/Shanghai')
            now_beijing = datetime.now(beijing_tz)
            
            start_date = (now_beijing - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
            end_date = (now_beijing + timedelta(days=1)).strftime("%Y-%m-%d")
            
            print(f"查询日期范围: {start_date} 至 {end_date}")
            
            # 查询交易日历
            df = bs.query_trade_dates(start_date=start_date, end_date=end_date)
            data = df.get_data()
            
            # 筛选出是交易日的日期
            trade_dates = data[data['is_trading_day'] == '1']['calendar_date'].tolist()
            
            # 排序
            trade_dates.sort()
            
            # 更新缓存
            self.recent_trade_dates_cache = trade_dates
            print(f"更新缓存，缓存交易日数量: {len(self.recent_trade_dates_cache)}")
            
            # 返回最近的N个交易日
            if len(trade_dates) >= days:
                return trade_dates[-days:]
            else:
                # 如果交易日不足N天，返回所有找到的交易日
                print(f"警告: 只找到 {len(trade_dates)} 个交易日，不足 {days} 个")
                return trade_dates
        finally:
            self._logout()
    
    def get_filtered_trade_dates(self, days=5):
        """
        获取最近N个交易日的日期，并根据当前时间过滤
        
        逻辑：
        - 如果当前北京时间 <= 15:30，则门槛日期为昨日
        - 如果当前北京时间 > 15:30，则门槛日期为今日
        - 剔除门槛日期后面的日期，只保留 <= 门槛日期的交易日
        
        Args:
            days (int): 需要获取的交易日数量，默认为5
            
        Returns:
            list: 过滤后的最近N个交易日的日期列表，格式为 ['YYYY-MM-DD', ...]，如果查询失败返回空列表
        """
        # 首先获取足够的交易日数据
        all_trade_dates = self.get_recent_trade_dates(max(days, 20))
        
        # 获取当前北京时间
        import pytz
        beijing_tz = pytz.timezone('Asia/Shanghai')
        now_beijing = datetime.now(beijing_tz)
        
        # 判断当前时间是否大于15:30
        cutoff_time = now_beijing.replace(hour=15, minute=30, second=0, microsecond=0)
        
        # 确定门槛日期
        if now_beijing > cutoff_time:
            # 当前时间 > 15:30，门槛日期为今日
            threshold_date = now_beijing.strftime("%Y-%m-%d")
            print(f"当前时间 {now_beijing.strftime('%H:%M')} > 15:30，门槛日期为今日: {threshold_date}")
        else:
            # 当前时间 <= 15:30，门槛日期为昨日
            yesterday = now_beijing - timedelta(days=1)
            threshold_date = yesterday.strftime("%Y-%m-%d")
            print(f"当前时间 {now_beijing.strftime('%H:%M')} <= 15:30，门槛日期为昨日: {threshold_date}")
        
        # 过滤：只保留 <= 门槛日期的交易日
        filtered_dates = [date for date in all_trade_dates if date <= threshold_date]
        
        print(f"原始交易日数量: {len(all_trade_dates)}，过滤后数量: {len(filtered_dates)}")
        # print(f"过滤后的交易日: {filtered_dates[-10:] if len(filtered_dates) > 10 else filtered_dates}")
        
        # 返回最近的N个过滤后的交易日
        if len(filtered_dates) >= days:
            return filtered_dates[-days:]
        else:
            # 如果过滤后的交易日不足N天，返回所有找到的
            print(f"警告: 过滤后只找到 {len(filtered_dates)} 个交易日，不足 {days} 个")
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
        # 获取当前月份
        import pytz
        beijing_tz = pytz.timezone('Asia/Shanghai')
        now_beijing = datetime.now(beijing_tz)
        current_month = now_beijing.strftime("%Y-%m")
        
        # 检查缓存是否有效（当月缓存）
        if self.cache_month == current_month and self.current_month_trade_dates_cache:
            print("使用缓存的本月交易日数据")
            # 直接使用缓存数据，然后进行时间过滤
            # 确定门槛日期
            cutoff_time = now_beijing.replace(hour=15, minute=30, second=0, microsecond=0)
            if now_beijing > cutoff_time:
                threshold_date = now_beijing.strftime("%Y-%m-%d")
            else:
                yesterday = now_beijing - timedelta(days=1)
                threshold_date = yesterday.strftime("%Y-%m-%d")
            
            # 过滤：只保留 <= 门槛日期的交易日
            filtered_dates = [date for date in self.current_month_trade_dates_cache if date <= threshold_date]
            return filtered_dates
        
        # 缓存无效，需要重新查询
        if not self._login():
            return []
        
        try:
            # 计算本月的第一天和最后一天
            first_day_of_month = now_beijing.replace(day=1)
            # 计算下个月的第一天
            if now_beijing.month == 12:
                first_day_of_next_month = first_day_of_month.replace(year=now_beijing.year + 1, month=1)
            else:
                first_day_of_next_month = first_day_of_month.replace(month=now_beijing.month + 1)
            # 本月的最后一天是下个月第一天减一天
            last_day_of_month = first_day_of_next_month - timedelta(days=1)
            
            start_date = first_day_of_month.strftime("%Y-%m-%d")
            end_date = last_day_of_month.strftime("%Y-%m-%d")
            
            print(f"查询本月交易日: {start_date} 至 {end_date}")
            
            # 查询交易日历
            df = bs.query_trade_dates(start_date=start_date, end_date=end_date)
            data = df.get_data()
            
            # 筛选出是交易日的日期
            trade_dates = data[data['is_trading_day'] == '1']['calendar_date'].tolist()
            
            # 排序
            trade_dates.sort()
            
            # 更新缓存
            self.current_month_trade_dates_cache = trade_dates
            self.cache_month = current_month
            print(f"更新本月交易日缓存，数量: {len(self.current_month_trade_dates_cache)}")
            
            # 确定门槛日期
            cutoff_time = now_beijing.replace(hour=15, minute=30, second=0, microsecond=0)
            if now_beijing > cutoff_time:
                threshold_date = now_beijing.strftime("%Y-%m-%d")
                print(f"当前时间 {now_beijing.strftime('%H:%M')} > 15:30，门槛日期为今日: {threshold_date}")
            else:
                yesterday = now_beijing - timedelta(days=1)
                threshold_date = yesterday.strftime("%Y-%m-%d")
                print(f"当前时间 {now_beijing.strftime('%H:%M')} <= 15:30，门槛日期为昨日: {threshold_date}")
            
            # 过滤：只保留 <= 门槛日期的交易日
            filtered_dates = [date for date in trade_dates if date <= threshold_date]
            
            print(f"本月原始交易日数量: {len(trade_dates)}，过滤后数量: {len(filtered_dates)}")
            print(f"本月交易日: {filtered_dates}")
            
            return filtered_dates
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
        if not self._login():
            return None
        
        try:
            # 从指定日期的后一天开始查询，往后查10天（足够覆盖周末和节假日）
            start_date = (trade_date + timedelta(days=1)).strftime("%Y-%m-%d")
            end_date = (trade_date + timedelta(days=10)).strftime("%Y-%m-%d")
            
            print(f"查询下一个交易日，范围: {start_date} 至 {end_date}")
            
            # 查询交易日历
            df = bs.query_trade_dates(start_date=start_date, end_date=end_date)
            data = df.get_data()
            
            # 筛选出是交易日的日期
            trade_dates = data[data['is_trading_day'] == '1']['calendar_date'].tolist()
            
            # 排序并获取第一个（最近的下一个交易日）
            trade_dates.sort()
            
            if trade_dates:
                next_trade_date = trade_dates[0]
                print(f"下一个交易日: {next_trade_date}")
                return next_trade_date
            else:
                print("未找到下一个交易日")
                return None
        finally:
            self._logout()


# 测试代码
if __name__ == "__main__":
    # 创建实例
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
    print("测试获取过滤后的最近10个交易日:")
    filtered_dates_10 = trade_date_util.get_filtered_trade_dates(10)
    print(f"过滤后的最近10个交易日: {filtered_dates_10}")
    
    print("\n" + "=" * 50)
    print("测试获取本月所有交易日:")
    month_dates = trade_date_util.get_current_month_trade_dates()
    print(f"本月所有交易日: {month_dates}")
    
    print("\n" + "=" * 50)
    print("测试缓存效果 - 再次获取最近5个交易日:")
    recent_dates_5 = trade_date_util.get_recent_trade_dates(5)
    print(f"最近5个交易日: {recent_dates_5}")
    
    print("\n" + "=" * 50)
    print("测试缓存效果 - 再次获取本月所有交易日:")
    month_dates_again = trade_date_util.get_current_month_trade_dates()
    print(f"本月所有交易日: {month_dates_again}")
