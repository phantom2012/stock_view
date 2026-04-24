#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试升浪形态得分计算 - 603256
"""

from datetime import datetime
from stock_filter import StockFilter
from stock_data import get_stock_cache

# 初始化API
from gm.api import set_token
set_token('2e664976b46df6a0903672349c30226ac68e7bf3')

# 初始化缓存
cache = get_stock_cache()
cache.set_api_token('2e664976b46df6a0903672349c30226ac68e7bf3')

# 初始化过滤器
filter = StockFilter()

# 测试股票
symbol = "SHSE.603256"  # 宏达股份
trade_date = datetime(2026, 4, 17)
days = 6  # 使用 day_max_gain_days 的值

print(f"=== 测试股票 {symbol} 的升浪形态得分 ===")
print(f"交易日期: {trade_date.strftime('%Y-%m-%d')}")
print(f"计算天数: {days}天")

# 获取日K线数据
data = cache.fetch_daily_data(symbol, trade_date, days)

if data is not None and not data.empty:
    print(f"\n获取到 {len(data)} 条数据")
    print("\n数据详情:")
    print(data[['eob', 'close', 'high', 'low', 'open']])

    # 计算升浪形态得分
    score = filter.calculate_rising_wave_score(symbol, trade_date, days)
    print(f"\n升浪形态得分: {score}")

    # 详细分析升浪形态
    if len(data) >= days:
        # 找到最低收盘价的索引
        min_close_loc = data['close'].idxmin()
        start_idx = min_close_loc
        print(f"\n最低点索引: {start_idx}")
        print(f"最低点价格: {data.iloc[start_idx]['close']}")
        print(f"最低点日期: {str(data.iloc[start_idx]['eob'])[:10]}")

        if start_idx >= len(data) - 1:
            print("最低价就是最后一天，无法计算升浪形态")
        else:
            # 从最低点的下一个交易日开始
            current_high = data.iloc[start_idx]['close']
            max_days_to_break = 0
            current_streak = 1  # 开始计数为1，因为当天也算一天

            print("\n升浪形态分析:")
            for i in range(start_idx + 1, len(data)):
                current_close = data.iloc[i]['close']
                current_date = str(data.iloc[i]['eob'])[:10]

                print(f"\n第{i}天 ({current_date}):")
                print(f"  收盘价: {current_close}")
                print(f"  当前最高: {current_high}")
                print(f"  当前连续天数: {current_streak}")

                if current_close >= current_high:
                    # 突破前高，记录当前突破所需天数
                    print(f"  突破前高！")
                    current_high = current_close
                    if current_streak > max_days_to_break:
                        max_days_to_break = current_streak
                        print(f"  新的最大突破天数: {max_days_to_break}")
                    # 重置计数，为下一次突破做准备
                    current_streak = 1  # 当天突破，记为1天
                else:
                    # 未突破，记录天数
                    current_streak += 1
                    print(f"  未突破，连续天数增加到: {current_streak}")

            # 检查最后一段未突破的天数（+1表示最好情况）
            if current_streak > max_days_to_break:
                max_days_to_break = current_streak + 1
                print(f"\n最后一段未突破，调整最大突破天数: {max_days_to_break}")

            print(f"\n最终最大突破天数: {max_days_to_break}")

            # 根据突破情况打分
            if max_days_to_break == 1:
                print("1天就突破前高（每天都突破），得分: 100")
            elif max_days_to_break == 2:
                print("2天突破，得分: 80")
            elif max_days_to_break == 3:
                print("3天突破，得分: 50")
            elif max_days_to_break == 4:
                print("4天突破，得分: 20")
            else:
                print(f"超过4天未突破，得分: 0")
else:
    print("数据获取失败")
