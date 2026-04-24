import sqlite3
import sys
sys.path.append('..')

from datetime import datetime
from stock_filter import StockFilter

# 检查002834是否在数据库中
conn = sqlite3.connect(r'F:\gupiao\_sqlite_stock_data\stock.db')
cursor = conn.cursor()

# 1. 检查002834所属板块
cursor.execute("""
    SELECT bi.block_code, bi.block_name 
    FROM block_stock bs 
    JOIN block_info bi ON bs.block_code = bi.block_code 
    WHERE bs.stock_code = '002834'
""")
blocks = cursor.fetchall()
print('002834所属板块:')
for block in blocks:
    print(f'  {block[0]} - {block[1]}')

# 2. 检查002834是否符合筛选条件
print('\n检查002834筛选条件:')
sf = StockFilter()
trade_date = datetime(2026, 4, 22)

# 检查性能条件
performance_ok, max_gain, max_daily_gain = sf.check_performance(
    'SZSE.002834', 
    trade_date, 
    recent_interval_days=40, 
    recent_interval_max_gain=60, 
    day_max_gain_days=6,
    day_max_gain=8
)
print(f'  性能检查: {performance_ok}')
print(f'    区间最大涨幅: {max_gain}% (需要>60%)')
print(f'    日内最大涨幅: {max_daily_gain}% (需要>8%)')

# 检查竞价条件
auction_data = sf.check_auction_condition('SZSE.002834', trade_date)
print(f'  竞价数据: {auction_data}')

# 检查升浪形态
rising_wave_score = sf.calculate_rising_wave_score('SZSE.002834', trade_date, 6)
print(f'  升浪形态得分: {rising_wave_score}')

# 检查股票类型
stock_type_ok = sf.check_stock_type('SZSE.002834')
print(f'  股票类型检查: {stock_type_ok}')

conn.close()
