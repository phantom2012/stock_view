"""验证 filter_results 表中的数据"""
import sys
from pathlib import Path

# 添加 backend 目录到路径
backend_dir = Path(__file__).parent.parent
sys.path.insert(0, str(backend_dir))

from stock_sqlite.database import get_db_connection

conn = get_db_connection()
cursor = conn.cursor()

cursor.execute('PRAGMA table_info(filter_results)')
print('filter_results表结构:')
for col in cursor.fetchall():
    print(f'  {col}')

cursor.execute('SELECT COUNT(*) FROM filter_results')
print(f'\n总记录数: {cursor.fetchone()[0]}')

cursor.execute('SELECT symbol, code, stock_name, auction_start_price, auction_end_price, price_diff, max_gain, max_daily_gain, today_gain, next_day_gain, higher_score FROM filter_results LIMIT 3')
rows = cursor.fetchall()
if rows:
    print('\n示例数据 (前3条):')
    for i, row in enumerate(rows, 1):
        print(f'\n记录 {i}:')
        print(f'  symbol: {row[0]}')
        print(f'  code: {row[1]}')
        print(f'  stock_name: {row[2]}')
        print(f'  auction_start_price: {row[3]}')
        print(f'  auction_end_price: {row[4]}')
        print(f'  price_diff: {row[5]}')
        print(f'  max_gain: {row[6]}')
        print(f'  max_daily_gain: {row[7]}')
        print(f'  today_gain: {row[8]}')
        print(f'  next_day_gain: {row[9]}')
        print(f'  higher_score: {row[10]}')
else:
    print('\n表为空')

conn.close()
