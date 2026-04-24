"""检查数据库中的交易日期"""
import sys
from pathlib import Path

backend_dir = Path(__file__).parent.parent
sys.path.insert(0, str(backend_dir))

from stock_sqlite.database import get_db_connection

conn = get_db_connection()
cursor = conn.cursor()

print('stock_daily表中最近10个交易日:')
cursor.execute('SELECT DISTINCT trade_date FROM stock_daily ORDER BY trade_date DESC LIMIT 10')
for row in cursor.fetchall():
    print(f'  {row[0]}')

print('\nblock_stock表中的股票数量:')
cursor.execute('SELECT COUNT(DISTINCT stock_code) FROM block_stock')
print(f'  {cursor.fetchone()[0]} 只股票')

conn.close()
