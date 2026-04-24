"""验证 block_stock 表结构"""
import sys
from pathlib import Path

# 添加 backend 目录到路径
backend_dir = Path(__file__).parent.parent
sys.path.insert(0, str(backend_dir))

from stock_sqlite.database import get_db_connection

conn = get_db_connection()
cursor = conn.cursor()

cursor.execute('PRAGMA table_info(block_stock)')
print('block_stock表结构:')
for col in cursor.fetchall():
    print(f'  {col}')

cursor.execute('SELECT COUNT(*) FROM block_stock')
print(f'\n总记录数: {cursor.fetchone()[0]}')

cursor.execute('SELECT * FROM block_stock LIMIT 5')
print('\n示例数据:')
for row in cursor.fetchall():
    print(f'  {row}')

# 验证 001309 是否在数据库中
cursor.execute("SELECT * FROM block_stock WHERE stock_code = '001309'")
rows = cursor.fetchall()
print(f'\n001309 所属板块数量: {len(rows)}')
if rows:
    print('前5个板块:')
    for row in rows[:5]:
        print(f'  {row}')

conn.close()
