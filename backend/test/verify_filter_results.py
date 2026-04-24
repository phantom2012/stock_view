"""验证 filter_results 表结构"""
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

cursor.execute('SELECT * FROM filter_results LIMIT 5')
rows = cursor.fetchall()
if rows:
    print('\n示例数据:')
    for row in rows:
        print(f'  {row}')
else:
    print('\n表为空，需要先运行策略筛选')

conn.close()
