"""验证 filter_results 表结构是否包含 type 字段"""
import sys
from pathlib import Path

# 添加 backend 目录到路径
backend_dir = Path(__file__).parent.parent
sys.path.insert(0, str(backend_dir))

from stock_sqlite.database import get_db_connection

conn = get_db_connection()
cursor = conn.cursor()

print('filter_results表结构:')
cursor.execute('PRAGMA table_info(filter_results)')
for col in cursor.fetchall():
    print(f'  {col}')

cursor.execute('SELECT COUNT(*) FROM filter_results')
print(f'\n总记录数: {cursor.fetchone()[0]}')

cursor.execute('SELECT DISTINCT type FROM filter_results')
types = cursor.fetchall()
print(f'\n不同的type值: {[t[0] for t in types]}')

conn.close()
