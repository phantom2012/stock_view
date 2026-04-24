import sqlite3

# 连接数据库
conn = sqlite3.connect(r'F:\gupiao\_sqlite_stock_data\stock.db')
cursor = conn.cursor()

# 检查表结构
print('=== 数据库表结构 ===')
cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = cursor.fetchall()
for table in tables:
    print(f'表: {table[0]}')
    cursor.execute(f"PRAGMA table_info({table[0]})")
    columns = cursor.fetchall()
    for col in columns:
        print(f'  - {col[1]} ({col[2]})')

# 检查数据
print('\n=== 数据检查 ===')
for table in tables:
    table_name = table[0]
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    print(f'{table_name}: {count} 条记录')

# 检查stock_info表的具体数据
print('\n=== stock_info 数据 ===')
cursor.execute("SELECT * FROM stock_info LIMIT 5")
rows = cursor.fetchall()
for row in rows:
    print(row)

# 检查stock_daily表的具体数据
print('\n=== stock_daily 数据 ===')
cursor.execute("SELECT * FROM stock_daily LIMIT 5")
rows = cursor.fetchall()
for row in rows:
    print(row)

conn.close()