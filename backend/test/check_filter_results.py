import sqlite3

conn = sqlite3.connect(r'F:\gupiao\_sqlite_stock_data\stock.db')
cursor = conn.cursor()

cursor.execute('SELECT * FROM filter_results WHERE type=1 LIMIT 1')
row = cursor.fetchone()

if row:
    cols = [desc[0] for desc in cursor.description]
    print('字段名:', cols)
    print('\n字段值:')
    for i in range(len(cols)):
        print(f'  {cols[i]}: {repr(row[i])}')
else:
    print('没有找到记录')

conn.close()
