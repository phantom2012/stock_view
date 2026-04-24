import sqlite3

conn = sqlite3.connect(r'F:\gupiao\_sqlite_stock_data\stock.db')
cursor = conn.cursor()

cursor.execute("SELECT COUNT(*) FROM block_stock WHERE stock_code='002834'")
count = cursor.fetchone()[0]
print(f'block_stock中002834记录数: {count}')

if count > 0:
    cursor.execute("SELECT block_name FROM block_info bi JOIN block_stock bs ON bi.block_code=bs.block_code WHERE bs.stock_code='002834'")
    blocks = cursor.fetchall()
    print(f'所属板块:')
    for b in blocks:
        print(f'  - {b[0]}')

conn.close()
