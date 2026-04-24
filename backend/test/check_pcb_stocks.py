import sqlite3

# 检查数据库中是否有包含2834的股票
conn = sqlite3.connect(r'F:\gupiao\_sqlite_stock_data\stock.db')
cursor = conn.cursor()

# 搜索所有包含2834的股票代码
cursor.execute("SELECT DISTINCT stock_code FROM block_stock WHERE stock_code LIKE '%2834%'")
results = cursor.fetchall()

print('block_stock中包含"2834"的股票:')
if results:
    for r in results:
        print(f'  - {r[0]}')
else:
    print('  未找到')

# 检查PCB概念板块的股票
cursor.execute("""
    SELECT bs.stock_code, bi.block_name 
    FROM block_stock bs 
    JOIN block_info bi ON bs.block_code = bi.block_code 
    WHERE bi.block_name LIKE '%PCB%'
    LIMIT 20
""")
pcb_stocks = cursor.fetchall()

print(f'\nPCB概念板块的前20只股票:')
for stock in pcb_stocks:
    print(f'  - {stock[0]} ({stock[1]})')

conn.close()
