import sqlite3

# 1. 连接数据库，不存在就自动创建 stock.db 文件
conn = sqlite3.connect("F:\gupiao\_sqlite_stock_data/stock.db")

# 2. 开启高性能内存缓存（关键配置，给你拉满速度）
cursor = conn.cursor()
# cache_size 单位KB，这里设 500MB 缓存
cursor.execute("PRAGMA cache_size = -512000;")
# 开启写优化、内存加速
cursor.execute("PRAGMA journal_mode = WAL;")

# 3. 简单建表测试（股票日线表示例）
cursor.execute('''
CREATE TABLE IF NOT EXISTS stock_daily (
    ts_code TEXT,
    trade_date TEXT,
    open REAL,
    close REAL,
    amount REAL,
    PRIMARY KEY (ts_code, trade_date)
)
''')

conn.commit()
conn.close()
print("SQLite 初始化成功，数据库文件已生成！")