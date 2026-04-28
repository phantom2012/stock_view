import sqlite3

# 连接数据库
conn = sqlite3.connect('F:/gupiao/_sqlite_stock_data/stock.db')
cursor = conn.cursor()

# 测试查询
try:
    # 执行简单查询
    cursor.execute('SELECT symbol, code, stock_name FROM filter_results WHERE type = 1 LIMIT 5')
    rows = cursor.fetchall()
    print('Query executed successfully')
    print('Number of rows:', len(rows))
    for row in rows:
        print(row)

    # 执行完整查询
    cursor.execute('''
        SELECT fr.symbol, fr.code, fr.stock_name, fr.pre_avg_price, fr.pre_close_price, fr.pre_price_gain, fr.open_price, fr.close_price, fr.next_close_price,
               fr.open_volume, fr.price_diff, fr.interval_max_rise, fr.max_day_rise,
               fr.trade_date, fr.higher_score, fr.rising_wave_score, fr.volume_ratio,
               COALESCE(sa.volume_ratio, 0) as auction_volume_ratio
        FROM filter_results fr
        LEFT JOIN stock_auction sa ON fr.code = sa.code AND fr.trade_date = sa.trade_date
        WHERE fr.type = 1
        ORDER BY fr.higher_score DESC
    ''')
    rows = cursor.fetchall()
    print('\nFull query executed successfully')
    print('Number of rows:', len(rows))
    if rows:
        print('First row:', rows[0])
except Exception as e:
    print('Error:', e)
finally:
    conn.close()
