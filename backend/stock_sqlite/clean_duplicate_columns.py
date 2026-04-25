"""
清理 stock_auction 表中的重复字段
保留 avg_price_5d 和 avg_price_10d，删除 avg_5d_price 和 avg_10d_price
"""
import sqlite3

DATABASE_PATH = r"F:\gupiao\_sqlite_stock_data\stock.db"

def clean_duplicate_columns():
    """清理重复的均价字段"""
    print(f"[INFO] 连接到数据库: {DATABASE_PATH}")
    
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        # 获取当前所有字段
        cursor.execute("PRAGMA table_info(stock_auction)")
        columns = [col[1] for col in cursor.fetchall()]
        
        print(f"\n[INFO] 当前字段列表:")
        for col in columns:
            print(f"  - {col}")
        
        # SQLite 不支持直接删除列，需要重建表
        # 检查是否有需要删除的字段
        cols_to_remove = []
        if 'avg_5d_price' in columns:
            cols_to_remove.append('avg_5d_price')
        if 'avg_10d_price' in columns:
            cols_to_remove.append('avg_10d_price')
        
        if not cols_to_remove:
            print("\n[INFO] 没有需要清理的重复字段")
            conn.close()
            return
        
        print(f"\n[WARNING] 发现需要删除的字段: {', '.join(cols_to_remove)}")
        print("[WARNING] SQLite 不支持直接删除列，需要重建表")
        print("[WARNING] 此操作会丢失数据！是否继续？(y/n)")
        
        choice = input().strip().lower()
        if choice != 'y':
            print("[INFO] 操作已取消")
            conn.close()
            return
        
        # 创建新表（不包含要删除的字段）
        print("\n[INFO] 开始重建表...")
        
        # 1. 重命名旧表
        cursor.execute("ALTER TABLE stock_auction RENAME TO stock_auction_old")
        print("[INFO] 已重命名旧表为 stock_auction_old")
        
        # 2. 创建新表
        cursor.execute("""
        CREATE TABLE stock_auction (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            price REAL,
            amount REAL,
            volume INTEGER,
            pre_close REAL,
            turn_over_rate REAL,
            volume_ratio REAL,
            float_share REAL,
            auction_price REAL,
            auction_amount REAL,
            open_price REAL,
            open_amount REAL,
            tail_57_price REAL,
            close_price REAL,
            tail_amount REAL,
            avg_price_5d REAL,
            avg_price_10d REAL,
            update_time TEXT,
            UNIQUE(code, trade_date)
        )
        """)
        print("[INFO] 已创建新表 stock_auction")
        
        # 3. 复制数据（只复制需要的字段）
        select_cols = [col for col in columns if col not in cols_to_remove and col != 'id']
        select_str = ', '.join(select_cols)
        
        cursor.execute(f"""
        INSERT INTO stock_auction ({select_str})
        SELECT {select_str} FROM stock_auction_old
        """)
        
        rows_affected = cursor.rowcount
        print(f"[INFO] 已复制 {rows_affected} 条数据")
        
        # 4. 删除旧表
        cursor.execute("DROP TABLE stock_auction_old")
        print("[INFO] 已删除旧表")
        
        # 5. 提交
        conn.commit()
        
        # 验证
        cursor.execute("PRAGMA table_info(stock_auction)")
        new_columns = cursor.fetchall()
        print(f"\n[SUCCESS] 新表字段列表:")
        for col in new_columns:
            print(f"  - {col[1]} ({col[2]})")
        
        cursor.execute("SELECT COUNT(*) FROM stock_auction")
        count = cursor.fetchone()[0]
        print(f"\n[INFO] stock_auction 表现有 {count} 条数据")
        
        conn.close()
        print("\n[SUCCESS] 字段清理完成！")
        
    except Exception as e:
        print(f"\n[ERROR] 清理失败: {e}")
        import traceback
        traceback.print_exc()
        conn.close()

if __name__ == "__main__":
    clean_duplicate_columns()
