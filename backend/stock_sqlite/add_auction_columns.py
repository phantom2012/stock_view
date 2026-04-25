"""
为 stock_auction 表添加新字段
不删除任何数据，只添加字段
"""
import sqlite3

DATABASE_PATH = r"F:\gupiao\_sqlite_stock_data\stock.db"

def add_columns():
    """为 stock_auction 表添加新字段"""
    print(f"[INFO] 连接到数据库: {DATABASE_PATH}")
    
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        # 1. 添加 close_price 字段
        try:
            cursor.execute("ALTER TABLE stock_auction ADD COLUMN close_price REAL")
            print("[SUCCESS] 已添加 close_price 字段")
        except sqlite3.OperationalError as e:
            if "duplicate column name" in str(e):
                print("[INFO] close_price 字段已存在，跳过")
            else:
                raise
        
        # 2. 添加 tail_amount 字段
        try:
            cursor.execute("ALTER TABLE stock_auction ADD COLUMN tail_amount REAL")
            print("[SUCCESS] 已添加 tail_amount 字段")
        except sqlite3.OperationalError as e:
            if "duplicate column name" in str(e):
                print("[INFO] tail_amount 字段已存在，跳过")
            else:
                raise
        
        # 3. 添加 avg_5d_price 字段
        try:
            cursor.execute("ALTER TABLE stock_auction ADD COLUMN avg_5d_price REAL")
            print("[SUCCESS] 已添加 avg_5d_price 字段")
        except sqlite3.OperationalError as e:
            if "duplicate column name" in str(e):
                print("[INFO] avg_5d_price 字段已存在，跳过")
            else:
                raise
        
        # 4. 添加 avg_10d_price 字段
        try:
            cursor.execute("ALTER TABLE stock_auction ADD COLUMN avg_10d_price REAL")
            print("[SUCCESS] 已添加 avg_10d_price 字段")
        except sqlite3.OperationalError as e:
            if "duplicate column name" in str(e):
                print("[INFO] avg_10d_price 字段已存在，跳过")
            else:
                raise
        
        # 提交更改
        conn.commit()
        
        # 验证字段是否添加成功
        cursor.execute("PRAGMA table_info(stock_auction)")
        columns = cursor.fetchall()
        print("\n[INFO] stock_auction 表当前字段:")
        for col in columns:
            print(f"  - {col[1]} ({col[2]})")
        
        # 查询数据行数
        cursor.execute("SELECT COUNT(*) FROM stock_auction")
        count = cursor.fetchone()[0]
        print(f"\n[INFO] stock_auction 表现有 {count} 条数据")
        
        conn.close()
        print("\n[SUCCESS] 字段添加完成！")
        
    except Exception as e:
        print(f"\n[ERROR] 添加字段失败: {e}")
        import traceback
        traceback.print_exc()
        conn.close()

if __name__ == "__main__":
    add_columns()
