"""
测试板块数据查询
"""
import sys
import os

# 添加backend目录到Python路径
backend_dir = r"F:\gupiao\stock_view\backend"
sys.path.insert(0, backend_dir)

from stock_sqlite.database import get_db_connection


def test_block_info():
    """测试板块信息查询"""
    print("=" * 60)
    print("测试1: 查询板块信息表")
    print("=" * 60)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # 查询板块总数
        cursor.execute("SELECT COUNT(*) FROM block_info")
        count = cursor.fetchone()[0]
        print(f"\n板块总数: {count}")
        
        # 查询前10个板块
        cursor.execute("SELECT block_code, block_name FROM block_info LIMIT 10")
        rows = cursor.fetchall()
        print("\n前10个板块:")
        for row in rows:
            print(f"  {row[0]} - {row[1]}")
        
    finally:
        cursor.close()
        conn.close()


def test_stock_block():
    """测试股票板块关系查询"""
    print("\n" + "=" * 60)
    print("测试2: 查询股票板块关系表")
    print("=" * 60)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # 查询关系总数
        cursor.execute("SELECT COUNT(*) FROM stock_block")
        count = cursor.fetchone()[0]
        print(f"\n股票-板块关系总数: {count}")
        
        # 查询某个股票的板块
        test_code = "600519"  # 贵州茅台
        cursor.execute(
            "SELECT sb.block_code, bi.block_name FROM stock_block sb JOIN block_info bi ON sb.block_code = bi.block_code WHERE sb.code = ? LIMIT 5",
            (test_code,)
        )
        rows = cursor.fetchall()
        print(f"\n股票 {test_code} 的板块（前5个）:")
        for row in rows:
            print(f"  {row[0]} - {row[1]}")
        
        # 查询某个板块的股票数量
        test_block = "880670"  # 光通信
        cursor.execute(
            "SELECT COUNT(*) FROM stock_block WHERE block_code = ?",
            (test_block,)
        )
        stock_count = cursor.fetchone()[0]
        print(f"\n板块 {test_block} 的股票数量: {stock_count}")
        
        # 查询该板块的前5只股票
        cursor.execute(
            "SELECT code FROM stock_block WHERE block_code = ? LIMIT 5",
            (test_block,)
        )
        stocks = cursor.fetchall()
        print(f"板块 {test_block} 的前5只股票:")
        for stock in stocks:
            print(f"  {stock[0]}")
        
    finally:
        cursor.close()
        conn.close()


def test_query_by_blocks():
    """测试根据多个板块查询股票"""
    print("\n" + "=" * 60)
    print("测试3: 根据多个板块查询股票")
    print("=" * 60)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # 选择几个板块
        block_codes = ["880670", "880656", "880672"]  # 光通信、CPO概念、存储芯片
        
        placeholders = ','.join(['?' for _ in block_codes])
        query = f"SELECT DISTINCT code FROM stock_block WHERE block_code IN ({placeholders})"
        cursor.execute(query, block_codes)
        rows = cursor.fetchall()
        
        stock_codes = [row[0] for row in rows]
        print(f"\n选中板块: {', '.join(block_codes)}")
        print(f"符合条件的股票总数: {len(stock_codes)}")
        print(f"前10只股票: {', '.join(stock_codes[:10])}")
        
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    test_block_info()
    test_stock_block()
    test_query_by_blocks()
    print("\n" + "=" * 60)
    print("所有测试完成！")
    print("=" * 60)
