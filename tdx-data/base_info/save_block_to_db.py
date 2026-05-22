"""
第二步：将本地板块数据文件存入数据库
从 output 目录读取匹配好的板块股票关系文件，保存到数据库
"""
import json
import os
import sys
from datetime import datetime

backend_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '..', 'backend')
sys.path.insert(0, backend_dir)

from stock_sqlite.database import get_db_connection


def parse_block_names(file_path):
    """
    解析all_block_names.txt文件
    返回: {block_code: block_name}
    """
    blocks = {}
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line and ',' in line:
                parts = line.split(',', 1)
                if len(parts) == 2:
                    code, name = parts
                    blocks[code.strip()] = name.strip()
    return blocks


def parse_block_mapping_stocks(file_path):
    """
    解析tdx_block_map_stocks.json文件
    返回: {f"{block_code},{block_name}": [stock_codes]}
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        mapping = json.load(f)
    return mapping


def save_block_info_to_db(blocks_dict):
    """
    保存板块信息到数据库
    存在则更新时间，不存在则插入新条目
    :param blocks_dict: {block_code: block_name}
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        insert_count = 0
        update_count = 0

        for block_code, block_name in blocks_dict.items():
            cursor.execute("SELECT 1 FROM block_info WHERE block_code = ?", (block_code,))
            exists = cursor.fetchone()

            if exists:
                cursor.execute(
                    "UPDATE block_info SET block_name = ?, update_time = ? WHERE block_code = ?",
                    (block_name, current_time, block_code)
                )
                update_count += 1
            else:
                cursor.execute(
                    "INSERT INTO block_info (block_code, block_name, update_time) VALUES (?, ?, ?)",
                    (block_code, block_name, current_time)
                )
                insert_count += 1

        conn.commit()
        print(f"[成功] 板块信息已保存，新增 {insert_count} 条，更新 {update_count} 条")
        return insert_count + update_count

    except Exception as e:
        conn.rollback()
        print(f"[错误] 保存板块信息失败: {e}")
        raise e
    finally:
        cursor.close()
        conn.close()


def normalize_stock_code(stock_code):
    """
    标准化股票代码，去除交易所前缀
    例如: sz.000025 -> 000025, sh.600018 -> 600018
    """
    if '.' in stock_code:
        return stock_code.split('.')[1]
    return stock_code


def save_stock_block_to_db(block_mapping_stocks):
    """
    保存股票板块关系到数据库
    :param block_mapping_stocks: {f"{block_code},{block_name}": [stock_codes]}
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        cursor.execute("DELETE FROM block_stock")
        cursor.execute("DELETE FROM sqlite_sequence WHERE name='block_stock'")

        insert_count = 0
        skip_count = 0

        for block_key, stock_list in block_mapping_stocks.items():
            parts = block_key.split(',', 1)
            if len(parts) != 2:
                print(f"[警告] 无效的板块键: {block_key}，跳过")
                skip_count += 1
                continue

            block_code, block_name = parts

            for stock_code in stock_list:
                normalized_code = normalize_stock_code(stock_code)

                try:
                    cursor.execute(
                        "INSERT OR IGNORE INTO block_stock (block_code, block_name, stock_code, update_time) VALUES (?, ?, ?, ?)",
                        (block_code, block_name, normalized_code, current_time)
                    )
                    insert_count += 1
                except Exception as e:
                    print(f"[错误] 插入板块 {block_code} - 股票 {normalized_code} 失败: {e}")
                    skip_count += 1

        conn.commit()
        print(f"[成功] 板块股票关系已保存，共 {insert_count} 条记录，跳过 {skip_count} 条")
        return insert_count, skip_count

    except Exception as e:
        conn.rollback()
        print(f"[错误] 保存股票板块关系失败: {e}")
        raise e
    finally:
        cursor.close()
        conn.close()


def save_block_data_to_db(output_dir):
    """
    将本地板块数据文件存入数据库

    Args:
        output_dir: 本地数据文件目录

    Returns:
        tuple: (block_count, relation_count, skip_count) 板块数、关系数、跳过数
    """
    block_names_file = os.path.join(output_dir, 'all_block_names.txt')
    mapping_stocks_file = os.path.join(output_dir, 'tdx_block_map_stocks.json')

    if not os.path.exists(block_names_file):
        print(f"[错误] 文件不存在: {block_names_file}")
        print("[提示] 请先执行第一步生成板块名称文件")
        return 0, 0, 0

    if not os.path.exists(mapping_stocks_file):
        print(f"[错误] 文件不存在: {mapping_stocks_file}")
        print("[提示] 请先执行第一步生成匹配后的板块股票映射文件")
        return 0, 0, 0

    print("\n[步骤1] 解析板块名称文件...")
    blocks_dict = parse_block_names(block_names_file)
    print(f"  解析到 {len(blocks_dict)} 个板块")

    print("\n[步骤2] 保存板块信息到数据库...")
    block_count = save_block_info_to_db(blocks_dict)

    print("\n[步骤3] 解析匹配后的板块股票映射文件...")
    block_mapping_stocks = parse_block_mapping_stocks(mapping_stocks_file)
    print(f"  解析到 {len(block_mapping_stocks)} 个板块的股票列表")

    print("\n[步骤4] 保存股票板块关系到数据库...")
    relation_count, skip_count = save_stock_block_to_db(block_mapping_stocks)

    return block_count, relation_count, skip_count


if __name__ == "__main__":
    output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'output')

    print("=" * 60)
    print("第二步：将本地板块数据文件存入数据库")
    print("=" * 60)

    block_count, relation_count, skip_count = save_block_data_to_db(output_dir)

    print("\n" + "=" * 60)
    print("导入完成！统计信息:")
    print(f"  板块总数: {block_count}")
    print(f"  股票-板块关系数: {relation_count}")
    print(f"  跳过记录数: {skip_count}")
    print("=" * 60)
