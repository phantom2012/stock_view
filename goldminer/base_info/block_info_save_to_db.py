"""
板块信息导入脚本
从all_block_names.txt和tdx_block_mapping.json读取数据并导入到数据库
"""
import json
import os
import sys
from datetime import datetime

# 添加backend目录到Python路径
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


def parse_block_mapping(file_path):
    """
    解析tdx_block_mapping.json文件
    返回: {block_name: [stock_codes]}
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        mapping = json.load(f)
    return mapping


def load_block_name_mapping(file_path):
    """
    加载板块名称映射配置
    返回: {源板块名: 目标板块名}
    """
    if not os.path.exists(file_path):
        print(f"[提示] 未找到板块名称映射配置文件: {file_path}，将不使用自定义映射")
        return {}
    
    with open(file_path, 'r', encoding='utf-8') as f:
        mapping = json.load(f)
    
    # 过滤掉以_开头的注释键
    return {k: v for k, v in mapping.items() if not k.startswith('_')}


def normalize_stock_code(stock_code):
    """
    标准化股票代码，去除交易所前缀
    例如: sz.000025 -> 000025, sh.600018 -> 600018
    """
    if '.' in stock_code:
        return stock_code.split('.')[1]
    return stock_code


def save_block_info_to_db(blocks_dict):
    """
    保存板块信息到数据库
    :param blocks_dict: {block_code: block_name}
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # 清空旧数据
        cursor.execute("DELETE FROM block_info")
        
        # 插入新数据
        insert_count = 0
        for block_code, block_name in blocks_dict.items():
            cursor.execute(
                "INSERT OR REPLACE INTO block_info (block_code, block_name, update_time) VALUES (?, ?, ?)",
                (block_code, block_name, current_time)
            )
            insert_count += 1
        
        conn.commit()
        print(f"[成功] 板块信息已保存，共 {insert_count} 条记录")
        return insert_count
        
    except Exception as e:
        conn.rollback()
        print(f"[错误] 保存板块信息失败: {e}")
        raise e
    finally:
        cursor.close()
        conn.close()


def save_stock_block_to_db(block_mapping, blocks_dict, name_mapping=None):
    """
    保存股票板块关系到数据库
    :param block_mapping: {block_name: [stock_codes]}
    :param blocks_dict: {block_code: block_name}
    :param name_mapping: {源板块名: 目标板块名} 自定义名称映射
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # 清空旧数据并重置自增ID
        cursor.execute("DELETE FROM block_stock")
        cursor.execute("DELETE FROM sqlite_sequence WHERE name='block_stock'")
        
        # 构建板块名称到代码的映射
        name_to_code = {name: code for code, name in blocks_dict.items()}
        
        # 如果没有提供name_mapping，则使用空字典
        if name_mapping is None:
            name_mapping = {}
        
        # 插入新数据
        insert_count = 0
        skip_count = 0
        
        for block_name, stock_list in block_mapping.items():
            # 获取板块代码
            block_code = name_to_code.get(block_name)
            
            # 如果精确匹配失败，尝试名称映射配置
            if not block_code:
                mapped_name = name_mapping.get(block_name)
                if mapped_name:
                    block_code = name_to_code.get(mapped_name)
                    if block_code:
                        print(f"[配置] 板块 '{block_name}' 通过自定义映射: '{mapped_name}'")
                    else:
                        print(f"[警告] 板块 '{block_name}' 配置映射到 '{mapped_name}'，但在数据库中未找到，跳过")
                        skip_count += 1
                        continue
                else:
                    # 没有配置映射，尝试模糊匹配（包含关系）
                    matched_blocks = [name for name in name_to_code.keys() if block_name in name]
                    
                    if len(matched_blocks) == 1:
                        # 只找到一个匹配的，使用这个
                        matched_name = matched_blocks[0]
                        block_code = name_to_code[matched_name]
                        print(f"[提示] 板块 '{block_name}' 未精确匹配，使用模糊匹配: '{matched_name}'")
                    elif len(matched_blocks) > 1:
                        # 找到多个匹配的，打印并跳过
                        print(f"[警告] 板块 '{block_name}' 模糊匹配到多个结果: {matched_blocks}，请添加名称映射配置")
                        skip_count += 1
                        continue
                    else:
                        # 完全找不到
                        print(f"[警告] 未找到板块 '{block_name}' 的代码，请添加名称映射配置")
                        skip_count += 1
                        continue
            
            for stock_code in stock_list:
                # 标准化股票代码
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


def main():
    """主函数"""
    print("=" * 60)
    print("开始导入板块数据到数据库...")
    print("=" * 60)
    
    # 文件路径 - 使用绝对路径
    goldminer_dir = r"F:\gupiao\stock_view\goldminer"
    block_names_file = os.path.join(goldminer_dir, 'all_block_names.txt')
    block_mapping_file = os.path.join(goldminer_dir, 'tdx_block_mapping.json')
    
    # 检查文件是否存在
    if not os.path.exists(block_names_file):
        print(f"[错误] 文件不存在: {block_names_file}")
        return
    
    if not os.path.exists(block_mapping_file):
        print(f"[错误] 文件不存在: {block_mapping_file}")
        return
    
    try:
        # 1. 解析板块名称文件
        print("\n[步骤1] 解析板块名称文件...")
        blocks_dict = parse_block_names(block_names_file)
        print(f"  解析到 {len(blocks_dict)} 个板块")
        
        # 2. 保存板块信息到数据库
        print("\n[步骤2] 保存板块信息到数据库...")
        block_count = save_block_info_to_db(blocks_dict)
        
        # 3. 解析板块映射文件
        print("\n[步骤3] 解析板块映射文件...")
        block_mapping = parse_block_mapping(block_mapping_file)
        print(f"  解析到 {len(block_mapping)} 个板块的股票列表")
        
        # 4. 加载板块名称映射配置
        print("\n[步骤4] 加载板块名称映射配置...")
        goldminer_dir = r"F:\gupiao\stock_view\goldminer"
        name_mapping_file = os.path.join(goldminer_dir, 'block_name_mapping.json')
        name_mapping = load_block_name_mapping(name_mapping_file)
        print(f"  加载到 {len(name_mapping)} 个自定义名称映射")
        
        # 5. 保存股票板块关系到数据库
        print("\n[步骤5] 保存股票板块关系到数据库...")
        relation_count, skip_count = save_stock_block_to_db(block_mapping, blocks_dict, name_mapping)
        
        # 6. 统计信息
        print("\n" + "=" * 60)
        print("导入完成！统计信息:")
        print(f"  板块总数: {block_count}")
        print(f"  自定义名称映射数: {len(name_mapping)}")
        print(f"  股票-板块关系数: {relation_count}")
        print(f"  跳过记录数: {skip_count}")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n[错误] 导入过程失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
