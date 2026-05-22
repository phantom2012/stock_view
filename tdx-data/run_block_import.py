"""
通达信板块数据导入统一入口
支持分步执行或一次性执行所有步骤
"""
import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
base_info_dir = os.path.join(current_dir, 'base_info')
output_dir = os.path.join(current_dir, 'output')

sys.path.insert(0, base_info_dir)

from parse_block_data import parse_block_data
from save_block_to_db import save_block_data_to_db


def run_step1():
    """
    第一步：解析通达信板块数据并输出到本地文件
    """
    print("\n" + "=" * 60)
    print("第一步：解析通达信板块数据并输出到本地文件")
    print("=" * 60)

    tdx_config_path = r"F:\soft\new_tdx64\T0002\hq_cache\tdxzs.cfg"
    tdx_block_data_path = r"F:\soft\new_tdx64\T0002\hq_cache\infoharbor_block.dat"
    name_mapping_file = os.path.join(output_dir, 'block_name_mapping.json')

    block_code_map, block_mapping_stocks = parse_block_data(
        tdx_config_path,
        tdx_block_data_path,
        output_dir,
        name_mapping_file
    )

    print("\n" + "=" * 60)
    print("第一步完成！")
    print(f"  板块数量: {len(block_code_map)}")
    print(f"  匹配后的板块股票映射数量: {len(block_mapping_stocks)}")
    print(f"  输出目录: {output_dir}")
    print("=" * 60)

    return block_code_map, block_mapping_stocks


def run_step2():
    """
    第二步：将本地板块数据文件存入数据库
    """
    print("\n" + "=" * 60)
    print("第二步：将本地板块数据文件存入数据库")
    print("=" * 60)

    block_count, relation_count, skip_count = save_block_data_to_db(output_dir)

    print("\n" + "=" * 60)
    print("第二步完成！统计信息:")
    print(f"  板块总数: {block_count}")
    print(f"  股票-板块关系数: {relation_count}")
    print(f"  跳过记录数: {skip_count}")
    print("=" * 60)

    return block_count, relation_count, skip_count


def run_all():
    """
    执行所有步骤：解析数据 -> 存入数据库
    """
    print("=" * 60)
    print("通达信板块数据导入 - 完整流程")
    print("=" * 60)

    run_step1()
    run_step2()

    print("\n" + "=" * 60)
    print("所有步骤完成！")
    print("=" * 60)

#  python .\run_block_import.py --step 1
#  python .\run_block_import.py --step 2
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='通达信板块数据导入工具')
    parser.add_argument(
        '--step',
        type=int,
        choices=[1, 2],
        help='执行指定步骤：1-解析数据到本地文件，2-本地文件存入数据库'
    )

    args = parser.parse_args()

    if args.step == 1:
        run_step1()
    elif args.step == 2:
        run_step2()
    else:
        run_all()
