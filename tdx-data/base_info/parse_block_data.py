#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
第一步：解析通达信板块数据并输出到本地文件
从通达信本地缓存文件解析板块信息和板块股票映射关系
"""

import os
import json


class BlockConfigParser:
    """
    处理本地板块个股配置信息的解析类
    """

    def __init__(self, tdx_config_path, tdx_block_data_path, output_dir, name_mapping_file=None):
        """
        初始化

        Args:
            tdx_config_path: tdxzs.cfg 文件路径
            tdx_block_data_path: infoharbor_block.dat 文件路径
            output_dir: 输出目录路径
            name_mapping_file: 板块名称映射配置文件路径（可选）
        """
        self.tdx_config_path = tdx_config_path
        self.tdx_block_data_path = tdx_block_data_path
        self.output_dir = output_dir
        self.name_mapping_file = name_mapping_file
        self.mapping_file = os.path.join(output_dir, 'tdx_block_mapping.json')
        self.block_names_file = os.path.join(output_dir, 'all_block_names.txt')
        self.mapping_stocks_file = os.path.join(output_dir, 'tdx_block_map_stocks.json')

    def parse_tdxzs_cfg(self):
        """
        解析tdxzs.cfg文件，提取板块代码和名称关系

        Returns:
            dict: 板块代码到名称的映射 {block_code: block_name}
        """
        block_map = {}
        try:
            with open(self.tdx_config_path, 'r', encoding='gbk') as f:
                content = f.read()

            lines = content.split('\n')
            for line in lines:
                line = line.strip()
                if not line:
                    continue

                parts = line.split('|')
                if len(parts) >= 2:
                    block_name = parts[0]
                    block_code = parts[1]
                    block_map[block_code] = block_name
        except Exception as e:
            print(f"[错误] 解析tdxzs.cfg失败: {e}")
        return block_map

    def parse_infoharbor_block_dat(self):
        """
        解析infoharbor_block.dat文件，提取板块和个股映射

        Returns:
            dict: 板块名称到股票列表的映射 {block_name: [stock_codes]}
        """
        block_mapping = {}

        try:
            with open(self.tdx_block_data_path, 'r', encoding='gbk') as f:
                content = f.read()

            lines = content.split('\n')
            current_block_name = None
            current_stocks = []

            for line in lines:
                line = line.strip()
                if not line:
                    continue

                if line.startswith('#GN_'):
                    if current_block_name and current_stocks:
                        block_mapping[current_block_name] = current_stocks

                    parts = line.split(',')
                    if len(parts) >= 2:
                        current_block_name = parts[0][4:]
                    current_stocks = []
                elif line.startswith('0#') or line.startswith('1#'):
                    stock_codes = line.split(',')
                    for code in stock_codes:
                        code = code.strip()
                        if code.startswith('0#'):
                            market = 'sz'
                            pure_code = code[2:].zfill(6)
                            full_code = f"{market}.{pure_code}"
                            current_stocks.append(full_code)
                        elif line.startswith('1#'):
                            market = 'sh'
                            pure_code = code[2:].zfill(6)
                            full_code = f"{market}.{pure_code}"
                            current_stocks.append(full_code)

            if current_block_name and current_stocks:
                block_mapping[current_block_name] = current_stocks

        except Exception as e:
            print(f"[错误] 解析infoharbor_block.dat失败: {e}")

        return block_mapping

    def load_block_name_mapping(self):
        """
        加载板块名称映射配置
        返回: {源板块名: 目标板块名}
        """
        if not self.name_mapping_file or not os.path.exists(self.name_mapping_file):
            return {}

        with open(self.name_mapping_file, 'r', encoding='utf-8') as f:
            mapping = json.load(f)

        return {k: v for k, v in mapping.items() if not k.startswith('_')}

    def match_block_and_generate_mapping(self, block_code_map, block_mapping):
        """
        匹配板块名称并生成新的板块股票映射文件

        Args:
            block_code_map: {block_code: block_name}
            block_mapping: {block_name: [stock_codes]}

        Returns:
            dict: {f"{block_code},{block_name}": [stock_codes]}
        """
        name_to_code = {name: code for code, name in block_code_map.items()}
        name_mapping = self.load_block_name_mapping()

        result = {}
        skip_count = 0
        match_count = 0

        for source_block_name, stock_list in block_mapping.items():
            block_code = name_to_code.get(source_block_name)

            if block_code:
                final_block_name = block_code_map[block_code]
            else:
                mapped_name = name_mapping.get(source_block_name)
                if mapped_name:
                    block_code = name_to_code.get(mapped_name)
                    if block_code:
                        final_block_name = mapped_name
                        print(f"  [配置] '{source_block_name}' -> '{mapped_name}'")
                    else:
                        print(f"  [警告] '{source_block_name}' 配置映射到 '{mapped_name}'，但未找到，跳过")
                        skip_count += 1
                        continue
                else:
                    matched_blocks = [name for name in name_to_code.keys() if source_block_name in name]

                    if len(matched_blocks) == 1:
                        matched_name = matched_blocks[0]
                        block_code = name_to_code[matched_name]
                        final_block_name = matched_name
                        print(f"  [模糊] '{source_block_name}' -> '{matched_name}'")
                    elif len(matched_blocks) > 1:
                        print(f"  [警告] '{source_block_name}' 模糊匹配到多个: {matched_blocks}，跳过")
                        skip_count += 1
                        continue
                    else:
                        print(f"  [警告] 未找到板块 '{source_block_name}'，跳过")
                        skip_count += 1
                        continue

            key = f"{block_code},{final_block_name}"
            result[key] = stock_list
            match_count += 1

        print(f"\n  匹配成功: {match_count} 个板块")
        print(f"  跳过: {skip_count} 个板块")

        return result

    def save_block_info(self):
        """
        保存板块信息到本地文件

        Returns:
            tuple: (block_code_map, block_mapping_stocks) 板块代码映射和匹配后的板块股票映射
        """
        print("\n[步骤1] 解析tdxzs.cfg获取板块代码和名称关系...")
        block_code_map = self.parse_tdxzs_cfg()

        os.makedirs(self.output_dir, exist_ok=True)

        with open(self.block_names_file, 'w', encoding='utf-8') as f:
            for block_code, block_name in block_code_map.items():
                f.write(f"{block_code},{block_name}\n")
        print(f"  板块代码和名称已保存到 {self.block_names_file}")
        print(f"  共 {len(block_code_map)} 个板块")

        print("\n[步骤2] 解析infoharbor_block.dat获取板块个股映射...")
        block_mapping = self.parse_infoharbor_block_dat()

        with open(self.mapping_file, 'w', encoding='utf-8') as f:
            json.dump(block_mapping, f, ensure_ascii=False, indent=2)
        print(f"  板块映射已保存到 {self.mapping_file}")
        print(f"  共 {len(block_mapping)} 个板块")

        print("\n[步骤3] 匹配板块名称并生成板块股票映射文件...")
        block_mapping_stocks = self.match_block_and_generate_mapping(block_code_map, block_mapping)

        with open(self.mapping_stocks_file, 'w', encoding='utf-8') as f:
            json.dump(block_mapping_stocks, f, ensure_ascii=False, indent=2)
        print(f"  匹配后的板块股票映射已保存到 {self.mapping_stocks_file}")

        return block_code_map, block_mapping_stocks

    def load_block_mapping(self):
        """
        加载板块映射

        Returns:
            dict: 板块映射，如果文件不存在返回None
        """
        if os.path.exists(self.mapping_file):
            with open(self.mapping_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return None

    def get_selected_stocks(self, block_list):
        """
        获取指定板块的股票

        Args:
            block_list: 板块列表

        Returns:
            list: 股票列表
        """
        block_mapping = self.load_block_mapping()
        if block_mapping is None:
            print("[错误] 无法加载通达信板块映射文件")
            return []

        selected_stocks = []
        for sector in block_list:
            sector_stocks = block_mapping.get(sector, [])
            selected_stocks.extend(sector_stocks)
            print(f"  {sector}板块: {len(sector_stocks)} 只股票")

        unique_stocks = list(set(selected_stocks))
        print(f"\n  去重后总共: {len(unique_stocks)} 只股票")

        return unique_stocks


def parse_block_data(tdx_config_path, tdx_block_data_path, output_dir, name_mapping_file=None):
    """
    解析通达信板块数据并输出到本地文件

    Args:
        tdx_config_path: tdxzs.cfg 文件路径
        tdx_block_data_path: infoharbor_block.dat 文件路径
        output_dir: 输出目录路径
        name_mapping_file: 板块名称映射配置文件路径（可选）

    Returns:
        tuple: (block_code_map, block_mapping_stocks) 板块代码映射和匹配后的板块股票映射
    """
    parser = BlockConfigParser(tdx_config_path, tdx_block_data_path, output_dir, name_mapping_file)
    return parser.save_block_info()


if __name__ == "__main__":
    tdx_config_path = r"F:\soft\new_tdx64\T0002\hq_cache\tdxzs.cfg"
    tdx_block_data_path = r"F:\soft\new_tdx64\T0002\hq_cache\infoharbor_block.dat"
    output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'output')
    name_mapping_file = os.path.join(output_dir, 'block_name_mapping.json')

    print("=" * 60)
    print("第一步：解析通达信板块数据并输出到本地文件")
    print("=" * 60)

    block_code_map, block_mapping_stocks = parse_block_data(
        tdx_config_path,
        tdx_block_data_path,
        output_dir,
        name_mapping_file
    )

    print("\n" + "=" * 60)
    print("解析完成！")
    print(f"  板块数量: {len(block_code_map)}")
    print(f"  匹配后的板块股票映射数量: {len(block_mapping_stocks)}")
    print("=" * 60)
