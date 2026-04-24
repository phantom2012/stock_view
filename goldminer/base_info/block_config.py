#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
板块配置信息解析类
"""

import os
import json

class BlockConfigParser:
    """
    处理本地板块个股配置信息的解析类
    """
    
    def __init__(self, root_dir):
        """
        初始化
        
        Args:
            root_dir: 根目录路径
        """
        self.root_dir = root_dir
        self.tdx_config_path = r"F:\soft\new_tdx64\T0002\hq_cache\tdxzs.cfg"
        self.tdx_block_data_path = r"F:\soft\new_tdx64\T0002\hq_cache\infoharbor_block.dat"
        self.mapping_file = os.path.join(root_dir, 'tdx_block_mapping.json')
        self.block_names_file = os.path.join(root_dir, 'all_block_names.txt')
    
    def parse_tdxzs_cfg(self):
        """
        解析tdxzs.cfg文件，提取板块代码和名称关系
        
        Returns:
            dict: 板块代码到名称的映射
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
            print(f"解析tdxzs.cfg错误: {e}")
        return block_map
    
    def parse_infoharbor_block_dat(self):
        """
        解析infoharbor_block.dat文件，提取板块和个股映射
        
        Returns:
            dict: 板块名称到股票列表的映射
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
                        elif code.startswith('1#'):
                            market = 'sh'
                            pure_code = code[2:].zfill(6)
                            full_code = f"{market}.{pure_code}"
                            current_stocks.append(full_code)

            if current_block_name and current_stocks:
                block_mapping[current_block_name] = current_stocks

        except Exception as e:
            print(f"解析infoharbor_block.dat错误: {e}")

        return block_mapping
    
    def save_block_info(self):
        """
        保存板块信息到文件
        
        Returns:
            dict: 板块映射
        """
        print("=== 步骤1: 解析tdxzs.cfg获取板块代码和名称关系 ===")
        block_code_map = self.parse_tdxzs_cfg()
        
        # 保存板块代码和名称到all_block_names.txt
        with open(self.block_names_file, 'w', encoding='utf-8') as f:
            for block_code, block_name in block_code_map.items():
                f.write(f"{block_code},{block_name}\n")
        print(f"板块代码和名称已保存到 {self.block_names_file}")
        print(f"共 {len(block_code_map)} 个板块")
        
        print("\n=== 步骤2: 解析infoharbor_block.dat获取板块个股映射 ===")
        block_mapping = self.parse_infoharbor_block_dat()
        
        # 保存板块映射到JSON文件
        with open(self.mapping_file, 'w', encoding='utf-8') as f:
            json.dump(block_mapping, f, ensure_ascii=False, indent=2)
        print(f"板块映射已保存到 {self.mapping_file}")
        print(f"共 {len(block_mapping)} 个板块")
        
        return block_mapping
    
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
            print("无法加载通达信板块映射文件")
            return []
        
        selected_stocks = []
        for sector in block_list:
            sector_stocks = block_mapping.get(sector, [])
            selected_stocks.extend(sector_stocks)
            print(f"{sector}板块: {len(sector_stocks)} 只股票")
        
        # 去重
        unique_stocks = list(set(selected_stocks))
        print(f"\n去重后总共: {len(unique_stocks)} 只股票")
        
        return unique_stocks
    
    def process_block_config(self, block_list):
        """
        处理板块配置
        
        Args:
            block_list: 板块列表
            
        Returns:
            list: 股票列表
        """
        # 保存板块信息
        self.save_block_info()
        
        # 获取选定板块的股票
        return self.get_selected_stocks(block_list)