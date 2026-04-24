from __future__ import print_function, absolute_import
import time
import pandas as pd
import json
import os
import argparse
from datetime import datetime, timedelta
from gm.api import *
from pandas.core.nanops import F

# 导入板块配置解析类
from base_info.block_config import BlockConfigParser

# 导入股票过滤器
from stock_filter import StockFilter

# 导入缓存池
from stock_data import get_stock_cache


# 要筛选的板块列表
CUR_ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
HIGHER_EXPECTED_STOCKS_CSV = os.path.join(CUR_ROOT_DIR, 'higher_expected_filtered_stocks.csv')
BLOCK_LIST = ['光通信', 'CPO概念', 'PCB概念', '存储芯片', '液冷服务', '东数西算']

#  运行模式
RUN_MODE = 2
# 配置参数
CONFIG = {
    'recent_interval_days': 40,  # 近N个交易日
    'recent_interval_max_gain': 60,  # 区间最大涨幅
    'day_max_gain_days': 6,  # 最近N日日内最大涨幅
    'day_max_gain': 8,  # 单日最大涨幅超过8%
    'api_key': '2e664976b46df6a0903672349c30226ac68e7bf3',
    'trade_date': "2026-04-20",  # 指定交易日期，None表示自动获取上一个交易日，格式：'2026-04-17'
    'debug_stock': None,  # 用于调试的股票代码，设置为股票代码（如'002475'）或完整代码（如'SZSE.002475'）
}

FILTERS = {
    'rising_wave': 1,
    'zaopan_expected': 0,
    'weipan_expected': 0,
}

# 初始化API
set_token(CONFIG['api_key'])

# 初始化缓存池
stock_cache = get_stock_cache()
stock_cache.set_api_token(CONFIG['api_key'])

def save_sector_stocks_to_ini():
    """将板块股票保存到JSON文件，并保存所有板块信息"""
    # 创建板块配置解析器实例
    parser = BlockConfigParser(CUR_ROOT_DIR)
    
    # 处理板块配置
    parser.process_block_config(BLOCK_LIST)


def load_stocks_from_ini():
    """从板块映射加载股票（run_mode=2模式）"""
    # 创建板块配置解析器实例
    parser = BlockConfigParser(CUR_ROOT_DIR)
    
    # 加载板块映射
    block_mapping = parser.load_block_mapping()
    if block_mapping is None:
        print("无法加载通达信板块映射文件")
        return [], {}
    
    # 收集筛选板块的股票
    selected_stocks = []
    stock_names = {}
    
    for sector in BLOCK_LIST:
        sector_stocks = block_mapping.get(sector, [])
        selected_stocks.extend(sector_stocks)
    
    # 去重
    selected_stocks = list(set(selected_stocks))
    
    # 转换为SHSE/SZSE格式
    stock_symbols = []
    for stock_code in selected_stocks:
        if stock_code.startswith('sz.'):
            code = stock_code.split('.')[1]
            symbol = f'SZSE.{code}'
            stock_symbols.append(symbol)
            stock_names[code] = '未知'
        elif stock_code.startswith('sh.'):
            code = stock_code.split('.')[1]
            symbol = f'SHSE.{code}'
            stock_symbols.append(symbol)
            stock_names[code] = '未知'
    
    print(f"\nLoaded {len(stock_symbols)} stocks from block mapping")
    return stock_symbols, stock_names


def filter_auction_stocks_from_ini(weipan_exceed=0, zaopan_exceed=0, rising_wave=0):
    """从INI文件加载股票并筛选符合条件的个股"""
    start_time = time.time()
    
    # 加载股票
    sector_symbols, stock_names = load_stocks_from_ini()
    
    if not sector_symbols:
        return

    # 获取交易日期
    if CONFIG['trade_date']:
        trade_date = datetime.strptime(CONFIG['trade_date'], '%Y-%m-%d')
    else:
        today = datetime.now()
        yesterday = today - timedelta(days=1)
        while yesterday.weekday() >= 5:
            yesterday -= timedelta(days=1)
        trade_date = yesterday

    print(f"\n开始筛选 {len(sector_symbols)} 只股票...")
    print(f"交易日期: {trade_date.strftime('%Y-%m-%d')}")

    # 创建过滤器实例
    filter = StockFilter()
    
    # 筛选股票
    results = filter.filter_stocks(
        sector_symbols, trade_date, 
        weipan_exceed, zaopan_exceed, rising_wave, 
        CONFIG
    )

    if results:
        result_df = pd.DataFrame(results)
        result_df['symbol'] = result_df['symbol'].apply(lambda x: x.split('.')[-1] if isinstance(x, str) and '.' in x else str(x))
        result_df['symbol'] = result_df['symbol'].apply(lambda x: str(x).zfill(6))
        result_df = result_df.rename(columns={'symbol': 'code'})

        # 通过过滤器获取股票名称
        all_symbols = [f"SZSE.{code}" if not code.startswith('6') else f"SHSE.{code}" for code in result_df['code']]
        api_stock_names = filter.get_stock_names(all_symbols)

        # 更新股票名称
        def update_stock_name(code):
            return api_stock_names.get(code, '未知')

        result_df['stock_name'] = result_df['code'].apply(update_stock_name)

        result_df = result_df[['code', 'stock_name', 'auction_start_price', 'auction_end_price', 'price_diff', 'max_gain', 'max_daily_gain', 'today_gain', 'next_day_gain', 'trade_date', 'rising_wave_score', 'higher_score']]

        for col in ['max_gain', 'max_daily_gain', 'today_gain', 'next_day_gain']:
            if col in result_df.columns:
                result_df[col] = result_df[col].apply(lambda x: f'{x:.2f}%' if x is not None and x != '' else 'None')
        
        if 'higher_score' in result_df.columns:
            result_df['higher_score'] = result_df['higher_score'].apply(lambda x: f'{x:.2f}' if x is not None else 'None')
        if 'rising_wave_score' in result_df.columns:
            result_df['rising_wave_score'] = result_df['rising_wave_score'].apply(lambda x: f'{x:.0f}' if x is not None else 'None')

        output_file = HIGHER_EXPECTED_STOCKS_CSV
        result_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"\n筛选完成，共 {len(results)} 只股票符合条件")
        print(f"结果已保存到: {output_file}")
        # 只打印前5只股票
        if len(result_df) > 5:
            print("\n前5只股票:")
            print(result_df.head(5))
        else:
            print("\n所有股票:")
            print(result_df)
    else:
        print("\n没有符合条件的股票")
    
    total_time = time.time() - start_time
    print(f"\n总执行时间: {total_time:.2f}秒")

if __name__ == '__main__':
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='Stock filtering script')
    parser.add_argument('--run_mode', type=int, help='Running mode: 1 for saving sector stocks to INI, 2 for filtering stocks')
    parser.add_argument('--trade_date', type=str, help='Trade date in format: YYYY-MM-DD')
    parser.add_argument('--weipan_exceed', type=int, default=FILTERS['weipan_expected'], help='尾盘超预期: 1=是, 0=否')
    parser.add_argument('--zaopan_exceed', type=int, default=FILTERS['zaopan_expected'], help='早盘超预期: 1=是, 0=否')
    parser.add_argument('--rising_wave', type=int, default=FILTERS['rising_wave'], help='上升形态: 1=是, 0=否')
    
    args = parser.parse_args()
    
    # 设置默认运行模式
    if args.run_mode is not None:
        RUN_MODE = args.run_mode
    if args.trade_date is not None:
        CONFIG['trade_date'] = args.trade_date
    
    # 执行主流程
    if RUN_MODE == 1:
        print("=== Running Mode 1: Save sector stocks to INI ===")
        save_sector_stocks_to_ini()
    elif RUN_MODE == 2:
        print("=== Running Mode 2: Filter stocks from INI ===")
        # 使用FILTERS中的默认值，只有当命令行明确传入时才覆盖
        weipan_value = args.weipan_exceed if args.weipan_exceed is not None else FILTERS['weipan_expected']
        zaopan_value = args.zaopan_exceed if args.zaopan_exceed is not None else FILTERS['zaopan_expected']
        rising_wave_value = args.rising_wave if args.rising_wave is not None else FILTERS['rising_wave']
        print(f"过滤器参数: weipan_exceed={weipan_value}, zaopan_exceed={zaopan_value}, rising_wave={rising_wave_value}")
        filter_auction_stocks_from_ini(weipan_exceed=weipan_value, zaopan_exceed=zaopan_value, rising_wave=rising_wave_value)
    else:
        print(f"Invalid RUN_MODE: {RUN_MODE}. Please set to 1 or 2.")