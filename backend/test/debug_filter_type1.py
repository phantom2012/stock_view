#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
调试德明利(001309)筛选问题 - 使用板块代码
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime
from stock_filter import get_stock_filter
from stock_cache import get_stock_cache
from common.block_stock_util import get_blocks_by_stock, get_stocks_by_blocks
from models.filter_params import FilterParams
from common.stock_code_convert import to_goldminer_symbol

# 初始化
stock_filter = get_stock_filter()
stock_cache = get_stock_cache()

# 用户筛选条件 - 使用板块代码
# 880491=半导体, 880656=CPO概念, 880672=存储芯片, 880952=芯片
params = FilterParams(
    trade_date='2026-04-30',
    weipan_exceed=0,
    zaopan_exceed=0,
    rising_wave=1,  # 勾选了上升形态
    select_blocks='880491,880656,880672,880952',  # 使用板块代码
    interval_days=50,
    interval_max_rise=30.0,
    recent_days=10,
    recent_max_day_rise=6.0,
    prev_high_price_rate=70.0,
    only_main_board=True
)

# 德明利股票代码
stock_code = '001309'
symbol = to_goldminer_symbol(stock_code)

print(f"========== 德明利({stock_code})筛选分析 ==========")
print(f"筛选日期: {params.trade_date}")
print(f"选择的板块代码: {params.select_blocks}")
print()

# 1. 检查板块归属
print("【步骤1: 板块归属检查】")
blocks = get_blocks_by_stock(stock_code)
print(f"德明利所属板块代码: {blocks}")

selected_block_codes = [b.strip() for b in params.select_blocks.split(',') if b.strip()]
print(f"用户选择的板块代码: {selected_block_codes}")

# 检查是否有交集
block_intersection = set(blocks) & set(selected_block_codes)
print(f"板块交集: {block_intersection}")

has_block_match = len(block_intersection) > 0
if has_block_match:
    print("  ✓ 通过板块筛选")
else:
    print("  ✗ 未匹配到用户选择的板块")

print()

# 2. 检查是否为主板股票
print("【步骤2: 主板股票检查】")
is_main_board = stock_filter.check_is_main_board(symbol)
print(f"  主板股票: {'✓ 是' if is_main_board else '✗ 否'}")

# 3. 检查是否为10cm股票
print("【步骤3: 10cm股票检查】")
is_10cm = stock_filter.check_is_10cm(symbol)
print(f"  10cm股票: {'✓ 是' if is_10cm else '✗ 否'}")

print()

# 4. 检查性能条件
print("【步骤4: 性能条件检查】")
trade_date = datetime.strptime(params.trade_date, '%Y-%m-%d')
performance = stock_filter.check_performance(symbol, trade_date, params)
print(f"  区间最大涨幅: {performance.interval_max_rise}% (要求: >= {params.interval_max_rise}%)")
print(f"  日内最大涨幅: {performance.max_day_rise}% (要求: >= {params.recent_max_day_rise}%)")
print(f"  股价相对高点比例: {performance.prev_high_price_rate}% (要求: >= {params.prev_high_price_rate}%)")
print(f"  性能检查通过: {'✓ 是' if performance.is_pass else '✗ 否'}")

print()

# 5. 检查上升形态
print("【步骤5: 上升形态检查】")
rising_wave_score = stock_filter.calculate_rising_wave_score(symbol, trade_date, params.recent_days)
print(f"  升浪形态得分: {rising_wave_score} (要求: > 0)")
print(f"  上升形态检查通过: {'✓ 是' if rising_wave_score > 0 else '✗ 否'}")

print()
print("========== 分析结果 ==========")
if not has_block_match:
    print("❌ 德明利未被筛选的原因: 不在用户选择的板块中")
elif not is_main_board:
    print("❌ 德明利未被筛选的原因: 不是主板股票")
elif not is_10cm:
    print("❌ 德明利未被筛选的原因: 不是10cm股票")
elif not performance.is_pass:
    print("❌ 德明利未被筛选的原因: 性能条件不满足")
    if performance.interval_max_rise < params.interval_max_rise:
        print(f"   - 区间最大涨幅不足: {performance.interval_max_rise}% < {params.interval_max_rise}%")
    if performance.max_day_rise < params.recent_max_day_rise:
        print(f"   - 日内最大涨幅不足: {performance.max_day_rise}% < {params.recent_max_day_rise}%")
    if performance.prev_high_price_rate < params.prev_high_price_rate:
        print(f"   - 股价相对高点比例不足: {performance.prev_high_price_rate}% < {params.prev_high_price_rate}%")
elif rising_wave_score <= 0:
    print("❌ 德明利未被筛选的原因: 上升形态得分不满足")
else:
    print("✅ 德明利应该被筛选出来，但实际没有，请检查数据是否完整")
    print("   可能原因: 数据库中板块数据与实际不符，或前端传递的板块代码有误")
