#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
调试 calculate_rising_wave_score_v1 接口的三项得分分解
测试股票: 002245 (蔚蓝锂芯)
测试日期: 2026-05-07
"""
import sys
import os

_project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_backend_dir = os.path.join(_project_root, 'backend')
sys.path.insert(0, _backend_dir)
sys.path.insert(0, _project_root)

from datetime import datetime
from stock_cache import get_stock_cache
from common.stock_code_convert import to_goldminer_symbol
from config import RISING_WAVE_V1_CONFIG

# ========================================================================
# 原始接口调用（仅输出总分）
# ========================================================================
def test_original_api(symbol, trade_date):
    """调用原始 calculate_rising_wave_score_v1 接口"""
    from stock_filter.stock_wave_analyzer import StockWaveAnalyzer

    cache = get_stock_cache()
    analyzer = StockWaveAnalyzer(cache)

    score = analyzer.calculate_rising_wave_score_v1(symbol, trade_date, recent_days=10)
    print(f"\n{'='*70}")
    print(f"【原始接口调用】calculate_rising_wave_score_v1")
    print(f"{'='*70}")
    print(f"  股票: {symbol}")
    print(f"  日期: {trade_date}")
    print(f"  总分: {score}")
    print(f"{'='*70}\n")
    return score


# ========================================================================
# 分解调试（逐项打印三项得分）
# ========================================================================
def debug_three_components(symbol, trade_date):
    """复现算法逻辑，逐项打印三项得分"""
    config = RISING_WAVE_V1_CONFIG
    MAX_GAP = config['max_gap']
    MIN_STREAK_DAYS = config['min_streak_days']
    MIN_STREAK_ALT_DAYS = config['min_streak_alt_days']
    MIN_GAIN_PCT = config['min_gain_pct']
    DAILY_STREAK_THRESHOLD = config['daily_streak_threshold']
    DAYS_COEF = config['days_score_coefficient']
    GAIN_COEF = config['gain_score_coefficient']
    PATTERN_SCORE_MAP = config['pattern_score_map']

    print(f"配置参数:")
    print(f"  MAX_GAP={MAX_GAP}, MIN_STREAK_DAYS={MIN_STREAK_DAYS}")
    print(f"  MIN_STREAK_ALT_DAYS={MIN_STREAK_ALT_DAYS}, MIN_GAIN_PCT={MIN_GAIN_PCT}%")
    print(f"  DAILY_STREAK_THRESHOLD={DAILY_STREAK_THRESHOLD}")
    print(f"  DAYS_COEF={DAYS_COEF}, GAIN_COEF={GAIN_COEF}")
    print(f"  PATTERN_SCORE_MAP={PATTERN_SCORE_MAP}")

    recent_days = 10
    total_days = recent_days + 20

    cache = get_stock_cache()
    data = cache.get_history_data(symbol, total_days, trade_date=trade_date, force_refresh=False)

    if data is None or len(data) < 3:
        print("\n❌ 未获取到足够的历史数据")
        return

    data = data.tail(total_days).reset_index(drop=True)
    n = len(data)

    print(f"\n获取到 {n} 条日K数据，日期范围: {data.iloc[0]['eob']} ~ {data.iloc[-1]['eob']}")

    for idx, row in data.iterrows():
        print(f"  [{idx}] eob={row['eob']}, close={row['close']:.2f}, high={row['high']:.2f}, volume={row['volume']}")

    found_valid = False
    has_qualifying = False
    best_score = 0.0
    best_gap = None
    best_detail = ""

    for start_idx in range(n - 1):
        current_high = data.iloc[start_idx]['close']
        if current_high <= 0:
            continue

        print(f"\n{'='*60}")
        print(f"尝试起始位置 [{start_idx}] (日期: {data.iloc[start_idx]['eob']}, close={current_high:.2f})")
        print(f"{'='*60}")

        streak = 0
        days_since_breakthrough = 0
        pattern_distribution = {1: 0, 2: 0, 3: 0}
        daily_streak = 0
        max_daily_streak = 0
        streak_end_idx = start_idx

        for i in range(start_idx + 1, n):
            current_close = data.iloc[i]['close']
            days_since_breakthrough += 1

            if current_close >= current_high:
                if days_since_breakthrough <= MAX_GAP:
                    print(f"    -> [{i}] 突破! close={current_close:.2f} >= high={current_high:.2f}, 间隔={days_since_breakthrough}天")
                    current_high = current_close
                    streak += 1
                    streak_end_idx = i
                    pattern_distribution[days_since_breakthrough] += 1

                    if days_since_breakthrough == 1:
                        daily_streak += 1
                        if daily_streak > max_daily_streak:
                            max_daily_streak = daily_streak
                    else:
                        daily_streak = 0

                    days_since_breakthrough = 0
                else:
                    print(f"    -> [{i}] 超过最大间隔({MAX_GAP}天)，序列断开")
                    break

            if days_since_breakthrough > MAX_GAP:
                print(f"    -> [{i}] 连续未突破天数>{MAX_GAP}，序列断开")
                break

        if streak == 0:
            print(f"  -> 未产生任何突破，跳过")
            continue

        streak_data = data.iloc[start_idx:streak_end_idx + 1]
        first_close = streak_data.iloc[0]['close']
        last_close = streak_data.iloc[-1]['close']
        period_gain = ((last_close - first_close) / first_close) * 100 if first_close > 0 else 0.0

        print(f"\n  [统计]")
        print(f"    连续突破次数(streak): {streak}")
        print(f"    区间起始: {streak_data.iloc[0]['eob']} -> 结束: {streak_data.iloc[-1]['eob']}")
        print(f"    起始收盘价: {first_close:.2f}, 结束收盘价: {last_close:.2f}")
        print(f"    区间涨幅(period_gain): {period_gain:.2f}%")
        print(f"    突破间隔分布: {pattern_distribution}")
        print(f"    连续每天突破最大天数(max_daily_streak): {max_daily_streak}")

        # 判断是否通过筛选
        qualifies = False
        qualify_reason = ""
        if streak >= MIN_STREAK_DAYS:
            qualifies = True
            qualify_reason = f"连续突破{streak}>={MIN_STREAK_DAYS}天"
        elif streak >= MIN_STREAK_ALT_DAYS and period_gain > MIN_GAIN_PCT:
            qualifies = True
            qualify_reason = f"连续突破{streak}>={MIN_STREAK_ALT_DAYS}天 且 涨幅{period_gain:.2f}%>{MIN_GAIN_PCT}%"

        if qualifies:
            print(f"  ✅ 通过筛选: {qualify_reason}")

            if max_daily_streak >= DAILY_STREAK_THRESHOLD:
                print(f"  ⚠️ 触发特殊规则: max_daily_streak({max_daily_streak}) >= {DAILY_STREAK_THRESHOLD}")
                print(f"     用 max_daily_streak({max_daily_streak}) 替换 streak({streak})")
                streak = max_daily_streak

            max_gap_present = max(k for k, v in pattern_distribution.items() if v > 0)
            pattern_score = PATTERN_SCORE_MAP.get(max_gap_present, 0)

            component1 = streak * DAYS_COEF
            component2 = period_gain * GAIN_COEF
            component3 = pattern_score

            total_score = component1 + component2 + component3

            print(f"\n  {'─'*50}")
            print(f"  【三项得分分解】")
            print(f"  {'─'*50}")
            print(f"  ① streak={streak} × DAYS_COEF({DAYS_COEF}) = {component1}")
            print(f"  ② period_gain={period_gain:.2f}% × GAIN_COEF({GAIN_COEF}) = {component2:.2f}")
            print(f"  ③ 最大突破间隔={max_gap_present}(分布={pattern_distribution}) → pattern_score={pattern_score}")
            print(f"  ★ 当前总分 = {component1} + {component2:.2f} + {component3} = {total_score:.2f}")
            print(f"  {'─'*50}")

            # 最优决策逻辑：与当前最优比较
            if best_gap is None or max_gap_present < best_gap:
                print(f"  🏆 突破间隔更小({max_gap_present}<{best_gap or '无'})，替换为最优")
                best_score = total_score
                best_gap = max_gap_present
                best_detail = f"streak={streak}, 涨幅={period_gain:.2f}%, 间隔={max_gap_present}"
            elif max_gap_present == best_gap and total_score > best_score:
                print(f"  🏆 突破间隔相同({max_gap_present})且得分更高({total_score:.2f}>{best_score:.2f})，替换为最优")
                best_score = total_score
                best_detail = f"streak={streak}, 涨幅={period_gain:.2f}%, 间隔={max_gap_present}"
            else:
                print(f"  ⏭️ 未优于当前最优(间隔={best_gap}, 得分={best_score:.2f})，跳过")

            has_qualifying = True
            print()
        else:
            print(f"  ❌ 未通过筛选，继续下一个起始位置")

    if has_qualifying:
        print(f"\n{'='*60}")
        print(f"  ★ 最终最优得分: {best_score:.2f} (详情: {best_detail})")
        print(f"{'='*60}")
    else:
        print(f"\n❌ 未找到任何满足条件的升浪序列")


if __name__ == "__main__":
    stock_code = "001309"
    symbol = to_goldminer_symbol(stock_code)
    trade_date = datetime(2026, 5, 7)

    print(f"{'#'*70}")
    print(f"#  升浪形态得分 V1 调试")
    print(f"#  股票: {stock_code} ({symbol})")
    print(f"#  日期: {trade_date.strftime('%Y-%m-%d')}")
    print(f"{'#'*70}")

    # 1. 调用原始接口
    original_score = test_original_api(symbol, trade_date)

    # 2. 分解调试
    debug_three_components(symbol, trade_date)
