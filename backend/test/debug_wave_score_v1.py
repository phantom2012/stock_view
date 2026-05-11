#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
调试 calculate_rising_wave_score 接口的完整分解
包含：基础分 + 周期内回调得分 + 周期间回调得分
测试股票: 001309
测试日期: 2026-05-07
"""

stock_code = "603118"
trade_date = "2026-05-08"


import sys
import os

_project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_backend_dir = os.path.join(_project_root, 'backend')
sys.path.insert(0, _backend_dir)
sys.path.insert(0, _project_root)

from datetime import datetime
from stock_cache import get_stock_cache
from common.stock_code_convert import to_goldminer_symbol
from config import RISING_WAVE_CONFIG


# ========================================================================
# 原始接口调用（仅输出总分）
# ========================================================================
def test_original_api(symbol, trade_date):
    """调用原始 calculate_rising_wave_score 接口"""
    from stock_filter.stock_wave_analyzer import StockWaveAnalyzer

    cache = get_stock_cache()
    analyzer = StockWaveAnalyzer(cache)

    score = analyzer.calculate_rising_wave_score(symbol, trade_date)
    print(f"\n{'='*70}")
    print(f"【原始接口调用】calculate_rising_wave_score")
    print(f"{'='*70}")
    print(f"  股票: {symbol}")
    print(f"  日期: {trade_date}")
    print(f"  总分: {score}")
    print(f"{'='*70}\n")
    return score


# ========================================================================
# 带回调得分的完整分解调试
# ========================================================================
def debug_full_components(symbol, trade_date):
    """复现完整算法逻辑，逐项打印基础分 + 回调分"""
    config = RISING_WAVE_CONFIG
    MAX_GAP = config['max_gap']
    MIN_STREAK_DAYS = config['min_streak_days']
    MIN_STREAK_ALT_DAYS = config['min_streak_alt_days']
    MIN_GAIN_PCT = config['min_gain_pct']
    DAYS_COEF = config['days_score_coefficient']
    GAIN_COEF = config['gain_score_coefficient']
    PATTERN_SCORE_MAP = config['pattern_score_map']
    LOOKBACK_DAYS = config['lookback_days']
    WITHIN_CYCLE_DD_SCORE_MAP = config['within_cycle_drawdown_score_map']
    BETWEEN_CYCLE_MAX_DD = config['between_cycle_max_drawdown']
    BETWEEN_CYCLE_DD_RATIO = config['between_cycle_drawdown_ratio']
    BETWEEN_CYCLE_DD_SCORE_MAP = config['between_cycle_drawdown_score_map']

    print(f"配置参数:")
    print(f"  MAX_GAP={MAX_GAP}, MIN_STREAK_DAYS={MIN_STREAK_DAYS}")
    print(f"  MIN_STREAK_ALT_DAYS={MIN_STREAK_ALT_DAYS}, MIN_GAIN_PCT={MIN_GAIN_PCT}%")
    print(f"  DAYS_COEF={DAYS_COEF}, GAIN_COEF={GAIN_COEF}")
    print(f"  PATTERN_SCORE_MAP={PATTERN_SCORE_MAP}")
    print(f"  LOOKBACK_DAYS={LOOKBACK_DAYS}")
    print(f"  周期内回调分段得分: {WITHIN_CYCLE_DD_SCORE_MAP}")
    print(f"  周期间最大回调={BETWEEN_CYCLE_MAX_DD}%, 回调/涨幅比例>{BETWEEN_CYCLE_DD_RATIO}")
    print(f"  周期间回调分段得分: {BETWEEN_CYCLE_DD_SCORE_MAP}")

    total_days = LOOKBACK_DAYS + 20

    cache = get_stock_cache()
    data = cache.get_history_data(symbol, total_days, trade_date=trade_date, force_refresh=False)

    if data is None or len(data) < 3:
        print("\n❌ 未获取到足够的历史数据")
        return

    data = data.tail(total_days).reset_index(drop=True)
    n = len(data)

    print(f"\n获取到 {n} 条日K数据，日期范围: {data.iloc[0]['eob']} ~ {data.iloc[-1]['eob']}")
    print(f"(回溯{LOOKBACK_DAYS}个交易日 + 20天缓冲 = {total_days}条)")

    for idx, row in data.iterrows():
        print(f"  [{idx:3d}] eob={row['eob']}, close={row['close']:.2f}, high={row['high']:.2f}")

    all_sequences = []

    start_idx = 0
    while start_idx < n - 1:
        current_high = data.iloc[start_idx]['close']
        if current_high <= 0:
            start_idx += 1
            continue

        wave_start = -1
        for j in range(start_idx, n - 1):
            if data.iloc[j + 1]['close'] > data.iloc[j]['close']:
                wave_start = j + 1
                break
        if wave_start == -1:
            break

        base_close = data.iloc[wave_start - 1]['close']
        current_high = data.iloc[wave_start]['close']

        print(f"\n{'='*60}")
        print(f"尝试起始 [{start_idx}] → 首涨日 [{wave_start}] (日期: {data.iloc[wave_start]['eob']}, close={current_high:.2f}, 基准收盘价: {base_close:.2f})")
        print(f"{'='*60}")

        streak = 0
        days_since_breakthrough = 0
        pattern_distribution = {1: 0, 2: 0, 3: 0}
        streak_end_idx = wave_start
        seq_high = current_high

        max_within_drawdown = 0.0
        in_gap = False
        gap_start_price = 0.0
        gap_start_idx = -1
        last_decline_close = 0.0

        for i in range(wave_start + 1, n):
            current_close = data.iloc[i]['close']
            days_since_breakthrough += 1

            if current_close >= current_high:
                if days_since_breakthrough <= MAX_GAP:
                    if in_gap and gap_start_price > 0:
                        gap_drawdown = (gap_start_price - last_decline_close) / gap_start_price * 100
                        print(f"    -> [{i}] 突破! 间隙结束: 起始价={gap_start_price:.2f}, 末收盘={last_decline_close:.2f}, "
                              f"回调={gap_drawdown:.2f}%")
                        if gap_drawdown > max_within_drawdown:
                            max_within_drawdown = gap_drawdown
                            print(f"       ↑ 更新周期内最大回调={max_within_drawdown:.2f}%")
                        in_gap = False
                    else:
                        print(f"    -> [{i}] 突破! close={current_close:.2f} >= high={current_high:.2f}, 间隔={days_since_breakthrough}天")

                    current_high = current_close
                    if current_close > seq_high:
                        seq_high = current_close
                    streak += 1
                    streak_end_idx = i
                    pattern_distribution[days_since_breakthrough] += 1
                    days_since_breakthrough = 0
                else:
                    print(f"    -> [{i}] 超过最大间隔({MAX_GAP}天)，序列断开")
                    break
            else:
                if not in_gap:
                    in_gap = True
                    gap_start_price = current_high
                    gap_start_idx = i
                    print(f"    -> [{i}] 进入间隙: high={gap_start_price:.2f}, close={current_close:.2f}")
                last_decline_close = current_close

            if days_since_breakthrough > MAX_GAP:
                print(f"    -> [{i}] 连续未突破天数>{MAX_GAP}，序列断开")
                break

        if streak == 0:
            print(f"  -> 未产生任何突破，跳过")
            start_idx = wave_start + 1
            continue

        last_close_val = data.iloc[streak_end_idx]['close']
        period_gain = ((last_close_val - base_close) / base_close) * 100 if base_close > 0 else 0.0

        print(f"\n  [统计]")
        print(f"    连续突破次数(streak): {streak}")
        print(f"    升浪区间: {data.iloc[wave_start]['eob']} -> {data.iloc[streak_end_idx]['eob']}")
        print(f"    基准收盘价(首涨日前一天): {base_close:.2f}, 结束收盘价: {last_close_val:.2f}")
        print(f"    区间涨幅(period_gain): {period_gain:.2f}%")
        print(f"    区间最高价(seq_high): {seq_high:.2f}")
        print(f"    突破间隔分布: {pattern_distribution}")
        print(f"    周期内最大回调: {max_within_drawdown:.2f}%")

        qualifies = False
        qualify_reason = ""
        if streak >= MIN_STREAK_DAYS:
            qualifies = True
            qualify_reason = f"连续突破{streak}>={MIN_STREAK_DAYS}天"
        elif streak >= MIN_STREAK_ALT_DAYS and period_gain > MIN_GAIN_PCT:
            qualifies = True
            qualify_reason = f"连续突破{streak}>={MIN_STREAK_ALT_DAYS}天 且 涨幅{period_gain:.2f}%>{MIN_GAIN_PCT}%"

        if not qualifies:
            print(f"  ❌ 未通过筛选，跳过")
            start_idx = wave_start + 1
            continue

        print(f"  ✅ 通过筛选: {qualify_reason}")

        max_gap_present = max(k for k, v in pattern_distribution.items() if v > 0)
        pattern_score = PATTERN_SCORE_MAP.get(max_gap_present, 0)

        streak_score = min(streak * DAYS_COEF, 10)
        gain_score = min(period_gain * GAIN_COEF, 15)
        base_score = streak_score + gain_score + pattern_score

        within_dd_score = 0.0
        for threshold in sorted(WITHIN_CYCLE_DD_SCORE_MAP.keys()):
            if max_within_drawdown <= threshold:
                within_dd_score = float(WITHIN_CYCLE_DD_SCORE_MAP[threshold])
                print(f"  周期内回调得分: {max_within_drawdown:.2f}% <= {threshold}% → {within_dd_score}分")
                break
        if within_dd_score == 0.0:
            print(f"  周期内回调得分: {max_within_drawdown:.2f}% 超出所有分段 → 0分")

        print(f"\n  {'─'*50}")
        print(f"  【基础分分解】")
        print(f"  {'─'*50}")
        print(f"  ① streak={streak} × DAYS_COEF({DAYS_COEF}) = {streak * DAYS_COEF}（上限10分，取 {streak_score}）")
        print(f"  ② period_gain={period_gain:.2f}% × GAIN_COEF({GAIN_COEF}) = {period_gain * GAIN_COEF:.2f}（上限15分，取 {gain_score:.2f}）")
        print(f"  ③ 最大突破间隔={max_gap_present}(分布={pattern_distribution}) → pattern_score={pattern_score}")
        print(f"  ★ 基础分 = {base_score:.2f}")
        print(f"  ★ 周期内回调加分 = +{within_dd_score}")
        print(f"  {'─'*50}")

        all_sequences.append({
            'start_idx': wave_start,
            'end_idx': streak_end_idx,
            'streak': streak,
            'period_gain': period_gain,
            'base_score': base_score,
            'max_gap_present': max_gap_present,
            'max_within_drawdown': max_within_drawdown,
            'within_dd_score': within_dd_score,
            'seq_high': seq_high,
            'desc': f"[{data.iloc[wave_start]['eob']}~{data.iloc[streak_end_idx]['eob']}] streak={streak}, gain={period_gain:.1f}%",
        })

        print(f"  -> 跳过已覆盖区间，下一个起始位置 → [{streak_end_idx + 1}]")
        start_idx = streak_end_idx + 1

    if not all_sequences:
        print(f"\n❌ 未找到任何满足条件的升浪序列")
        return

    # ===== 第二阶段：取倒数第二个到最后一个序列的间隔计算周期间回调 =====
    print(f"\n{'#'*70}")
    print(f"#  第二阶段：周期间回调（倒数第二个→最后一个序列的间隔）")
    print(f"{'#'*70}")

    last_seq = all_sequences[-1]
    last_idx = len(all_sequences) - 1
    print(f"\n最终计算序列（最后一个） #{last_idx}: {last_seq['desc']}")
    print(f"  基础分={last_seq['base_score']:.2f}, 周期内回调加分={last_seq['within_dd_score']:.2f}")

    between_dd_score = 0.0

    if len(all_sequences) >= 2:
        prev_seq = all_sequences[-2]
        print(f"\n取倒数第二个序列 #{last_idx - 1}: {prev_seq['desc']}")
        print(f"  -> 倒数第二个升浪最高收盘价: {prev_seq['seq_high']:.2f}")

        between_data = data.iloc[prev_seq['end_idx'] + 1:last_seq['start_idx'] + 1]
        if len(between_data) > 0:
            decline_low = between_data['close'].min()
            between_drawdown = (prev_seq['seq_high'] - decline_low) / prev_seq['seq_high'] * 100
            decline_ratio = between_drawdown / prev_seq['period_gain'] if prev_seq['period_gain'] > 0 else 0

            print(f"  -> 间隙数据: [{data.iloc[prev_seq['end_idx'] + 1]['eob']} ~ {data.iloc[last_seq['start_idx']]['eob']}]")
            print(f"  -> 间隙最低收盘价: {decline_low:.2f}")
            print(f"  -> 回调跌幅: {between_drawdown:.2f}% (条件: <={BETWEEN_CYCLE_MAX_DD}%)")
            print(f"  -> 回调/涨幅比例: {decline_ratio:.4f} (条件: <{BETWEEN_CYCLE_DD_RATIO})")

            if between_drawdown <= BETWEEN_CYCLE_MAX_DD and \
               prev_seq['period_gain'] > 0 and \
               decline_ratio < BETWEEN_CYCLE_DD_RATIO:
                for threshold in sorted(BETWEEN_CYCLE_DD_SCORE_MAP.keys()):
                    if between_drawdown <= threshold:
                        between_dd_score = float(BETWEEN_CYCLE_DD_SCORE_MAP[threshold])
                        print(f"  ✅ 条件满足！回调分段得分: {between_drawdown:.2f}% <= {threshold}% → {between_dd_score}分")
                        break
                if between_dd_score == 0.0:
                    print(f"  ✅ 条件满足！但回调幅度 {between_drawdown:.2f}% 超出所有分段 → 0分")
            else:
                fail_reasons = []
                if between_drawdown > BETWEEN_CYCLE_MAX_DD:
                    fail_reasons.append(f"回调{between_drawdown:.2f}% > 阈值{BETWEEN_CYCLE_MAX_DD}%")
                if prev_seq['period_gain'] <= 0:
                    fail_reasons.append(f"涨幅{prev_seq['period_gain']:.2f}% <= 0")
                elif decline_ratio >= BETWEEN_CYCLE_DD_RATIO:
                    fail_reasons.append(f"回调/涨幅({decline_ratio:.4f}) >= 阈值({BETWEEN_CYCLE_DD_RATIO})")
                print(f"  ❌ 条件不满足: {'; '.join(fail_reasons)}")
                print(f"  -> 周期间回调不得分")
        else:
            print(f"  -> 间隙无数据，周期间回调不得分")
    else:
        print(f"  -> 仅1个有效升浪周期（共{len(all_sequences)}个），无周期间回调数据，不得分")

    total_score = last_seq['base_score'] + last_seq['within_dd_score'] + between_dd_score
    print(f"\n{'='*60}")
    print(f"  ★ 最终序列 #{last_idx}: {last_seq['desc']}")
    print(f"  得分明细: 基础分({last_seq['base_score']:.2f}) + 周期内回调分({last_seq['within_dd_score']}) + 周期间回调分({between_dd_score}) = {total_score:.2f}")
    print(f"{'='*60}")



if __name__ == "__main__":

    symbol = to_goldminer_symbol(stock_code)
    dt = datetime.strptime(trade_date, '%Y-%m-%d')

    print(f"{'#'*70}")
    print(f"#  升浪形态得分 完整分解调试")
    print(f"#  股票: {stock_code} ({symbol})")
    print(f"#  日期: {trade_date}")
    print(f"{'#'*70}")

    original_score = test_original_api(symbol, dt)

    debug_full_components(symbol, dt)
