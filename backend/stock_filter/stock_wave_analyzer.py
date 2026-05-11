import logging
from datetime import datetime
from typing import Optional

import pandas as pd

from config import RISING_WAVE_CONFIG

logger = logging.getLogger(__name__)


class StockWaveAnalyzer:
    """
    日K线波形形态分析器
    基于日K线数据进行纯形态指标计算，不涉及业务逻辑和数据库操作
    职责：升浪形态、价格区间比例、区间涨幅、日内最大涨幅等
    """

    def __init__(self, stock_cache):
        self.cache = stock_cache

    # ==================== 升浪形态 ====================
    def _score_drawdown(self, drawdown: float, score_map: dict) -> float:
        """根据分段得分映射表计算回调幅度得分"""
        for threshold in sorted(score_map.keys()):
            if drawdown <= threshold:
                return float(score_map[threshold])
        return 0.0

    def calculate_rising_wave_score(self, symbol: str, trade_date: datetime, recent_days: int = 40) -> float:
        """
        计算升浪形态得分

        算法逻辑：
        1. 在 lookback_days + 20 个交易日范围内，从第一天开始向后遍历
        2. 对每个起始位置构建升浪序列：当日收盘 >= 当前最高价即记为一次突破
           - 突破间隔天数限制在 MAX_GAP（3天）以内
           - 突破间隔 1=每天突破, 2=隔日突破, 3=隔2日突破
        3. 累计连续突破次数(streak)、统计各类突破间隔的天数分布
        4. 当升浪序列断开时检查是否通过筛选：
           - streak >= min_streak_days（10天），或
           - streak >= min_streak_alt_days（5天）且区间涨幅 > min_gain_pct（30%）
        5. 遍历完所有日期后，取最后一个满足条件的升浪周期作为计算区间
        6. 对最终区间计算三项得分：
           a. 周期内最大回调幅度（未突破期间的最大回调）及分段得分
           b. 周期间回调幅度：取倒数第二个序列到最后一个序列之间的回调间隔，
              需同时满足两个条件才给分，条件不满足或序列不足2个时周期间得分为0
              - 回调跌幅 <= between_cycle_max_drawdown（20%）
              - 回调跌幅/上一升浪累计涨幅 < between_cycle_drawdown_ratio（50%，浅回调条件）
        7. 最终得分 = 连续突破天数 × days_coef + 区间涨幅 × gain_coef
                      + 主力突破形态对应分值 + 周期内回调得分 + 周期间回调得分

        Args:
            symbol: 股票代码
            trade_date: 交易日期
            recent_days: 区间天数（兼容旧调用，实际使用配置中的 lookback_days）

        Returns:
            升浪形态得分（float），未通过筛选返回 0
        """
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

        try:
            data = self.cache.get_history_data(symbol, LOOKBACK_DAYS, trade_date=trade_date, force_refresh=False)

            if data is None or len(data) < 3:
                return 0.0

            data = data.tail(LOOKBACK_DAYS).reset_index(drop=True)
            n = len(data)

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

                streak = 0
                days_since_breakthrough = 0
                pattern_distribution = {1: 0, 2: 0, 3: 0}
                streak_end_idx = wave_start
                seq_high = current_high

                max_within_drawdown = 0.0
                in_gap = False
                gap_start_price = 0.0
                last_decline_close = 0.0

                for i in range(wave_start + 1, n):
                    current_close = data.iloc[i]['close']
                    days_since_breakthrough += 1

                    if current_close >= current_high:
                        if days_since_breakthrough <= MAX_GAP:
                            if in_gap and gap_start_price > 0:
                                gap_drawdown = (gap_start_price - last_decline_close) / gap_start_price * 100
                                if gap_drawdown > max_within_drawdown:
                                    max_within_drawdown = gap_drawdown
                                in_gap = False

                            current_high = current_close
                            if current_close > seq_high:
                                seq_high = current_close
                            streak += 1
                            streak_end_idx = i
                            pattern_distribution[days_since_breakthrough] += 1
                            days_since_breakthrough = 0
                        else:
                            break
                    else:
                        if not in_gap:
                            in_gap = True
                            gap_start_price = current_high
                        last_decline_close = current_close

                    if days_since_breakthrough > MAX_GAP:
                        break

                if streak == 0:
                    start_idx = wave_start + 1
                    continue

                last_close = data.iloc[streak_end_idx]['close']
                period_gain = ((last_close - base_close) / base_close) * 100 if base_close > 0 else 0.0

                qualifies = False
                if streak >= MIN_STREAK_DAYS:
                    qualifies = True
                elif streak >= MIN_STREAK_ALT_DAYS and period_gain > MIN_GAIN_PCT:
                    qualifies = True

                if not qualifies:
                    start_idx = wave_start + 1
                    continue

                max_gap_present = max(k for k, v in pattern_distribution.items() if v > 0)
                pattern_score = PATTERN_SCORE_MAP.get(max_gap_present, 0)

                original_score = min(streak * DAYS_COEF, 10) + min(period_gain * GAIN_COEF, 15) + pattern_score

                within_dd_score = self._score_drawdown(max_within_drawdown, WITHIN_CYCLE_DD_SCORE_MAP)

                all_sequences.append({
                    'start_idx': wave_start,
                    'end_idx': streak_end_idx,
                    'streak': streak,
                    'period_gain': period_gain,
                    'original_score': original_score,
                    'max_gap_present': max_gap_present,
                    'max_within_drawdown': max_within_drawdown,
                    'within_dd_score': within_dd_score,
                    'seq_high': seq_high,
                })

                start_idx = streak_end_idx + 1

            if not all_sequences:
                return 0.0

            last_seq = all_sequences[-1]

            between_dd_score = 0.0

            if len(all_sequences) >= 2:
                prev_seq = all_sequences[-2]
                between_data = data.iloc[prev_seq['end_idx'] + 1:last_seq['start_idx'] + 1]

                if len(between_data) > 0:
                    decline_low = between_data['close'].min()
                    prev_wave_high = prev_seq['seq_high']
                    between_drawdown = (prev_wave_high - decline_low) / prev_wave_high * 100

                    if between_drawdown <= BETWEEN_CYCLE_MAX_DD and \
                       prev_seq['period_gain'] > 0 and \
                       (between_drawdown / prev_seq['period_gain']) < BETWEEN_CYCLE_DD_RATIO:
                        between_dd_score = self._score_drawdown(between_drawdown, BETWEEN_CYCLE_DD_SCORE_MAP)

            total_score = last_seq['original_score'] + last_seq['within_dd_score'] + between_dd_score

            return round(total_score, 2)
        except Exception as e:
            logger.error(f"[StockWaveAnalyzer] Error calculating rising wave score for {symbol}: {e}")
            return 0.0

    # ==================== 区间形态计算 ====================

    def calculate_price_ratio(self, data: pd.DataFrame) -> float:
        """
        计算区间内前期高点收盘价的最大回落比例
        """
        close_prices = data['close'].dropna()
        if len(close_prices) < 2:
            return 0.0

        min_ratio = 100.0

        for i in range(len(close_prices) - 1):
            high_price = close_prices.iloc[i]
            if high_price <= 0:
                continue

            after_prices = close_prices.iloc[i:]
            low_price = after_prices.min()

            if low_price >= high_price:
                continue

            ratio = (low_price / high_price) * 100
            if ratio < min_ratio:
                min_ratio = ratio

        if min_ratio >= 100.0:
            return 100.0

        return min_ratio

    def calculate_period_gain(self, data: pd.DataFrame) -> float:
        """计算区间最大涨幅（首尾收盘价）"""
        first_close = data.iloc[0]['close']
        last_close = data.iloc[-1]['close']
        if first_close > 0:
            return ((last_close - first_close) / first_close) * 100
        return 0.0

    def calculate_max_day_rise(self, data: pd.DataFrame, recent_days: int) -> float:
        """计算日内最大涨幅"""
        max_day_rise = 0.0
        if len(data) >= 2:
            for i in range(1, min(recent_days + 1, len(data))):
                current_row = data.iloc[-i]
                prev_row = data.iloc[-i - 1]
                current_high = current_row['high']
                prev_close = prev_row['close']
                if prev_close > 0:
                    day_gain = ((current_high - prev_close) / prev_close) * 100
                    if day_gain > max_day_rise:
                        max_day_rise = day_gain
        return max_day_rise
