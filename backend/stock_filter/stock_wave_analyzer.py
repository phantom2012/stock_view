import logging
from datetime import datetime
from typing import Optional

import pandas as pd

from config import RISING_WAVE_SCORE_MAP, RISING_WAVE_V1_CONFIG

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

    def calculate_rising_wave_score(self, symbol: str, trade_date: datetime,
                                    recent_days: int = 10) -> int:
        """
        计算升浪形态得分（0-100）

        Args:
            symbol: 股票代码（掘金格式）
            trade_date: 交易日期
            recent_days: 最近最大涨幅天数

        Returns:
            升浪形态得分（0-100）
        """
        try:
            data = self.cache.get_history_data(symbol, recent_days + 5, trade_date=trade_date, force_refresh=False)

            if data is not None and len(data) >= recent_days:
                data = data.tail(recent_days).reset_index(drop=True)

                min_close_loc = data['close'].idxmin()
                start_idx = min_close_loc

                if start_idx >= len(data) - 1:
                    return 0

                current_high = data.iloc[start_idx]['close']
                max_days_to_break = 0
                current_streak = 1

                for i in range(start_idx + 1, len(data)):
                    current_close = data.iloc[i]['close']
                    if current_close >= current_high:
                        current_high = current_close
                        if current_streak > max_days_to_break:
                            max_days_to_break = current_streak
                        current_streak = 1
                    else:
                        current_streak += 1

                if current_streak > max_days_to_break:
                    max_days_to_break = current_streak

                if max_days_to_break > 4:
                    return 0

                return RISING_WAVE_SCORE_MAP.get(max_days_to_break, 0)

            return 0
        except Exception as e:
            logger.error(f"[StockWaveAnalyzer] Error calculating rising wave score for {symbol}: {e}")
            return 0

    # ==================== 升浪形态 V1 ====================

    def calculate_rising_wave_score_v1(self, symbol: str, trade_date: datetime,
                                       recent_days: int = 10) -> float:
        """
        计算升浪形态得分V1（升级版）

        算法逻辑：
        1. 在 recent_days + 20 个交易日范围内，从第一天开始向后遍历
        2. 对每个起始位置构建升浪序列：当日收盘 >= 当前最高价即记为一次突破
           - 突破间隔天数限制在 MAX_GAP（3天）以内
           - 突破间隔 1=每天突破, 2=隔日突破, 3=隔2日突破
        3. 累计连续突破次数(streak)、统计各类突破间隔的天数分布、跟踪连续每天突破最大次数
        4. 当升浪序列断开时检查是否通过筛选：
           - streak >= min_streak_days（10天），或
           - streak >= min_streak_alt_days（5天）且区间涨幅 > min_gain_pct（30%）
        5. 特殊规则：若序列内存在连续每天突破 >= daily_streak_threshold（10天），
           则以连续每天突破最大天数替代整体 streak
        6. 找到第一个符合条件的序列后不立即返回，记录其得分和突破间隔作为最优基准
        7. 继续向后遍历，后续序列若满足以下条件之一则替换最优：
           - 突破间隔更小（形态更纯）→ 无条件替换为最优
           - 突破间隔相同且得分更高 → 替换为更优
        8. 最终得分 = 连续突破天数 × days_coef + 区间涨幅 × gain_coef + 主力突破形态对应分值
           主力突破形态 = 三类突破间隔中存在的最大间隔天数（如序列中出现了gap=3，则按gap=3取分）

        Args:
            symbol: 股票代码
            trade_date: 交易日期
            recent_days: 区间天数

        Returns:
            升浪形态得分（float），未通过筛选返回 0
        """
        config = RISING_WAVE_V1_CONFIG
        MAX_GAP = config['max_gap']
        MIN_STREAK_DAYS = config['min_streak_days']
        MIN_STREAK_ALT_DAYS = config['min_streak_alt_days']
        MIN_GAIN_PCT = config['min_gain_pct']
        DAILY_STREAK_THRESHOLD = config['daily_streak_threshold']
        DAYS_COEF = config['days_score_coefficient']
        GAIN_COEF = config['gain_score_coefficient']
        PATTERN_SCORE_MAP = config['pattern_score_map']

        total_days = recent_days + 20

        try:
            data = self.cache.get_history_data(symbol, total_days, trade_date=trade_date, force_refresh=False)

            if data is None or len(data) < 3:
                return 0.0

            data = data.tail(total_days).reset_index(drop=True)
            n = len(data)

            best_score = 0.0
            best_gap = None

            for start_idx in range(n - 1):
                current_high = data.iloc[start_idx]['close']
                if current_high <= 0:
                    continue

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
                            break

                    if days_since_breakthrough > MAX_GAP:
                        break

                if streak == 0:
                    continue

                streak_data = data.iloc[start_idx:streak_end_idx + 1]
                period_gain = self.calculate_period_gain(streak_data)

                qualifies = False
                if streak >= MIN_STREAK_DAYS:
                    qualifies = True
                elif streak >= MIN_STREAK_ALT_DAYS and period_gain > MIN_GAIN_PCT:
                    qualifies = True

                if not qualifies:
                    continue

                if max_daily_streak >= DAILY_STREAK_THRESHOLD:
                    streak = max_daily_streak

                max_gap_present = max(k for k, v in pattern_distribution.items() if v > 0)
                pattern_score = PATTERN_SCORE_MAP.get(max_gap_present, 0)

                total_score = streak * DAYS_COEF + period_gain * GAIN_COEF + pattern_score

                if best_gap is None or max_gap_present < best_gap:
                    best_score = total_score
                    best_gap = max_gap_present
                elif max_gap_present == best_gap and total_score > best_score:
                    best_score = total_score

            return round(best_score, 2)
        except Exception as e:
            logger.error(f"[StockWaveAnalyzer] Error calculating rising wave score v1 for {symbol}: {e}")
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
