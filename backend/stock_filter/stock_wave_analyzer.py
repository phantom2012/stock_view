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
        1. 在 LOOKBACK_DAYS 个交易日范围内，从第一天开始向后遍历
        2. 从当前扫描位置找到第一个上涨日（收盘价 > 前一日收盘价）作为升浪起始日，
           以前一日收盘价作为周期涨幅基准价，起始日收盘价作为当前前高
        3. 继续向后扫描突破序列：当日收盘 >= 当前最高价即记为一次突破
           - 突破间隔天数限制在 MAX_GAP（3天）以内
           - 突破间隔 1=每天突破, 2=隔日突破, 3=隔2日突破
        4. 累计连续突破次数(streak)、统计各类突破间隔的天数分布
        5. 当升浪序列断开时检查是否通过筛选：
           - streak >= min_streak_days（10天），或
           - streak >= min_streak_alt_days（5天）且区间涨幅 > min_gain_pct（30%）
        6. 通过基础筛选后，额外检查周期内上涨天数占比：
           - 统计从首涨日到最后一个突破日的全部交易日中，收盘上涨的占比
           - 需满足 上涨天数/总交易日 > min_up_day_ratio（0.7）
        7. 通过筛选则记录该周期，并从结束日之后继续扫描下一个周期，确保序列不重叠
        8. 遍历完所有日期后，取最后一个满足条件的升浪周期作为计算区间
        9. 对最终区间计算三项得分：
           a. 周期内最大回调幅度（未突破期间的最大回调）及分段得分，
              同时检查单日最大跌幅 <= within_cycle_max_single_day_drop（9%）和
              连续两日最大累计跌幅 <= within_cycle_max_two_day_drop（5%），
              任一超出则周期内回调得分为0
           b. 周期间回调幅度：取倒数第二个到最后一个序列之间的回调跌幅，
              与最后一个序列结束后到数据末尾的回调跌幅，两者取较大值，
              需同时满足两个条件才给分，条件不满足时周期间得分为0：
              - 回调跌幅 <= between_cycle_max_drawdown（20%）
              - 回调跌幅/参考升浪累计涨幅 < between_cycle_drawdown_ratio（50%，浅回调条件）
        10. 基础分中连续突破天数得分上限12分，区间涨幅得分上限15分

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
        WITHIN_CYCLE_MAX_SINGLE_DAY_DROP = config['within_cycle_max_single_day_drop']
        WITHIN_CYCLE_MAX_TWO_DAY_DROP = config['within_cycle_max_two_day_drop']
        MIN_UP_DAY_RATIO = config['min_up_day_ratio']
        MIN_AVG_DAILY_GAIN = config['min_avg_daily_gain']
        MIN_LIMIT_UP_DAYS = config['min_limit_up_days']
        LIMIT_UP_NEXT_RED_RATIO = config['limit_up_next_red_ratio']
        BETWEEN_CYCLE_MAX_DD = config['between_cycle_max_drawdown']
        BETWEEN_CYCLE_DD_RATIO = config['between_cycle_drawdown_ratio']
        BETWEEN_CYCLE_DD_SCORE_MAP = config['between_cycle_drawdown_score_map']

        try:
            data = self.cache.get_history_data(symbol, LOOKBACK_DAYS, trade_date=trade_date, force_refresh=False)

            if data is None or len(data) < 3:
                return 0.0

            data = data.tail(LOOKBACK_DAYS).reset_index(drop=True)
            n = len(data)

            limit_up_days = 0
            limit_up_next_red_days = 0
            for i in range(n - 1):
                pre_close = data.iloc[i]['pre_close']
                if pre_close <= 0:
                    continue
                limit_up_price = round(pre_close * 1.10, 2)
                if round(data.iloc[i]['close'], 2) >= limit_up_price:
                    limit_up_days += 1
                    if data.iloc[i + 1]['close'] > data.iloc[i]['close']:
                        limit_up_next_red_days += 1

            if limit_up_days < MIN_LIMIT_UP_DAYS:
                return 0.0

            next_red_ratio = limit_up_next_red_days / limit_up_days if limit_up_days > 0 else 0.0
            if next_red_ratio < LIMIT_UP_NEXT_RED_RATIO:
                return 0.0

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
                max_single_day_drop = 0.0
                max_two_day_drop = 0.0

                for i in range(wave_start + 1, n):
                    current_close = data.iloc[i]['close']
                    days_since_breakthrough += 1

                    prev_close = data.iloc[i - 1]['close']
                    if current_close < prev_close:
                        single_drop = (prev_close - current_close) / prev_close * 100
                        if single_drop > max_single_day_drop:
                            max_single_day_drop = single_drop

                    if i >= wave_start + 2:
                        two_day_prev_close = data.iloc[i - 2]['close']
                        if current_close < two_day_prev_close:
                            two_day_drop = (two_day_prev_close - current_close) / two_day_prev_close * 100
                            if two_day_drop > max_two_day_drop:
                                max_two_day_drop = two_day_drop

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

                total_days_in_cycle = streak_end_idx - wave_start + 1
                up_days_in_cycle = 1
                for k in range(wave_start + 1, streak_end_idx + 1):
                    if data.iloc[k]['close'] > data.iloc[k - 1]['close']:
                        up_days_in_cycle += 1

                max_gap_present = max(k for k, v in pattern_distribution.items() if v > 0)
                pattern_score = PATTERN_SCORE_MAP.get(max_gap_present, 0)

                original_score = min(streak * DAYS_COEF, 12) + min(period_gain * GAIN_COEF, 15) + pattern_score

                within_dd_score = self._score_drawdown(max_within_drawdown, WITHIN_CYCLE_DD_SCORE_MAP)

                if max_single_day_drop > WITHIN_CYCLE_MAX_SINGLE_DAY_DROP or max_two_day_drop > WITHIN_CYCLE_MAX_TWO_DAY_DROP:
                    within_dd_score = 0.0

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
                    'max_single_day_drop': max_single_day_drop,
                    'max_two_day_drop': max_two_day_drop,
                    'total_days_in_cycle': total_days_in_cycle,
                    'up_days_in_cycle': up_days_in_cycle,
                })

                start_idx = streak_end_idx + 1

            if not all_sequences:
                return 0.0

            total_up_days = sum(s['up_days_in_cycle'] for s in all_sequences)
            total_days_all = sum(s['total_days_in_cycle'] for s in all_sequences)
            combined_up_ratio = total_up_days / total_days_all if total_days_all > 0 else 0.0
            if combined_up_ratio <= MIN_UP_DAY_RATIO:
                return 0.0

            total_gain_all = sum(s['period_gain'] for s in all_sequences)
            combined_avg_daily_gain = total_gain_all / total_days_all if total_days_all > 0 else 0.0
            if combined_avg_daily_gain <= MIN_AVG_DAILY_GAIN:
                return 0.0

            last_seq = all_sequences[-1]

            between_dd_score = 0.0
            max_drawdown = 0.0
            drawdown_ref_gain = 0.0

            if len(all_sequences) >= 2:
                prev_seq = all_sequences[-2]
                between_data = data.iloc[prev_seq['end_idx'] + 1:last_seq['start_idx'] + 1]

                if len(between_data) > 0:
                    decline_low = between_data['close'].min()
                    prev_wave_high = prev_seq['seq_high']
                    between_drawdown = (prev_wave_high - decline_low) / prev_wave_high * 100
                    if between_drawdown > max_drawdown:
                        max_drawdown = between_drawdown
                        drawdown_ref_gain = prev_seq['period_gain']

            after_data = data.iloc[last_seq['end_idx'] + 1:]
            if len(after_data) > 0:
                after_low = after_data['close'].min()
                after_high = last_seq['seq_high']
                after_drawdown = (after_high - after_low) / after_high * 100
                if after_drawdown > max_drawdown:
                    max_drawdown = after_drawdown
                    drawdown_ref_gain = last_seq['period_gain']

            if max_drawdown > 0 and drawdown_ref_gain > 0 and \
               max_drawdown <= BETWEEN_CYCLE_MAX_DD and \
               (max_drawdown / drawdown_ref_gain) < BETWEEN_CYCLE_DD_RATIO:
                between_dd_score = self._score_drawdown(max_drawdown, BETWEEN_CYCLE_DD_SCORE_MAP)

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
