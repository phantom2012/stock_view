"""
股票评分配置
集中管理所有评分相关的参数和映射表
"""
# 区间涨幅得分系数
# 最终得分 = interval_max_rise × INTERVAL_RISE_SCORE_COEFFICIENT
INTERVAL_RISE_SCORE_COEFFICIENT = 0.4
# ==================== 升浪形态V1配置 ====================

RISING_WAVE_CONFIG = {
    # 固定回溯天数（交易日）
    'lookback_days': 60,
    # 突破前高的最大间隔天数限制
    'max_gap': 3,
    # 连续突破天数 >= 该值即通过筛选
    'min_streak_days': 8,
    # 连续突破天数 >= 该值 且 区间涨幅 > min_gain_pct 也可通过筛选
    'min_streak_alt_days': 2,
    'min_gain_pct': 20.0,
    # 连续突破天数得分系数，最高10分
    'days_score_coefficient': 1.2,
    # 连续区间涨幅得分系数，最高15分
    'gain_score_coefficient': 0.15,
    # 突破形态分值映射表
    # key: 突破间隔天数 (1=每天突破, 2=隔日突破, 3=隔2日突破)
    # value: 对应得分
    'pattern_score_map': {
        1: 20,
        2: 12,
        3: 6,
    },
    # 周期内回调幅度阈值（%），超过该值不得分
    'within_cycle_drawdown_threshold': 10.0,
    # 周期内回调幅度分段得分（key: 回调幅度上限%, value: 得分）
    'within_cycle_drawdown_score_map': {
        0.5: 20,
        3: 15,
        6: 10,
        10: 5,
    },
    # 周期间最大允许回调跌幅（%）
    'between_cycle_max_drawdown': 20.0,
    # 周期间回调跌幅/上一升浪累计涨幅 比例阈值
    'between_cycle_drawdown_ratio': 0.5,
    # 周期间回调幅度分段得分
    'between_cycle_drawdown_score_map': {
        5: 15,
        10: 10,
        15: 5,
    },
}

# ==================== 转强得分配置 ====================

# key: turn_start_date 距离 trade_date 的天数上限
# value: 对应的得分
TURN_START_SCORE_MAP = {
    1: 30,
    3: 20,
    7: 15,
}

# 转强周期衰减配置
# 控制转强周期提前结束的两个补充条件
TURN_STRONG_CYCLE_CONFIG = {
    # 单个转强周期最长持续的天数（交易日）
    'max_cycle_days': 6,
    # 涨跌幅超过该阈值视为转强启动/重置信号（%）
    'pct_threshold': 3.0,
    # 开启新周期的最低净流入占比条件（%）：涨幅达标的同时净流入占比也须超过该值才允许开启
    'cycle_start_min_rate': 1.5,
    # 累计净流入衰退比例：当某日累计净流入 ≤ 周期内最大累计净流入 × 该比例时，当前转强周期结束
    'cumulative_decay_ratio': 0.6,
    # 日内流出触发比例：当某日净流出 > 周期内最大单日净流入 × 该比例时，当前转强周期结束或重置
    'daily_outflow_ratio': 0.7,
}
