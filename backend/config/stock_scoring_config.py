"""
股票评分配置
集中管理所有评分相关的参数和映射表
"""

# 升浪形态得分映射表
# key: 最大未突破天数 (max_days_to_break)
# value: 对应的得分
RISING_WAVE_SCORE_MAP = {
    1: 50,
    2: 30,
    3: 20,
    4: 10,
}

# 区间涨幅得分系数
# 最终得分 = interval_max_rise × INTERVAL_RISE_SCORE_COEFFICIENT
INTERVAL_RISE_SCORE_COEFFICIENT = 0.5

# ==================== 升浪形态V1配置 ====================

RISING_WAVE_V1_CONFIG = {
    # 突破前高的最大间隔天数限制
    'max_gap': 3,
    # 连续突破天数 >= 该值即通过筛选
    'min_streak_days': 10,
    # 连续突破天数 >= 该值 且 区间涨幅 > min_gain_pct 也可通过筛选
    'min_streak_alt_days': 5,
    'min_gain_pct': 30.0,
    # 区间内连续每天突破(gap=1)天数达到该值时，触发特殊规则：取每天突破最大天数替代整体连续天数
    'daily_streak_threshold': 10,
    # 连续突破天数得分系数
    'days_score_coefficient': 1.0,
    # 连续区间涨幅得分系数
    'gain_score_coefficient': 0.1,
    # 突破形态分值映射表
    # key: 突破间隔天数 (1=每天突破, 2=隔日突破, 3=隔2日突破)
    # value: 对应得分
    'pattern_score_map': {
        1: 30,
        2: 20,
        3: 10,
    },
}
