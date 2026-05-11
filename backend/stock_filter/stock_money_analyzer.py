import logging
from datetime import datetime
from typing import List, Dict, Optional

from config import TURN_START_SCORE_MAP, TURN_STRONG_CYCLE_CONFIG
from shared.db import get_session, get_session_ro, StockMoneyFlow, StockInfo, StockScore
from shared.trade_date_util import TradeDateUtil

logger = logging.getLogger(__name__)

trade_date_util = TradeDateUtil()
SCAN_DAYS = 30


class StockMoneyAnalyzer:
    """
    资金流向分析器
    负责个股资金流向的转强计算、得分计算及数据库更新
    与数据同步服务职责分离：只做计算和入库，不负责数据拉取
    """

    @staticmethod
    def _get_money_flow_records(code: str, trade_dates: list) -> List[Dict]:
        """从数据库读取指定股票的资金流向记录"""
        try:
            with get_session_ro() as db:
                records = db.query(StockMoneyFlow).filter(
                    StockMoneyFlow.code == code,
                    StockMoneyFlow.trade_date.in_(trade_dates)
                ).order_by(StockMoneyFlow.trade_date).all()

                return [
                    {
                        'trade_date': r.trade_date,
                        'pct_change': r.pct_change or 0,
                        'net_amount': r.net_amount or 0,
                        'net_amount_rate': r.net_amount_rate or 0,
                        'turn_start_date': r.turn_start_date,
                        'turn_start_net_amount': r.turn_start_net_amount or 0,
                        'turn_start_net_amount_rate': r.turn_start_net_amount_rate or 0,
                    }
                    for r in records
                ]
        except Exception as e:
            logger.error(f"读取资金流向数据失败 {code}: {e}")
            return []

    @staticmethod
    def _get_circ_mv(code: str) -> float:
        """从 stock_info 表获取流通市值（万元）"""
        try:
            with get_session_ro() as db:
                stock_info = db.query(StockInfo).filter(StockInfo.code == code).first()
                if stock_info and stock_info.circ_mv:
                    return float(stock_info.circ_mv)
        except Exception as e:
            logger.error(f"获取 {code} 流通市值失败: {e}")
        return 0.0

    @staticmethod
    def _calc_turn_start_score(turn_start_date: str, trade_date: str) -> float:
        if not turn_start_date or not trade_date:
            return 0.0
        try:
            days = trade_date_util.count_trade_days_between(turn_start_date, trade_date)
            if days < 0:
                return 0.0
            for threshold in sorted(TURN_START_SCORE_MAP.keys()):
                if days <= threshold:
                    return TURN_START_SCORE_MAP[threshold]
            return 0.0
        except Exception:
            return 0.0

    @staticmethod
    def _update_turn_strong_fields(code: str, records: List[Dict]):
        """将转强字段更新到 money_flow 表，同时更新 score 表的 turn_start_score"""
        with get_session() as db:
            for rec in records:
                trade_date = rec['trade_date']
                money_flow = db.query(StockMoneyFlow).filter(
                    StockMoneyFlow.code == code,
                    StockMoneyFlow.trade_date == trade_date
                ).first()
                if money_flow:
                    money_flow.turn_start_date = rec.get('turn_start_date')
                    money_flow.turn_start_net_amount = rec.get('turn_start_net_amount', 0)
                    money_flow.turn_start_net_amount_rate = rec.get('turn_start_net_amount_rate', 0)
                    money_flow.update_time = datetime.now()

                    turn_start_score = StockMoneyAnalyzer._calc_turn_start_score(
                        rec.get('turn_start_date'), trade_date
                    )
                    score_record = db.query(StockScore).filter(
                        StockScore.code == code,
                        StockScore.trade_date == trade_date
                    ).first()
                    if score_record:
                        score_record.turn_start_score = turn_start_score
                        score_record.update_time = datetime.now()
                    else:
                        db.add(StockScore(
                            code=code,
                            trade_date=trade_date,
                            turn_start_score=turn_start_score,
                            update_time=datetime.now()
                        ))
            db.commit()

    @staticmethod
    def calculate_and_update(code: str, trade_dates: Optional[List[str]] = None):
        """
        计算某只股票的转强字段并更新数据库

        Args:
            code: 纯数字股票代码
            trade_dates: 交易日列表，不传则使用最近 SCAN_DAYS 个交易日
        """
        if trade_dates is None:
            trade_dates = trade_date_util.get_recent_trade_dates(SCAN_DAYS)

        if not trade_dates:
            logger.warning(f"[{code}] 无交易日数据，跳过")
            return

        records = StockMoneyAnalyzer._get_money_flow_records(code, trade_dates)
        if not records:
            return

        circ_mv = StockMoneyAnalyzer._get_circ_mv(code)
        StockMoneyAnalyzer._calc_turn_strong_fields(records, circ_mv)
        StockMoneyAnalyzer._update_turn_strong_fields(code, records)
        logger.debug(f"[{code}] 转强字段更新完成，共 {len(records)} 条")

    # ==================== 转强计算核心算法 ====================

    @staticmethod
    def _calc_turn_strong_fields(records: List[Dict], circ_mv: float):
        """
        计算转强字段

        核心算法逻辑：
        1. 参数说明：
           - MAX_CYCLE_DAYS=6：单个转强周期最长持续6个交易日
           - PCT_THRESHOLD=5.0：涨跌幅超过5%视为转强启动/重置信号
           - DAILY_OUTFLOW_RATIO=0.7：周期内某日净流出超过周期内最大单日净流入 × 该比例，则触发结束或重置
           - CUMULATIVE_DECAY_RATIO=0.6：累计净流入衰退到最大累计净流入 × 该比例，则结束周期

        2. 预处理：
           - 前缀和：对 net_amount 构建前缀和数组，支持 O(1) 范围区间求和
           - 连续净流入起始索引：识别连续净流入（net_amount>0且net_amount_rate>2%）的起始日索引，最长追溯 MAX_CYCLE_DAYS 天
           - RMQ 稀疏表：对 net_amount 构建区间最小值查询结构，用于快速查找指定区间内的最大净流出（最负值）

        3. 主循环遍历每条记录：
           - 非周期状态：若当日涨幅 > PCT_THRESHOLD（5%），调用 _start_new_cycle() 启动新周期
           - 周期状态：
             a) 周期已超 MAX_CYCLE_DAYS（6天）：强制结束，重新判断是否启动新周期
             b) 当日涨幅 > PCT_THRESHOLD（5%）：检查自周期启动以来（除启动日）的最大净流出是否超过周期内最大单日净流入的 DAILY_OUTFLOW_RATIO
                - 若超过：说明资金面转弱，重置周期（重新以当日为起点启动新周期）
                - 若未超过：延续当前周期
             c) 当日净流出 > 周期内最大单日净流入 × DAILY_OUTFLOW_RATIO（来自 TURN_STRONG_CYCLE_CONFIG）：说明资金异常出逃，结束当前周期
             d) 当日累计净流入 ≤ 周期内最大累计净流入 × CUMULATIVE_DECAY_RATIO（来自 TURN_STRONG_CYCLE_CONFIG）：说明资金持续衰退，结束当前周期
             e) 普通情况：延续周期，累加净流入，更新周期内最大累计净流入和最大单日净流入

        4. _start_new_cycle(i) 内部逻辑：
           - 若当日满足净流入条件（net_amount>0且net_amount_rate>2%），取连续净流入起始索引作为周期起点
           - 否则以当日为起点
           - 从周期起点到当日 i，逐日记录：turn_start_date（周期启动日期）、turn_start_net_amount（累计净流入）、turn_start_net_amount_rate（累计净流入/流通市值*100）

        5. 输出字段（写入 records 每一项）：
           - turn_start_date：所属转强周期的启动日期
           - turn_start_net_amount：自周期启动以来的累计主力净流入（万元）
           - turn_start_net_amount_rate：累计净流入占流通市值的比例（%）
        """
        MAX_CYCLE_DAYS = TURN_STRONG_CYCLE_CONFIG['max_cycle_days']
        PCT_THRESHOLD = TURN_STRONG_CYCLE_CONFIG['pct_threshold']
        CYCLE_START_MIN_RATE = TURN_STRONG_CYCLE_CONFIG['cycle_start_min_rate']
        CUMULATIVE_DECAY_RATIO = TURN_STRONG_CYCLE_CONFIG['cumulative_decay_ratio']
        DAILY_OUTFLOW_RATIO = TURN_STRONG_CYCLE_CONFIG['daily_outflow_ratio']

        n = len(records)
        if n == 0:
            return

        prefix_sum = [0.0] * n
        prefix_sum[0] = records[0]['net_amount']
        for i in range(1, n):
            prefix_sum[i] = prefix_sum[i - 1] + records[i]['net_amount']

        def range_sum(l: int, r: int) -> float:
            if l > r:
                return 0.0
            if l == 0:
                return prefix_sum[r]
            return prefix_sum[r] - prefix_sum[l - 1]

        consecutive_start_limit = list(range(n))
        for i in range(1, n):
            if (records[i]['net_amount'] > 0 and records[i].get('net_amount_rate', 0) > 2 and
                    records[i - 1]['net_amount'] > 0 and records[i - 1].get('net_amount_rate', 0) > 2):
                candidate = consecutive_start_limit[i - 1]
                if i - candidate + 1 <= MAX_CYCLE_DAYS:
                    consecutive_start_limit[i] = candidate

        log_table = [0] * (n + 1)
        for i in range(2, n + 1):
            log_table[i] = log_table[i // 2] + 1
        K = log_table[n] + 1
        st = [[0.0] * n for _ in range(K)]
        for i in range(n):
            st[0][i] = records[i]['net_amount']
        for k in range(1, K):
            step = 1 << (k - 1)
            for i in range(n - (1 << k) + 1):
                st[k][i] = min(st[k - 1][i], st[k - 1][i + step])

        def range_min_net(l: int, r: int) -> float:
            if l > r:
                return 0.0
            length = r - l + 1
            k = log_table[length]
            return min(st[k][l], st[k][r - (1 << k) + 1])

        def max_outflow_between(l: int, r: int) -> float:
            if l > r:
                return 0.0
            min_val = range_min_net(l, r)
            return abs(min_val) if min_val < 0 else 0.0

        turn_start_dates = [None] * n
        turn_start_amounts = [0.0] * n
        turn_start_rate_amounts = [0.0] * n

        def _start_new_cycle(i: int) -> tuple:
            net_amount_i = records[i]['net_amount']
            if net_amount_i > 0 and records[i].get('net_amount_rate', 0) > 2:
                new_start_idx = consecutive_start_limit[i]
            else:
                new_start_idx = i

            cum = range_sum(new_start_idx, i)

            partial_sum = 0.0
            start_date = records[new_start_idx]['trade_date']
            for j in range(new_start_idx, i + 1):
                partial_sum += records[j]['net_amount']
                turn_start_dates[j] = start_date
                turn_start_amounts[j] = partial_sum
                if circ_mv > 0:
                    turn_start_rate_amounts[j] = (partial_sum / circ_mv) * 100
                else:
                    turn_start_rate_amounts[j] = 0.0

            return new_start_idx, records[new_start_idx]['net_amount'], i - new_start_idx + 1, cum

        in_cycle = False
        cycle_start_idx = -1
        cycle_start_net_amount = 0.0
        cycle_days = 0
        cumulative_net_amount = 0.0
        cycle_max_cumulative_net_amount = 0.0
        cycle_max_daily_net_amount = 0.0

        def _init_cycle_state(i: int):
            nonlocal in_cycle, cycle_start_idx, cycle_start_net_amount, \
                cycle_days, cumulative_net_amount, \
                cycle_max_cumulative_net_amount, cycle_max_daily_net_amount
            cycle_start_idx, cycle_start_net_amount, cycle_days, cumulative_net_amount = \
                _start_new_cycle(i)
            in_cycle = True
            cycle_max_cumulative_net_amount = cumulative_net_amount
            cycle_max_daily_net_amount = max(
                (records[j]['net_amount'] for j in range(cycle_start_idx, i + 1)),
                default=0.0
            )

        def _end_cycle(i: int, pct_change: float):
            nonlocal in_cycle, cycle_start_idx, cycle_start_net_amount, \
                cycle_days, cumulative_net_amount, \
                cycle_max_cumulative_net_amount, cycle_max_daily_net_amount
            turn_start_dates[i] = None
            turn_start_amounts[i] = 0.0
            turn_start_rate_amounts[i] = 0.0
            in_cycle = False
            cycle_start_idx = -1
            cycle_start_net_amount = 0.0
            cycle_days = 0
            cumulative_net_amount = 0.0
            cycle_max_cumulative_net_amount = 0.0
            cycle_max_daily_net_amount = 0.0
            if pct_change > PCT_THRESHOLD and records[i].get('net_amount_rate', 0) > CYCLE_START_MIN_RATE:
                _init_cycle_state(i)

        for i in range(n):
            pct_change = records[i]['pct_change']
            net_amount = records[i]['net_amount']

            if not in_cycle:
                if pct_change > PCT_THRESHOLD and records[i].get('net_amount_rate', 0) > CYCLE_START_MIN_RATE:
                    _init_cycle_state(i)
                else:
                    turn_start_dates[i] = None
                    turn_start_amounts[i] = 0.0
                    turn_start_rate_amounts[i] = 0.0
            else:
                if cycle_days > MAX_CYCLE_DAYS:
                    _end_cycle(i, pct_change)
                else:
                    need_reset = False
                    if pct_change > PCT_THRESHOLD:
                        max_outflow = max_outflow_between(cycle_start_idx + 1, i - 1)
                        if max_outflow > cycle_max_daily_net_amount * DAILY_OUTFLOW_RATIO:
                            need_reset = True

                    if not need_reset and net_amount < 0 and cycle_max_daily_net_amount > 0:
                        if abs(net_amount) > cycle_max_daily_net_amount * DAILY_OUTFLOW_RATIO:
                            _end_cycle(i, pct_change)
                            continue

                    if need_reset:
                        _end_cycle(i, pct_change)
                    else:
                        cycle_days += 1
                        cumulative_net_amount += net_amount

                        if cumulative_net_amount <= cycle_max_cumulative_net_amount * CUMULATIVE_DECAY_RATIO:
                            _end_cycle(i, pct_change)
                            continue

                        if cumulative_net_amount > cycle_max_cumulative_net_amount:
                            cycle_max_cumulative_net_amount = cumulative_net_amount
                        if net_amount > cycle_max_daily_net_amount:
                            cycle_max_daily_net_amount = net_amount

                        turn_start_dates[i] = records[cycle_start_idx]['trade_date']
                        turn_start_amounts[i] = cumulative_net_amount
                        if circ_mv > 0:
                            turn_start_rate_amounts[i] = (cumulative_net_amount / circ_mv) * 100
                        else:
                            turn_start_rate_amounts[i] = 0.0

        for i in range(n):
            records[i]['turn_start_date'] = turn_start_dates[i]
            records[i]['turn_start_net_amount'] = turn_start_amounts[i]
            records[i]['turn_start_net_amount_rate'] = turn_start_rate_amounts[i]
