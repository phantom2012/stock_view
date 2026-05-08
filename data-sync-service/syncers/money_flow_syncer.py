"""
资金流向数据同步器
从 backend/tasks/money_flow_sync_task.py 和 backend/services/money_flow_service.py 迁移
包含转强字段计算逻辑
"""
import logging
from datetime import datetime
from typing import List, Set, Tuple, Dict, Any

from shared.db import (
    get_session, get_session_ro, StockMoneyFlow, FilterResult, StockInfo, StockScore,
    upsert_by_unique_keys
)
from shared.stock_code_convert import to_goldminer_symbol
from shared.trade_date_util import TradeDateUtil
from external_data import get_query_handler
from config import MONEY_FLOW_CONFIG, TURN_START_SCORE_MAP, TURN_STRONG_CYCLE_CONFIG
from .base_syncer import BaseSyncer
from utils.log_utils import log_progress

logger = logging.getLogger(__name__)

trade_date_util = TradeDateUtil()
SCAN_DAYS = 30


class MoneyFlowSyncer(BaseSyncer):
    """
    资金流向数据同步器
    1. 从 filter_results 表获取所有股票代码
    2. 检查 stock_money_flow 表中最近30个交易日的数据是否完整
    3. 如有缺失则调用外部接口补充
    4. 同步完成后计算转强字段并更新
    """

    def sync(self, stock_codes=None) -> Tuple[bool, int, int, str]:
        logger.info("===== 开始资金流向数据同步 =====")
        try:
            # 如果没有传入股票列表，从 filter_results 表读取
            if stock_codes is None:
                stock_codes = self._get_filter_stock_codes()

            if not stock_codes:
                logger.warning("filter_results 表中没有股票数据，跳过同步")
                return True, 0, 0, "无股票数据"

            all_trade_dates = trade_date_util.get_recent_trade_dates(SCAN_DAYS)
            if not all_trade_dates:
                return False, 0, 0, "获取交易日列表失败"

            logger.info(f"最近 {SCAN_DAYS} 个交易日: {all_trade_dates[0]} ~ {all_trade_dates[-1]}")

            query_handler = get_query_handler()
            total_stocks = len(stock_codes)
            total_saved = 0
            failed_stocks = 0

            # 同步缺失数据
            for idx, code in enumerate(stock_codes, 1):
                try:
                    missing_dates = self._get_missing_trade_dates(code, all_trade_dates)
                    if not missing_dates:
                        continue

                    log_progress(f"[{idx}/{total_stocks}] {code}: 缺失 {len(missing_dates)} 个交易日", idx, total_stocks)
                    start_date = missing_dates[0]
                    end_date = missing_dates[-1]

                    symbol = to_goldminer_symbol(code)
                    df = query_handler.get_money_flow_data(
                        symbol=symbol, start_date=start_date, end_date=end_date
                    )

                    if df is None or df.empty:
                        logger.warning(f"  外部接口未返回 {code} 的数据")
                        failed_stocks += 1
                        continue

                    saved = self._save_money_flow_records(code, df)
                    total_saved += saved

                except Exception as e:
                    logger.error(f"  同步 {code} 失败: {e}")
                    failed_stocks += 1
                    continue

            # 计算转强字段
            logger.info("开始计算转强字段...")
            turn_strong_success = 0
            turn_strong_failed = 0
            for code in stock_codes:
                try:
                    self._calc_and_update_turn_strong_fields(code, all_trade_dates)
                    turn_strong_success += 1
                except Exception as e:
                    logger.error(f"  计算 {code} 转强字段失败: {e}")
                    turn_strong_failed += 1

            logger.info("===== 资金流向数据同步完成 =====")
            logger.info(f"总股票数: {total_stocks}, 成功保存: {total_saved}, 失败: {failed_stocks}")
            logger.info(f"转强字段: 成功 {turn_strong_success}, 失败 {turn_strong_failed}")

            return True, total_saved, failed_stocks, f"同步{total_saved}条, 转强{turn_strong_success}只"

        except Exception as e:
            logger.error(f"资金流向数据同步异常: {e}")
            import traceback; traceback.print_exc()
            return False, 0, 0, str(e)

    def _get_filter_stock_codes(self) -> List[str]:
        """从 filter_results 表获取所有股票代码"""
        try:
            with get_session_ro() as db:
                rows = db.query(FilterResult.code).distinct().all()
                codes = [row[0] for row in rows if row[0]]
                logger.info(f"从 filter_results 获取到 {len(codes)} 个股票代码")
                return codes
        except Exception as e:
            logger.error(f"获取 filter_results 股票代码失败: {e}")
            return []

    def _get_existing_trade_dates(self, code: str) -> Set[str]:
        """查询某只股票在 stock_money_flow 表中已有的交易日"""
        try:
            with get_session_ro() as db:
                rows = db.query(StockMoneyFlow.trade_date).filter(
                    StockMoneyFlow.code == code
                ).all()
                return {row[0] for row in rows if row[0]}
        except Exception as e:
            logger.error(f"查询 {code} 已有交易日失败: {e}")
            return set()

    def _get_missing_trade_dates(self, code: str, all_trade_dates: List[str]) -> List[str]:
        """获取某只股票缺失的交易日列表"""
        existing = self._get_existing_trade_dates(code)
        return [d for d in all_trade_dates if d not in existing]

    def _save_money_flow_records(self, code: str, df) -> int:
        """将 DataFrame 格式的资金流向数据保存到数据库"""
        if df is None or df.empty:
            return 0

        df_sorted = df.sort_values(by='trade_date').reset_index(drop=True)
        records = []
        for _, row in df_sorted.iterrows():
            trade_date = str(row.get('trade_date', ''))
            if len(trade_date) == 8:
                trade_date = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:8]}"
            records.append({
                'trade_date': trade_date,
                'pct_change': float(row.get('pct_change', 0)),
                'net_amount': float(row.get('net_amount', 0)),
                'name': row.get('name', ''),
                'close': row.get('close', 0),
                'net_amount_rate': row.get('net_amount_rate', 0),
                'net_d5_amount': row.get('net_d5_amount', 0),
                'buy_elg_amount': row.get('buy_elg_amount', 0),
                'buy_elg_amount_rate': row.get('buy_elg_amount_rate', 0),
                'buy_lg_amount': row.get('buy_lg_amount', 0),
                'buy_lg_amount_rate': row.get('buy_lg_amount_rate', 0),
                'buy_md_amount': row.get('buy_md_amount', 0),
                'buy_md_amount_rate': row.get('buy_md_amount_rate', 0),
                'buy_sm_amount': row.get('buy_sm_amount', 0),
                'buy_sm_amount_rate': row.get('buy_sm_amount_rate', 0),
            })

        saved_count = 0
        with get_session() as db:
            for rec in records:
                trade_date = rec['trade_date']
                unique_keys = {'code': code, 'trade_date': trade_date}
                update_data = {k: v for k, v in rec.items() if k != 'trade_date'}
                update_data['update_time'] = datetime.now()
                upsert_by_unique_keys(db, StockMoneyFlow, unique_keys, update_data)
                saved_count += 1
        return saved_count

    def _calc_and_update_turn_strong_fields(self, code: str, trade_dates: List[str]):
        """
        计算并更新转强字段
        从 backend/services/money_flow_service.py 迁移
        """
        records = self._get_money_flow_records_from_db(code, trade_dates)
        if not records:
            return

        circ_mv = self._get_circ_mv(code)
        self._calc_turn_strong_fields(records, circ_mv)
        self._update_turn_strong_fields(code, records)

    def _get_money_flow_records_from_db(self, code: str, trade_dates: List[str]) -> List[Dict]:
        """从数据库读取指定股票的资金流向记录"""
        try:
            with get_session_ro() as db:
                records = db.query(StockMoneyFlow).filter(
                    StockMoneyFlow.code == code,
                    StockMoneyFlow.trade_date.in_(trade_dates)
                ).order_by(StockMoneyFlow.trade_date).all()

                result = []
                for record in records:
                    result.append({
                        'trade_date': record.trade_date,
                        'pct_change': record.pct_change or 0,
                        'net_amount': record.net_amount or 0,
                        'net_amount_rate': record.net_amount_rate or 0,
                        'turn_start_date': record.turn_start_date,
                        'turn_start_net_amount': record.turn_start_net_amount or 0,
                        'turn_start_net_amount_rate': record.turn_start_net_amount_rate or 0,
                    })
                return result
        except Exception as e:
            logger.error(f"从数据库读取 {code} 资金流向数据失败: {e}")
            return []

    def _get_circ_mv(self, code: str) -> float:
        """从 stock_info 表获取流通市值（万元）"""
        try:
            with get_session_ro() as db:
                stock_info = db.query(StockInfo).filter(StockInfo.code == code).first()
                if stock_info and stock_info.circ_mv:
                    return float(stock_info.circ_mv)
        except Exception as e:
            logger.error(f"获取 {code} 流通市值失败: {e}")
        return 0.0

    def _calc_turn_start_score(self, turn_start_date: str, trade_date: str) -> float:
        if not turn_start_date or not trade_date:
            return 0.0
        try:
            start = datetime.strptime(turn_start_date, '%Y-%m-%d')
            end = datetime.strptime(trade_date, '%Y-%m-%d')
            days = (end - start).days
            if days <= 0:
                return 0.0
            for threshold in sorted(TURN_START_SCORE_MAP.keys()):
                if days <= threshold:
                    return TURN_START_SCORE_MAP[threshold]
            return 0.0
        except ValueError:
            return 0.0

    def _update_turn_strong_fields(self, code: str, records: List[Dict]):
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

                    turn_start_score = self._calc_turn_start_score(
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
            logger.debug(f"{code}: 更新 {len(records)} 条转强字段")

    def _calc_turn_strong_fields(self, records: List[Dict], circ_mv: float):
        """
        计算转强字段

        核心算法逻辑：
        1. 参数说明：
           - MAX_CYCLE_DAYS=6：单个转强周期最长持续6个交易日
           - PCT_THRESHOLD=5.0：涨跌幅超过5%视为转强启动/重置信号
           - NET_OUTFLOW_RATIO=0.3：周期内累计净流出若超过启动日净流入的30%，则重置周期

        2. 预处理：
           - 前缀和：对 net_amount 构建前缀和数组，支持 O(1) 范围区间求和
           - 连续净流入起始索引：识别连续净流入（net_amount>0且net_amount_rate>2%）的起始日索引，最长追溯 MAX_CYCLE_DAYS 天
           - RMQ 稀疏表：对 net_amount 构建区间最小值查询结构，用于快速查找指定区间内的最大净流出（最负值）

        3. 主循环遍历每条记录：
           - 非周期状态：若当日涨幅 > PCT_THRESHOLD（5%），调用 _start_new_cycle() 启动新周期
           - 周期状态：
             a) 周期已超 MAX_CYCLE_DAYS（6天）：强制结束，重新判断是否启动新周期
             b) 当日涨幅 > PCT_THRESHOLD（5%）：检查自周期启动以来（除启动日）的最大净流出是否超过启动日净流入的 NET_OUTFLOW_RATIO（30%）
                - 若超过：说明资金面转弱，重置周期（重新以当日为起点启动新周期）
                - 若未超过：延续当前周期
             c) 当日净流出 > 周期内最大单日净流入 × DAILY_OUTFLOW_RATIO（来自 TURN_STRONG_CYCLE_CONFIG）：说明资金异常出逃，结束当前周期（不重置；当日涨幅>5%时另启新周期）
             d) 当日累计净流入 ≤ 周期内最大累计净流入 × CUMULATIVE_DECAY_RATIO（来自 TURN_STRONG_CYCLE_CONFIG）：说明资金持续衰退，结束当前周期（不重置；当日涨幅>5%时另启新周期）
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
        MAX_CYCLE_DAYS = 6
        PCT_THRESHOLD = 5.0
        NET_OUTFLOW_RATIO = 0.3
        CUMULATIVE_DECAY_RATIO = TURN_STRONG_CYCLE_CONFIG['cumulative_decay_ratio']
        DAILY_OUTFLOW_RATIO = TURN_STRONG_CYCLE_CONFIG['daily_outflow_ratio']

        n = len(records)
        if n == 0:
            return

        # 前缀和
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

        # 连续净流入起始索引
        consecutive_start_limit = list(range(n))
        for i in range(1, n):
            if (records[i]['net_amount'] > 0 and records[i].get('net_amount_rate', 0) > 2 and
                    records[i - 1]['net_amount'] > 0 and records[i - 1].get('net_amount_rate', 0) > 2):
                candidate = consecutive_start_limit[i - 1]
                if i - candidate + 1 <= MAX_CYCLE_DAYS:
                    consecutive_start_limit[i] = candidate

        # RMQ 稀疏表
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

            start_net = records[new_start_idx]['net_amount']
            days = i - new_start_idx + 1
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

            return new_start_idx, start_net, days, cum

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
            if pct_change > PCT_THRESHOLD:
                _init_cycle_state(i)

        for i in range(n):
            pct_change = records[i]['pct_change']
            net_amount = records[i]['net_amount']

            if not in_cycle:
                if pct_change > PCT_THRESHOLD:
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
                        if max_outflow > cycle_start_net_amount * NET_OUTFLOW_RATIO:
                            need_reset = True

                    if not need_reset and net_amount < 0 and cycle_max_daily_net_amount > 0:
                        if abs(net_amount) > cycle_max_daily_net_amount * DAILY_OUTFLOW_RATIO:
                            _end_cycle(i, pct_change)
                            continue

                    if need_reset:
                        _init_cycle_state(i)
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
