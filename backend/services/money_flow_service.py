import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

from models import StockMoneyFlow, StockInfo, get_session, get_session_ro
from baostock_data.trade_date_util import TradeDateUtil
from external_data.ext_data_query_handle import get_query_handler
from common.singleton import SingletonMixin
from common.db_utils import upsert_by_unique_keys
from common.stock_code_convert import to_goldminer_symbol
from tasks.money_flow_sync_task import sync_money_flow_data

logger = logging.getLogger(__name__)

trade_date_util = TradeDateUtil()


class MoneyFlowService(SingletonMixin):
    def __init__(self):
        self.query_handler = None
        self._init_query_handler()

    def _init_query_handler(self):
        try:
            self.query_handler = get_query_handler()
            logger.info("ExternalDataQueryHandler initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize ExternalDataQueryHandler: {e}")
            self.query_handler = None

    def load_money_flow_data(self, stocks: List[Dict[str, Any]], days: int = 30) -> Dict[str, Any]:
        """
        加载资金流向数据（修改版）

        流程：
        1. 调用 sync_money_flow_data 同步任务检查并补充缺失数据
        2. 从数据库读取最近30天的资金流向数据
        3. 计算转强字段（turn_start_date, turn_start_net_amount, turn_start_net_amount_rate）
        4. 更新数据库中的这三个字段

        Args:
            stocks: 股票列表
            days: 天数，默认30天

        Returns:
            Dict: 执行结果
        """
        result = {
            'success': 0,
            'failed': 0,
            'total': len(stocks)
        }

        try:
            # 1. 调用同步任务检查并补充缺失数据
            logger.info("调用资金流向数据同步任务...")
            sync_success = sync_money_flow_data()
            if not sync_success:
                logger.warning("资金流向数据同步任务执行失败，但继续处理已有数据")

            # 2. 获取最近30个交易日
            recent_trade_dates = trade_date_util.get_recent_trade_dates(days)
            if not recent_trade_dates:
                logger.error("获取最近交易日失败")
                return {"status": "error", "msg": "获取最近交易日失败"}

            logger.info(f"时间范围: {recent_trade_dates[0]} 至 {recent_trade_dates[-1]}")

            # 3. 遍历每只股票，读取数据库数据并更新转强字段
            for stock in stocks:
                code = stock.get('code', '')
                name = stock.get('name', '')

                if not code:
                    result['failed'] += 1
                    continue

                try:
                    # 3.1 从数据库读取最近30天的资金流向数据
                    records = self._get_money_flow_records_from_db(code, recent_trade_dates)
                    if not records:
                        logger.warning(f"{code}: 数据库中没有资金流向数据")
                        result['failed'] += 1
                        continue

                    # 3.2 获取流通市值
                    circ_mv = self._get_circ_mv(code)

                    # 3.3 计算转强字段
                    self._calc_turn_strong_fields(records, circ_mv)

                    # 3.4 更新数据库中的转强字段
                    self._update_turn_strong_fields(code, records)

                    result['success'] += 1
                    logger.info(f"{code}: 成功更新 {len(records)} 条转强字段")

                except Exception as e:
                    logger.error(f"处理 {code} 失败: {e}")
                    result['failed'] += 1

            return {"status": "success", "data": result}

        except Exception as e:
            logger.error(f"Error in load_money_flow_data: {str(e)}")
            import traceback
            traceback.print_exc()
            return {"status": "error", "msg": str(e)}

    def _get_money_flow_records_from_db(self, code: str, trade_dates: List[str]) -> List[Dict]:
        """
        从数据库读取指定股票的资金流向记录

        Args:
            code: 股票代码
            trade_dates: 交易日列表

        Returns:
            List[Dict]: 资金流向记录列表（已按日期升序）
        """
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

    def _update_turn_strong_fields(self, code: str, records: List[Dict]):
        """
        更新数据库中的转强字段

        Args:
            code: 股票代码
            records: 包含转强字段的记录列表
        """
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

            db.commit()
            logger.debug(f"{code}: 更新 {len(records)} 条转强字段")

    def _get_circ_mv(self, code: str) -> float:
        """从 stock_info 表获取流通市值（万元）"""
        try:
            with get_session() as db:
                stock_info = db.query(StockInfo).filter(StockInfo.code == code).first()
                if stock_info and stock_info.circ_mv:
                    return float(stock_info.circ_mv)
        except Exception as e:
            logger.error(f"Failed to get circ_mv for {code}: {e}")
        return 0.0

    def _calc_turn_strong_fields(self, records: List[Dict], circ_mv: float):
        """
        计算转强启动日期(turn_start_date)、累计净流入金额(turn_start_net_amount)
        和累计净流入占比(turn_start_net_amount_rate)
        （优化版：空间换时间，一次遍历 + 预计算辅助数组，消除嵌套循环）

        算法逻辑：
        遍历所有记录（已按日期升序），维护一个"转强周期"状态。

        1. 启动条件：当日涨幅 > 5%，则启动一个转强周期
           - 从当日往前回溯，找到连续净流入（net_amount > 0 且 net_amount_rate > 2）的第一日作为周期启动日
           - 回溯范围限制在涨幅>5%这天前面的最多5天，防止回溯到上一个周期
           - turn_start_date = 该启动日日期
           - 从该启动日开始累加 net_amount 为 turn_start_net_amount
           - turn_start_net_amount_rate = (turn_start_net_amount / 流通市值) * 100
           - 回溯更新启动日到当日之间所有记录

        2. 周期内每日处理：
           - 累加 net_amount 到 turn_start_net_amount
           - turn_start_net_amount_rate = (turn_start_net_amount / 流通市值) * 100
           - turn_start_date 保持为周期第一日

        3. 周期结束条件（满足任一即结束）：
           a) 周期持续超过6日（即 >= 7日）
           - 周期结束后，后续日期如果涨幅 > 5%，按第1点启动新周期
           - 回溯连续净流入时最多往前追溯5天，防止回溯到上一个周期

        4. 周期内遇到涨幅 > 5% 时的特殊处理：
           - 检查从启动日次日到该日前一日之间的所有日期
           - 如果其中存在某日净流出金额（net_amount < 0 的绝对值）> 启动日净流入金额的 0.3 倍
           - 则按第1点方式启动新一轮周期（回溯找连续净流入首日，最多5天）
           - 否则继续正常累加

        5. 非周期内的日期：
           - turn_start_date = None
           - turn_start_net_amount = 0
           - turn_start_net_amount_rate = 0

        Args:
            records: 资金流向记录列表（已按日期升序）
            circ_mv: 流通市值（万元）
        """
        MAX_CYCLE_DAYS = 6  # 周期最大持续天数，同时也是回溯连续净流入的最大天数限制
        PCT_THRESHOLD = 5.0  # 涨幅阈值 5%
        NET_OUTFLOW_RATIO = 0.3  # 净流出/启动日净流入比例阈值


        n = len(records)
        if n == 0:
            return

        # ========== 预计算辅助数组（空间换时间） ==========

        # 1. 前缀和数组：prefix_sum[i] = records[0..i] 的 net_amount 累加和
        #    用于 O(1) 计算任意区间 [l, r] 的累加和
        prefix_sum = [0.0] * n
        prefix_sum[0] = records[0]['net_amount']
        for i in range(1, n):
            prefix_sum[i] = prefix_sum[i - 1] + records[i]['net_amount']

        def range_sum(l: int, r: int) -> float:
            """O(1) 计算 records[l..r] 的 net_amount 累加和"""
            if l > r:
                return 0.0
            if l == 0:
                return prefix_sum[r]
            return prefix_sum[r] - prefix_sum[l - 1]

        # 2. 带周期天数限制的连续净流入起始索引数组：consecutive_start_limit[i]
        #    从 i 往前，连续满足 (net_amount > 0 and net_amount_rate > 2) 的第一日索引
        #    但最多往前追溯 MAX_CYCLE_DAYS 天，防止回溯到上一个周期
        #    如果 i 本身不满足条件，则 consecutive_start_limit[i] = i
        consecutive_start_limit = list(range(n))
        for i in range(1, n):
            if (records[i]['net_amount'] > 0 and records[i].get('net_amount_rate', 0) > 2 and
                    records[i - 1]['net_amount'] > 0 and records[i - 1].get('net_amount_rate', 0) > 2):
                # 检查从 consecutive_start_limit[i-1] 到 i 是否超过 MAX_CYCLE_DAYS 天
                candidate = consecutive_start_limit[i - 1]
                if i - candidate + 1 <= MAX_CYCLE_DAYS:
                    consecutive_start_limit[i] = candidate
                # 如果超过周期天数限制，则保持 consecutive_start_limit[i] = i（从当前日重新开始）
            # 否则保持 consecutive_start_limit[i] = i


        # 3. 区间最小值查询（RMQ）- 使用稀疏表实现 O(1) 查询任意区间 [l, r] 的最小 net_amount
        #    用于快速判断周期内涨幅>5%时，中间区间是否存在超过阈值的净流出
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
            """O(1) 查询 records[l..r] 区间内的最小 net_amount 值"""
            if l > r:
                return 0.0
            length = r - l + 1
            k = log_table[length]
            return min(st[k][l], st[k][r - (1 << k) + 1])

        def max_outflow_between(l: int, r: int) -> float:
            """O(1) 获取 records[l..r] 区间内的最大净流出绝对值
            如果区间内没有负值，返回 0
            """
            if l > r:
                return 0.0
            min_val = range_min_net(l, r)
            return abs(min_val) if min_val < 0 else 0.0

        # ========== 辅助数组暂存计算结果 ==========
        turn_start_dates = [None] * n
        turn_start_amounts = [0.0] * n
        turn_start_rate_amounts = [0.0] * n

        # ========== 提取公共方法：启动新周期并回溯更新 ==========
        def _start_new_cycle(i: int) -> tuple:
            """在索引 i 处启动新周期，返回 (new_start_idx, cycle_start_net_amount, cycle_days, cumulative_net_amount)
            回溯连续净流入时最多往前追溯 MAX_CYCLE_DAYS 天，防止回溯到上一个周期
            """

            net_amount_i = records[i]['net_amount']
            if net_amount_i > 0 and records[i].get('net_amount_rate', 0) > 2:
                new_start_idx = consecutive_start_limit[i]
            else:
                new_start_idx = i

            start_net = records[new_start_idx]['net_amount']
            days = i - new_start_idx + 1
            cum = range_sum(new_start_idx, i)

            # 回溯更新 new_start_idx 到 i 之间的所有记录
            partial_sum = 0.0
            start_date = records[new_start_idx]['trade_date']
            for j in range(new_start_idx, i + 1):
                partial_sum += records[j]['net_amount']
                turn_start_dates[j] = start_date
                turn_start_amounts[j] = partial_sum
                # 计算 turn_start_net_amount_rate = (累计净流入 / 流通市值) * 100
                if circ_mv > 0:
                    turn_start_rate_amounts[j] = (partial_sum / circ_mv) * 100
                else:
                    turn_start_rate_amounts[j] = 0.0

            return new_start_idx, start_net, days, cum

        # ========== 一次遍历完成所有逻辑 ==========
        in_cycle = False
        cycle_start_idx = -1
        cycle_start_net_amount = 0.0
        cycle_days = 0
        cumulative_net_amount = 0.0

        for i in range(n):
            pct_change = records[i]['pct_change']
            net_amount = records[i]['net_amount']

            if not in_cycle:
                if pct_change > PCT_THRESHOLD:
                    cycle_start_idx, cycle_start_net_amount, cycle_days, cumulative_net_amount = \
                        _start_new_cycle(i)
                    in_cycle = True
                else:
                    turn_start_dates[i] = None
                    turn_start_amounts[i] = 0.0
                    turn_start_rate_amounts[i] = 0.0
            else:
                # ---- 在周期中 ----
                if cycle_days > MAX_CYCLE_DAYS:
                    # 周期持续超过6日，结束当前周期
                    in_cycle = False
                    cycle_start_idx = -1
                    cycle_start_net_amount = 0.0
                    cycle_days = 0
                    cumulative_net_amount = 0.0

                    if pct_change > PCT_THRESHOLD:
                        cycle_start_idx, cycle_start_net_amount, cycle_days, cumulative_net_amount = \
                            _start_new_cycle(i)
                        in_cycle = True
                    else:
                        turn_start_dates[i] = None
                        turn_start_amounts[i] = 0.0
                        turn_start_rate_amounts[i] = 0.0
                else:
                    need_reset = False

                    if pct_change > PCT_THRESHOLD:
                        # 周期内再次遇到涨幅 > 5%，检查是否需要重置周期
                        # O(1) 查询从启动日次日到该日前一日之间的最大净流出
                        max_outflow = max_outflow_between(cycle_start_idx + 1, i - 1)
                        if max_outflow > cycle_start_net_amount * NET_OUTFLOW_RATIO:
                            need_reset = True

                    if need_reset:
                        cycle_start_idx, cycle_start_net_amount, cycle_days, cumulative_net_amount = \
                            _start_new_cycle(i)
                        in_cycle = True
                    else:
                        # 周期内正常累加
                        cycle_days += 1
                        cumulative_net_amount += net_amount

                        turn_start_dates[i] = records[cycle_start_idx]['trade_date']
                        turn_start_amounts[i] = cumulative_net_amount
                        # 计算 turn_start_net_amount_rate = (累计净流入 / 流通市值) * 100
                        if circ_mv > 0:
                            turn_start_rate_amounts[i] = (cumulative_net_amount / circ_mv) * 100
                        else:
                            turn_start_rate_amounts[i] = 0.0

        # ========== 将计算结果写回 records ==========
        for i in range(n):
            records[i]['turn_start_date'] = turn_start_dates[i]
            records[i]['turn_start_net_amount'] = turn_start_amounts[i]
            records[i]['turn_start_net_amount_rate'] = turn_start_rate_amounts[i]




def get_money_flow_service() -> MoneyFlowService:
    return MoneyFlowService.get_instance()
