import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

from models import StockMoneyFlow, get_session
from baostock_data.trade_date_util import TradeDateUtil
from external_data.ext_data_query_handle import get_query_handler
from common.singleton import SingletonMixin
from common.db_utils import upsert_by_unique_keys
from common.stock_code_convert import to_goldminer_symbol

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
        result = {
            'success': 0,
            'failed': 0,
            'total': len(stocks)
        }

        if not self.query_handler:
            logger.error("ExternalDataQueryHandler not initialized")
            return {"status": "error", "msg": "外部数据查询器初始化失败"}

        try:
            recent_trade_dates = trade_date_util.get_recent_trade_dates(days)
            if not recent_trade_dates:
                logger.error("Failed to get recent trade dates")
                return {"status": "error", "msg": "获取最近交易日失败"}

            logger.info(f"获取到 {len(recent_trade_dates)} 个交易日")

            if len(recent_trade_dates) < 2:
                logger.error("需要至少2个交易日")
                return {"status": "error", "msg": "需要至少2个交易日"}

            start_date = recent_trade_dates[0]
            end_date = recent_trade_dates[-1]

            logger.info(f"时间范围: {start_date} 至 {end_date}")

            for stock in stocks:
                code = stock.get('code', '')
                name = stock.get('name', '')

                if not code:
                    result['failed'] += 1
                    continue

                try:
                    symbol = to_goldminer_symbol(code)

                    logger.info(f"正在获取资金流向数据: {symbol}")

                    df = self.query_handler.get_money_flow_data(
                        symbol=symbol,
                        start_date=start_date,
                        end_date=end_date
                    )

                    if df is None or df.empty:
                        logger.warning(f"未获取到 {symbol} 的资金流向数据")
                        result['failed'] += 1
                        continue

                    self._save_money_flow_data(df, code, name)
                    result['success'] += 1
                    logger.info(f"成功保存 {symbol} 的资金流向数据，共 {len(df)} 条")

                except Exception as e:
                    logger.error(f"Failed to load money flow data for {code}: {e}")
                    result['failed'] += 1

            return {"status": "success", "data": result}

        except Exception as e:
            logger.error(f"Error in load_money_flow_data: {str(e)}")
            import traceback
            traceback.print_exc()
            return {"status": "error", "msg": str(e)}

    def _save_money_flow_data(self, df, code, name):
        """保存资金流向数据到数据库（存在则更新，不存在则插入）

        同时计算 turn_start_date（转强启动日期）和 turn_start_net_amount（转强累计净流入）：
        算法逻辑详见 _calc_turn_strong_fields 方法
        """
        # 先按日期升序排序
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
                'name': row.get('name', name),
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

        # 计算 turn_start_date 和 turn_start_net_amount
        self._calc_turn_strong_fields(records)

        # 入库
        with get_session() as db:
            for rec in records:
                trade_date = rec['trade_date']
                unique_keys = {'code': code, 'trade_date': trade_date}
                update_data = {
                    'name': rec['name'],
                    'pct_change': rec['pct_change'],
                    'close': rec['close'],
                    'net_amount': rec['net_amount'],
                    'net_amount_rate': rec['net_amount_rate'],
                    'net_d5_amount': rec['net_d5_amount'],
                    'turn_start_date': rec.get('turn_start_date'),
                    'turn_start_net_amount': rec.get('turn_start_net_amount', 0),
                    'buy_elg_amount': rec['buy_elg_amount'],
                    'buy_elg_amount_rate': rec['buy_elg_amount_rate'],
                    'buy_lg_amount': rec['buy_lg_amount'],
                    'buy_lg_amount_rate': rec['buy_lg_amount_rate'],
                    'buy_md_amount': rec['buy_md_amount'],
                    'buy_md_amount_rate': rec['buy_md_amount_rate'],
                    'buy_sm_amount': rec['buy_sm_amount'],
                    'buy_sm_amount_rate': rec['buy_sm_amount_rate'],
                    'update_time': datetime.now()
                }

                upsert_by_unique_keys(db, StockMoneyFlow, unique_keys, update_data)

            logger.info(f"成功保存 {len(records)} 条资金流向数据到数据库")

    def _calc_turn_strong_fields(self, records: List[Dict]):
        """
        计算转强启动日期(turn_start_date)和累计净流入金额(turn_start_net_amount)

        算法逻辑：
        遍历所有记录（已按日期升序），维护一个"转强周期"状态。

        1. 启动条件：当日涨幅 > 5%，则启动一个转强周期
           - turn_start_date = 当日日期
           - 从当日开始累加 net_amount 为 turn_start_net_amount

        2. 周期内每日处理：
           - 累加 net_amount 到 turn_start_net_amount
           - turn_start_date 保持为周期第一日

        3. 周期结束条件（满足任一即结束）：
           a) 周期持续超过6日（即 >= 7日）


        4. 周期内遇到涨幅 > 5% 时的特殊处理：
           - 检查从启动日次日到该日前一日之间的所有日期
           - 如果其中存在某日净流出金额（net_amount < 0 的绝对值）> 启动日净流入金额的 0.3 倍
           - 则把该涨幅 > 5% 的当日作为新一轮周期的第一日，重新开始累计
           - 否则继续正常累加

        5. 非周期内的日期：
           - turn_start_date = None
           - turn_start_net_amount = 0
        """
        MAX_CYCLE_DAYS = 6
        PCT_THRESHOLD = 5.0  # 涨幅阈值 5%
        NET_OUTFLOW_RATIO = 0.3  # 净流出/启动日净流入比例阈值

        n = len(records)
        # 当前周期状态
        in_cycle = False
        cycle_start_idx = -1  # 周期启动日在 records 中的索引
        cycle_start_net_amount = 0.0  # 启动日的净流入金额
        cycle_days = 0
        cumulative_net_amount = 0.0

        for i, rec in enumerate(records):
            pct_change = rec['pct_change']
            net_amount = rec['net_amount']

            if not in_cycle:
                # ---- 不在周期中 ----
                if pct_change > PCT_THRESHOLD:
                    # 启动新周期
                    in_cycle = True
                    cycle_start_idx = i
                    cycle_start_net_amount = net_amount
                    cycle_days = 1
                    cumulative_net_amount = net_amount

                    rec['turn_start_date'] = rec['trade_date']
                    rec['turn_start_net_amount'] = cumulative_net_amount
                else:
                    rec['turn_start_date'] = None
                    rec['turn_start_net_amount'] = 0
            else:
                # ---- 在周期中 ----
                # 先检查是否超过最大天数（大于6天即>=7天时结束）
                if cycle_days > MAX_CYCLE_DAYS:

                    # 周期结束
                    in_cycle = False
                    cycle_start_idx = -1
                    cycle_start_net_amount = 0.0
                    cycle_days = 0
                    cumulative_net_amount = 0.0

                    # 当前日期重新判断是否启动新周期
                    if pct_change > PCT_THRESHOLD:
                        in_cycle = True
                        cycle_start_idx = i
                        cycle_start_net_amount = net_amount
                        cycle_days = 1
                        cumulative_net_amount = net_amount

                        rec['turn_start_date'] = rec['trade_date']
                        rec['turn_start_net_amount'] = cumulative_net_amount
                    else:
                        rec['turn_start_date'] = None
                        rec['turn_start_net_amount'] = 0
                else:
                    # 周期未超限，检查是否需要重置
                    need_reset = False

                    if pct_change > PCT_THRESHOLD:
                        # 遇到涨幅 > 5%，检查从启动日次日到该日前一日之间
                        # 是否存在某日净流出金额 > 启动日净流入金额的 0.3 倍
                        for j in range(cycle_start_idx + 1, i):
                            mid_net = records[j]['net_amount']
                            if mid_net < 0:  # 当日净流出
                                outflow_amount = abs(mid_net)
                                if cycle_start_net_amount > 0 and outflow_amount > cycle_start_net_amount * NET_OUTFLOW_RATIO:
                                    need_reset = True
                                    break

                    if need_reset:
                        # 重新开始新周期（以当前涨幅 > 5% 的日期为新的启动日）
                        in_cycle = True
                        cycle_start_idx = i
                        cycle_start_net_amount = net_amount
                        cycle_days = 1
                        cumulative_net_amount = net_amount

                        rec['turn_start_date'] = rec['trade_date']
                        rec['turn_start_net_amount'] = cumulative_net_amount
                    else:
                        # 正常累加
                        cycle_days += 1
                        cumulative_net_amount += net_amount

                        rec['turn_start_date'] = records[cycle_start_idx]['trade_date']
                        rec['turn_start_net_amount'] = cumulative_net_amount


def get_money_flow_service() -> MoneyFlowService:
    return MoneyFlowService.get_instance()
