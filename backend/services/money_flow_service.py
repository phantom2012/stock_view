import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

from models import StockMoneyFlow, get_session
from baostock_data.trade_date_util import TradeDateUtil
from external_data.ext_data_query_handle import get_query_handler

logger = logging.getLogger(__name__)

trade_date_util = TradeDateUtil()


class MoneyFlowService:
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
                    # 转换为掘金格式的symbol
                    if code.startswith('6'):
                        symbol = f"SHSE.{code}"
                    else:
                        symbol = f"SZSE.{code}"

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
        """保存资金流向数据到数据库（存在则更新，不存在则插入）"""
        with get_session() as db:
            for _, row in df.iterrows():
                trade_date = str(row.get('trade_date', ''))
                if len(trade_date) == 8:
                    trade_date = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:8]}"

                # 先查找是否存在相同记录
                existing = db.query(StockMoneyFlow).filter(
                    StockMoneyFlow.code == code,
                    StockMoneyFlow.trade_date == trade_date
                ).first()

                # 更新字段值
                update_data = {
                    'name': row.get('name', name),
                    'pct_change': row.get('pct_change', 0),
                    'latest': row.get('latest', 0),
                    'net_amount': row.get('net_amount', 0),
                    'net_d5_amount': row.get('net_d5_amount', 0),
                    'buy_lg_amount': row.get('buy_lg_amount', 0),
                    'buy_lg_amount_rate': row.get('buy_lg_amount_rate', 0),
                    'buy_md_amount': row.get('buy_md_amount', 0),
                    'buy_md_amount_rate': row.get('buy_md_amount_rate', 0),
                    'buy_sm_amount': row.get('buy_sm_amount', 0),
                    'buy_sm_amount_rate': row.get('buy_sm_amount_rate', 0),
                    'update_time': datetime.now()
                }

                if existing:
                    # 更新现有记录
                    for key, value in update_data.items():
                        setattr(existing, key, value)
                else:
                    # 插入新记录
                    record = StockMoneyFlow(code=code, trade_date=trade_date, **update_data)
                    db.add(record)

            db.commit()
            logger.info(f"成功保存 {len(df)} 条资金流向数据到数据库")


_money_flow_service: Optional[MoneyFlowService] = None


def get_money_flow_service() -> MoneyFlowService:
    global _money_flow_service
    if _money_flow_service is None:
        _money_flow_service = MoneyFlowService()
    return _money_flow_service
