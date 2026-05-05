"""
股票基础信息同步器
从 backend/tasks/stock_info_sync_task.py 迁移
检查 stock_info 表中 free_share 字段是否为空或0，调用 daily_basic 接口补充
同时更新 list_status、list_date、delist_date 字段
"""
import logging
from datetime import datetime, date as dt_date
from sqlalchemy import Date
from typing import List, Tuple

from shared.db import get_session, get_session_ro, FilterResult, StockInfo
from shared.stock_code_convert import to_tushare_ts_code, to_pure_code
from shared.trade_date_util import TradeDateUtil
from external_data import get_query_handler
from .base_syncer import BaseSyncer
from utils.log_utils import log_progress

logger = logging.getLogger(__name__)

trade_date_util = TradeDateUtil()


class StockInfoSyncer(BaseSyncer):
    """
    股票流通股本信息同步器
    每天下午4点执行一次，更新所有股票信息
    """

    def sync(self, stock_codes=None) -> Tuple[bool, int, int, str]:
        logger.info("===== 开始股票基础信息同步 =====")
        try:
            query_handler = get_query_handler()

            # 获取股票基本信息（已包含所有状态，list_status 已在接口内部显式设置）
            logger.info(f"获取股票基本信息（所有状态）")
            instruments_df = query_handler.get_instruments(list_status=None)
            if instruments_df is None or instruments_df.empty:
                return False, 0, 0, "获取股票基本信息失败"

            # 构建股票基本信息字典
            stock_basic_data = {}
            for _, row in instruments_df.iterrows():
                pure_code = to_pure_code(row['symbol'])
                stock_basic_data[pure_code] = {
                    'name': row.get('sec_name', ''),
                    'exchange': row.get('exchange', ''),
                    'list_status': row.get('list_status', ''),
                    'list_date': self._format_date(row.get('list_date', '')),
                    'delist_date': self._format_date(row.get('delist_date', '')),
                }

            logger.info(f"获取股票基本信息成功，共 {len(stock_basic_data)} 只股票")

            # 获取每日基本面数据（用于更新 free_share 等字段）
            latest_trade_date = trade_date_util.get_latest_trade_date()
            if not latest_trade_date:
                return False, 0, 0, "获取最新交易日失败"

            logger.info(f"批量查询全市场每日基本面数据，日期: {latest_trade_date}")
            all_stock_data = query_handler.get_daily_basic_data(trade_date=latest_trade_date)
            if all_stock_data is None:
                return False, 0, 0, "批量查询全市场数据失败"

            logger.info(f"批量查询全市场基本面数据成功，获取到 {len(all_stock_data)} 只股票的数据")

            # 遍历所有股票信息，进行插入或更新
            total_stocks = len(stock_basic_data)
            updated_count = 0
            failed_count = 0

            for idx, (code, basic_info) in enumerate(stock_basic_data.items(), 1):
                try:
                    ts_code = to_tushare_ts_code(code)
                    daily_data = all_stock_data.get(ts_code)

                    success = self._update_stock_info(
                        code=code,
                        basic_info=basic_info,
                        daily_data=daily_data
                    )
                    if success:
                        updated_count += 1
                        log_progress(f"进度: [{idx}/{total_stocks}] 已处理 {updated_count} 只股票", idx, total_stocks)
                    else:
                        failed_count += 1

                except Exception as e:
                    logger.error(f"  处理 {code} 失败: {e}")
                    failed_count += 1
                    continue

            logger.info("===== 股票基础信息同步完成 =====")
            logger.info(f"总股票数: {total_stocks}, 已更新: {updated_count}, 失败: {failed_count}")
            return True, updated_count, failed_count, f"更新{updated_count}只, 失败{failed_count}只"

        except Exception as e:
            logger.error(f"股票基础信息同步异常: {e}")
            import traceback; traceback.print_exc()
            return False, 0, 0, str(e)

    def _format_date(self, date_str: str) -> str:
        """格式化日期，将 YYYYMMDD 转换为 YYYY-MM-DD"""
        if not date_str:
            return ''
        date_str = str(date_str)
        if len(date_str) == 8 and '-' not in date_str:
            return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        return date_str

    def _update_stock_info(self, code: str, basic_info: dict, daily_data: dict = None) -> bool:
        try:
            with get_session() as db:
                stock_info = db.query(StockInfo).filter(StockInfo.code == code).first()

                if stock_info:
                    # 更新股票基本信息
                    if basic_info:
                        if basic_info.get('name'):
                            stock_info.name = basic_info['name']
                        if basic_info.get('exchange'):
                            stock_info.exchange = basic_info['exchange']
                        # 上市状态 'L' 是有效的，不能用 if 判断（非空字符串 'L' 为真，但空字符串为假）
                        list_status_val = basic_info.get('list_status')
                        if list_status_val is not None:
                            stock_info.list_status = list_status_val
                        if basic_info.get('list_date'):
                            stock_info.list_date = basic_info['list_date']
                        # 退市日期可能为空（未退市），但如果有值就更新
                        delist_date_val = basic_info.get('delist_date')
                        if delist_date_val is not None:
                            stock_info.delist_date = delist_date_val

                    # 更新每日基本面数据
                    if daily_data:
                        free_share = daily_data.get('free_share')
                        close = daily_data.get('close')
                        circ_mv = daily_data.get('circ_mv')

                        if free_share is not None:
                            stock_info.free_share = float(free_share)
                        if circ_mv is not None:
                            stock_info.circ_mv = float(circ_mv)
                        elif free_share is not None and close is not None:
                            stock_info.circ_mv = float(free_share) * float(close)

                    stock_info.update_time = datetime.now()
                else:
                    # 新增股票记录
                    free_share = float(daily_data.get('free_share')) if daily_data and daily_data.get('free_share') else 0
                    circ_mv = float(daily_data.get('circ_mv')) if daily_data and daily_data.get('circ_mv') else (
                        free_share * float(daily_data.get('close')) if daily_data and daily_data.get('close') else 0
                    )

                    db.add(StockInfo(
                        code=code,
                        name=basic_info.get('name', '未知'),
                        exchange=basic_info.get('exchange', ''),
                        free_share=free_share,
                        circ_mv=circ_mv,
                        list_status=basic_info.get('list_status', ''),
                        list_date=basic_info.get('list_date', ''),
                        delist_date=basic_info.get('delist_date', ''),
                        need_sync=1,
                        update_time=datetime.now()
                    ))
                db.commit()
                return True
        except Exception as e:
            logger.error(f"更新 {code} 失败: {e}")
            return False
