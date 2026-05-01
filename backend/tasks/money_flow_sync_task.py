"""
资金流向数据同步任务
每5分钟执行一次，扫描 filter_results 表中所有股票代码，
检查 stock_money_flow 表中最近30个交易日的数据是否完整，
如有缺失则调用外部接口补充

逻辑：
1. 从 filter_results 表获取所有股票 code
2. 获取最近30个交易日列表
3. 对每只股票，查询 stock_money_flow 表中已有的交易日
4. 找出缺失的交易日（已有数据的不查，只查缺失的）
5. 如果缺失日期是连续的（从最早缺失日到最新缺失日），则调用一次接口批量获取
6. 将获取到的数据 upsert 入库
"""
import logging
from datetime import datetime
from typing import List, Optional, Set

from models import FilterResult, StockMoneyFlow, get_session, get_session_ro
from baostock_data.trade_date_util import TradeDateUtil
from external_data.ext_data_query_handle import get_query_handler
from common.stock_code_convert import to_goldminer_symbol
from common.db_utils import upsert_by_unique_keys

logger = logging.getLogger(__name__)

trade_date_util = TradeDateUtil()

# 扫描最近多少个交易日
SCAN_DAYS = 30


def _get_filter_stock_codes() -> List[str]:
    """
    从 filter_results 表获取所有股票代码

    Returns:
        List[str]: 股票代码列表（纯数字格式）
    """
    try:
        with get_session_ro() as db:
            rows = db.query(FilterResult.code).distinct().all()
            codes = [row[0] for row in rows if row[0]]
            logger.info(f"从 filter_results 获取到 {len(codes)} 个股票代码")
            return codes
    except Exception as e:
        logger.error(f"获取 filter_results 股票代码失败: {e}")
        return []


def _get_existing_trade_dates(code: str) -> Set[str]:
    """
    查询某只股票在 stock_money_flow 表中已有的交易日

    Args:
        code: 股票代码

    Returns:
        Set[str]: 已有的交易日集合，格式 {'YYYY-MM-DD', ...}
    """
    try:
        with get_session_ro() as db:
            rows = db.query(StockMoneyFlow.trade_date).filter(
                StockMoneyFlow.code == code
            ).all()
            return {row[0] for row in rows if row[0]}
    except Exception as e:
        logger.error(f"查询 {code} 已有交易日失败: {e}")
        return set()


def _get_missing_trade_dates(code: str, all_trade_dates: List[str]) -> List[str]:
    """
    获取某只股票缺失的交易日列表

    Args:
        code: 股票代码
        all_trade_dates: 最近30个交易日列表

    Returns:
        List[str]: 缺失的交易日列表（已按日期排序）
    """
    existing = _get_existing_trade_dates(code)
    missing = [d for d in all_trade_dates if d not in existing]
    return missing


def _save_money_flow_records(code: str, df, name: str = "") -> int:
    """
    将 DataFrame 格式的资金流向数据保存到数据库

    Args:
        code: 股票代码
        df: 资金流向数据 DataFrame
        name: 股票名称

    Returns:
        int: 保存的记录数
    """
    if df is None or df.empty:
        return 0

    # 按日期升序排序
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

    # 入库
    saved_count = 0
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
            saved_count += 1

    return saved_count


def sync_money_flow_data() -> bool:
    """
    资金流向数据同步主函数
    每5分钟执行一次，增量补充缺失数据

    Returns:
        bool: 执行成功返回True
    """
    logger.info("===== 开始资金流向数据同步任务 =====")

    try:
        # 1. 获取所有需要同步的股票代码
        stock_codes = _get_filter_stock_codes()
        if not stock_codes:
            logger.warning("filter_results 表中没有股票数据，跳过同步")
            return True

        # 2. 获取最近30个交易日
        all_trade_dates = trade_date_util.get_recent_trade_dates(SCAN_DAYS)
        if not all_trade_dates:
            logger.error("获取交易日列表失败")
            return False

        logger.info(f"最近 {SCAN_DAYS} 个交易日: {all_trade_dates[0]} ~ {all_trade_dates[-1]}")

        # 3. 获取外部数据查询器
        query_handler = get_query_handler()

        total_stocks = len(stock_codes)
        total_missing = 0
        total_saved = 0
        failed_stocks = 0

        # 4. 遍历每只股票，检查并补充缺失数据
        for idx, code in enumerate(stock_codes, 1):
            try:
                # 4.1 找出缺失的交易日
                missing_dates = _get_missing_trade_dates(code, all_trade_dates)
                if not missing_dates:
                    continue

                total_missing += len(missing_dates)
                logger.info(f"[{idx}/{total_stocks}] {code}: 缺失 {len(missing_dates)} 个交易日数据")

                # 4.2 确定需要查询的日期范围（从最早缺失日到最晚缺失日）
                start_date = missing_dates[0]
                end_date = missing_dates[-1]

                # 4.3 调用外部接口获取数据
                symbol = to_goldminer_symbol(code)
                logger.info(f"  查询外部接口: {symbol}, {start_date} ~ {end_date}")

                df = query_handler.get_money_flow_data(
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date
                )

                if df is None or df.empty:
                    logger.warning(f"  外部接口未返回 {code} 的数据")
                    failed_stocks += 1
                    continue

                # 4.4 保存到数据库
                saved = _save_money_flow_records(code, df)
                total_saved += saved
                logger.info(f"  成功保存 {saved} 条记录")

            except Exception as e:
                logger.error(f"  同步 {code} 失败: {e}")
                failed_stocks += 1
                continue

        # 5. 汇总结果
        logger.info("===== 资金流向数据同步完成 =====")
        logger.info(f"总股票数: {total_stocks}")
        logger.info(f"总缺失数据项: {total_missing}")
        logger.info(f"成功保存: {total_saved}")
        logger.info(f"失败股票数: {failed_stocks}")

        return True

    except Exception as e:
        logger.error(f"资金流向数据同步异常: {e}")
        import traceback
        traceback.print_exc()
        return False
