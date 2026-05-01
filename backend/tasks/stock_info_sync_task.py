"""
股票基础信息同步任务
每5分钟执行一次，扫描 filter_results 表中所有股票代码，
检查 stock_info 表中 free_share（流通股本）字段是否为空或0，
如果为空则调用 daily_basic 接口查询并更新 free_share 和 circ_mv（流通市值）字段

逻辑：
1. 从 filter_results 表获取所有股票 code
2. 对每只股票，检查 stock_info 表中 free_share 是否为空或0
3. 如果为空或0，则调用 get_daily_basic_data 接口获取最新数据
4. 将获取到的 free_share 和 circ_mv 更新到 stock_info 表
"""
import logging
from datetime import datetime
from typing import List

from models import FilterResult, StockInfo, get_session, get_session_ro
from external_data.ext_data_query_handle import get_query_handler
from common.stock_code_convert import to_goldminer_symbol

logger = logging.getLogger(__name__)


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


def _needs_update(code: str) -> bool:
    """
    检查某只股票是否需要更新 free_share 数据

    Args:
        code: 股票代码

    Returns:
        bool: True表示需要更新，False表示已有数据
    """
    try:
        with get_session_ro() as db:
            stock_info = db.query(StockInfo).filter(StockInfo.code == code).first()
            if stock_info:
                # 检查 free_share 是否为空或0
                if stock_info.free_share is None or stock_info.free_share <= 0:
                    logger.debug(f"{code}: free_share={stock_info.free_share}, 需要更新")
                    return True
                else:
                    logger.debug(f"{code}: free_share={stock_info.free_share}, 已有数据，无需更新")
                    return False
            else:
                logger.debug(f"{code}: stock_info 表中不存在该股票")
                return True
    except Exception as e:
        logger.error(f"检查 {code} 是否需要更新失败: {e}")
        return False


def _update_stock_info(code: str, free_share: float, circ_mv: float) -> bool:
    """
    更新 stock_info 表中的 free_share 和 circ_mv 字段

    Args:
        code: 股票代码
        free_share: 自由流通股本（万股）
        circ_mv: 流通市值（万元）

    Returns:
        bool: 更新成功返回True
    """
    try:
        with get_session() as db:
            stock_info = db.query(StockInfo).filter(StockInfo.code == code).first()

            if stock_info:
                # 更新现有记录
                stock_info.free_share = free_share
                stock_info.circ_mv = circ_mv
                stock_info.update_time = datetime.now()
                db.commit()
                logger.debug(f"{code}: 更新成功 - free_share={free_share:.2f}万, circ_mv={circ_mv:.2f}万")
            else:
                # 创建新记录
                new_stock_info = StockInfo(
                    code=code,
                    name='',
                    exchange='',
                    free_share=free_share,
                    circ_mv=circ_mv,
                    update_time=datetime.now()
                )
                db.add(new_stock_info)
                db.commit()
                logger.debug(f"{code}: 创建新记录 - free_share={free_share:.2f}万, circ_mv={circ_mv:.2f}万")

            return True
    except Exception as e:
        logger.error(f"更新 {code} 失败: {e}")
        return False


def sync_stock_info_free_share() -> bool:
    """
    股票流通股本信息同步主函数
    每5分钟执行一次，检查并更新缺失的 free_share 数据

    Returns:
        bool: 执行成功返回True
    """
    logger.info("===== 开始股票流通股本信息同步任务 =====")

    try:
        # 1. 获取所有需要同步的股票代码
        stock_codes = _get_filter_stock_codes()
        if not stock_codes:
            logger.warning("filter_results 表中没有股票数据，跳过同步")
            return True

        # 2. 获取外部数据查询器
        query_handler = get_query_handler()

        total_stocks = len(stock_codes)
        updated_count = 0
        skipped_count = 0
        failed_count = 0

        # 3. 遍历每只股票，检查并更新数据
        for idx, code in enumerate(stock_codes, 1):
            try:
                # 3.1 检查是否需要更新
                if not _needs_update(code):
                    skipped_count += 1
                    continue

                logger.info(f"[{idx}/{total_stocks}] {code}: 需要更新流通股本信息")

                # 3.2 调用外部接口获取数据（不传日期，获取最新数据）
                symbol = to_goldminer_symbol(code)
                logger.debug(f"  查询外部接口: {symbol}")

                data = query_handler.get_daily_basic_data(symbol=symbol)

                if data is None:
                    logger.warning(f"  外部接口未返回 {code} 的数据")
                    failed_count += 1
                    continue

                # 3.3 提取需要的字段
                free_share = data.get('free_share')
                close = data.get('close')

                # 通过计算得出流通市值（万元）= 流通股数（万股）× 收盘价（元）
                if free_share is not None and close is not None:
                    circ_mv = free_share * close
                else:
                    circ_mv = None

                if free_share is None or free_share <= 0:
                    logger.warning(f"  获取到的 free_share 无效: {free_share}")
                    failed_count += 1
                    continue

                # 3.4 更新到数据库
                success = _update_stock_info(code, free_share, circ_mv)
                if success:
                    updated_count += 1
                    logger.info(f"  更新成功: free_share={free_share:.2f}万股, circ_mv={circ_mv:.2f}万元")
                else:
                    failed_count += 1

            except Exception as e:
                logger.error(f"  同步 {code} 失败: {e}")
                failed_count += 1
                continue

        # 4. 汇总结果
        logger.info("===== 股票流通股本信息同步完成 =====")
        logger.info(f"总股票数: {total_stocks}")
        logger.info(f"已更新: {updated_count}")
        logger.info(f"跳过(已有数据): {skipped_count}")
        logger.info(f"失败: {failed_count}")

        return True

    except Exception as e:
        logger.error(f"股票流通股本信息同步异常: {e}")
        import traceback
        traceback.print_exc()
        return False
