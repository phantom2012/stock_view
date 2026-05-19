"""
财务指标数据同步器
使用 Tushare fina_indicator_vip 接口按季度批量获取全市场财务数据
通过通知表触发（手动设置 trigger_flag=1），不设定时任务
"""
import logging
from datetime import datetime, timedelta
from typing import List, Tuple, Optional, Dict, Set

from sqlalchemy.exc import IntegrityError

from shared.db import get_session, get_session_ro, StockFinancial
from shared.stock_code_convert import to_pure_code
from external_data import get_query_handler
from .base_syncer import BaseSyncer

logger = logging.getLogger(__name__)

SYNC_TYPE = 'financial_data'
QUARTERS_TO_SYNC = 8


class FinancialDataSyncer(BaseSyncer):
    """
    财务指标数据同步器
    按季度批量查询全市场财务数据，增量写入 stock_financial 表

    同步逻辑：
    1. 计算最近4个季度的季度末日期（如 2025Q3, 2025Q4, 2026Q1, 2026Q2）
    2. 对每个季度，检查 stock_financial 表中是否有该季度的数据
    3. 如缺少，调用 fina_indicator_vip(period=季度末) 一次获取全市场数据
    4. 逐条写入，跳过已存在记录
    """

    def sync(self, stock_codes: Optional[List[str]] = None) -> Tuple[bool, int, int, str]:
        logger.info("===== 开始财务指标数据同步 =====")
        try:
            query_handler = get_query_handler()

            quarter_end_dates = self._calc_quarter_end_dates()
            logger.info(f"需要同步的季度: {quarter_end_dates}")

            total_saved = 0
            total_failed_quarters = 0
            total_skipped_quarters = 0

            for period in quarter_end_dates:
                try:
                    if self._has_quarter_data(period):
                        logger.info(f"  季度 {period} 数据已存在，跳过")
                        total_skipped_quarters += 1
                        continue

                    logger.info(f"  季度 {period}: 开始查询全市场财务数据...")
                    df = query_handler.get_fina_indicator_vip_data(period=period)

                    if df is None or df.empty:
                        logger.warning(f"  季度 {period}: 未返回数据")
                        total_failed_quarters += 1
                        continue

                    saved = self._save_quarter_records(df)
                    if saved > 0:
                        total_saved += saved
                        logger.info(f"  季度 {period}: 保存 {saved} 条记录")
                    else:
                        logger.info(f"  季度 {period}: 无新增记录")

                except Exception as e:
                    logger.error(f"  同步季度 {period} 失败: {e}")
                    total_failed_quarters += 1
                    continue

            logger.info("===== 财务指标数据同步完成 =====")
            logger.info(f"同步 {len(quarter_end_dates)} 个季度, 保存 {total_saved} 条, 跳过 {total_skipped_quarters} 个, 失败 {total_failed_quarters} 个")

            return True, total_saved, total_failed_quarters, f"同步{total_saved}条, 跳过{total_skipped_quarters}个季度"

        except Exception as e:
            logger.error(f"财务指标数据同步异常: {e}")
            import traceback; traceback.print_exc()
            return False, 0, 0, str(e)

    @staticmethod
    def _calc_quarter_end_dates() -> List[str]:
        """计算最近 N 个季度的季度末日期（如 20250630, 20250930, 20251231, 20260331）"""
        now = datetime.now()
        current_year = now.year
        current_month = now.month

        current_quarter = (current_month - 1) // 3 + 1

        end_dates = []
        for i in range(QUARTERS_TO_SYNC):
            q = current_quarter - i
            year = current_year
            while q <= 0:
                q += 4
                year -= 1

            if q == 1:
                month_day = '0331'
            elif q == 2:
                month_day = '0630'
            elif q == 3:
                month_day = '0930'
            else:
                month_day = '1231'

            end_dates.append(f"{year}{month_day}")

        return end_dates

    def _has_quarter_data(self, period: str) -> bool:
        """检查数据库中是否已有该季度的财务数据"""
        try:
            end_date_fmt = f"{period[:4]}-{period[4:6]}-{period[6:8]}"
            with get_session_ro() as db:
                count = db.query(StockFinancial).filter(
                    StockFinancial.end_date == end_date_fmt
                ).count()
                return count > 4000
        except Exception as e:
            logger.error(f"检查季度 {period} 数据失败: {e}")
            return False

    def _save_quarter_records(self, df) -> int:
        """批量保存一个季度的全市场财务数据"""
        saved_count = 0
        try:
            with get_session() as db:
                for _, row in df.iterrows():
                    ts_code = str(row.get('ts_code', ''))
                    end_date = str(row.get('end_date', ''))

                    if not ts_code or not end_date or len(end_date) < 8:
                        continue

                    code = to_pure_code(ts_code)
                    if not code:
                        continue

                    end_date_fmt = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}"

                    existing = db.query(StockFinancial).filter(
                        StockFinancial.code == code,
                        StockFinancial.end_date == end_date_fmt
                    ).first()
                    if existing:
                        continue

                    record = StockFinancial(
                        code=code,
                        end_date=end_date_fmt,
                        eps=self._safe_float(row.get('eps')),
                        dt_eps=self._safe_float(row.get('dt_eps')),
                        bps=self._safe_float(row.get('bps')),
                        ocfps=self._safe_float(row.get('ocfps')),
                        cfps=self._safe_float(row.get('cfps')),
                        total_revenue_ps=self._safe_float(row.get('total_revenue_ps')),
                        ebit_ps=self._safe_float(row.get('ebit_ps')),
                        roe=self._safe_float(row.get('roe')),
                        roe_waa=self._safe_float(row.get('roe_waa')),
                        roa=self._safe_float(row.get('roa')),
                        roic=self._safe_float(row.get('roic')),
                        grossprofit_margin=self._safe_float(row.get('grossprofit_margin')),
                        netprofit_margin=self._safe_float(row.get('netprofit_margin')),
                        npta=self._safe_float(row.get('npta')),
                        netprofit_yoy=self._safe_float(row.get('netprofit_yoy')),
                        basic_eps_yoy=self._safe_float(row.get('basic_eps_yoy')),
                        tr_yoy=self._safe_float(row.get('tr_yoy')),
                        or_yoy=self._safe_float(row.get('or_yoy')),
                        revenue_yoy=self._safe_float(row.get('revenue_yoy')),
                        assets_yoy=self._safe_float(row.get('assets_yoy')),
                        equity_yoy=self._safe_float(row.get('equity_yoy')),
                        op_income=self._safe_float(row.get('op_income')),
                        profit_dedt=self._safe_float(row.get('profit_dedt')),
                        ebit=self._safe_float(row.get('ebit')),
                        ebitda=self._safe_float(row.get('ebitda')),
                        debt_to_assets=self._safe_float(row.get('debt_to_assets')),
                        current_ratio=self._safe_float(row.get('current_ratio')),
                        quick_ratio=self._safe_float(row.get('quick_ratio')),
                        tangible_asset=self._safe_float(row.get('tangible_asset')),
                        capital_rese_ps=self._safe_float(row.get('capital_rese_ps')),
                        surplus_rese_ps=self._safe_float(row.get('surplus_rese_ps')),
                        undist_profit_ps=self._safe_float(row.get('undist_profit_ps')),
                        update_time=datetime.now(),
                    )
                    db.add(record)
                    saved_count += 1

                    if saved_count % 500 == 0:
                        db.flush()

                db.commit()
                return saved_count

        except IntegrityError:
            db.rollback()
            logger.warning("批量保存发生唯一键冲突，执行逐条降级保存...")
            return self._save_quarter_records_fallback(df)
        except Exception as e:
            logger.error(f"批量保存季度数据失败: {e}")
            return 0

    def _save_quarter_records_fallback(self, df) -> int:
        """逐条降级保存，冲突则跳过"""
        saved_count = 0
        with get_session() as db:
            for _, row in df.iterrows():
                ts_code = str(row.get('ts_code', ''))
                end_date = str(row.get('end_date', ''))
                if not ts_code or not end_date or len(end_date) < 8:
                    continue
                code = to_pure_code(ts_code)
                if not code:
                    continue
                end_date_fmt = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}"

                existing = db.query(StockFinancial).filter(
                    StockFinancial.code == code,
                    StockFinancial.end_date == end_date_fmt
                ).first()
                if existing:
                    continue

                record = StockFinancial(
                    code=code, end_date=end_date_fmt,
                    eps=self._safe_float(row.get('eps')),
                    dt_eps=self._safe_float(row.get('dt_eps')),
                    bps=self._safe_float(row.get('bps')),
                    ocfps=self._safe_float(row.get('ocfps')),
                    cfps=self._safe_float(row.get('cfps')),
                    total_revenue_ps=self._safe_float(row.get('total_revenue_ps')),
                    ebit_ps=self._safe_float(row.get('ebit_ps')),
                    roe=self._safe_float(row.get('roe')),
                    roe_waa=self._safe_float(row.get('roe_waa')),
                    roa=self._safe_float(row.get('roa')),
                    roic=self._safe_float(row.get('roic')),
                    grossprofit_margin=self._safe_float(row.get('grossprofit_margin')),
                    netprofit_margin=self._safe_float(row.get('netprofit_margin')),
                    npta=self._safe_float(row.get('npta')),
                    netprofit_yoy=self._safe_float(row.get('netprofit_yoy')),
                    basic_eps_yoy=self._safe_float(row.get('basic_eps_yoy')),
                    tr_yoy=self._safe_float(row.get('tr_yoy')),
                    or_yoy=self._safe_float(row.get('or_yoy')),
                    revenue_yoy=self._safe_float(row.get('revenue_yoy')),
                    assets_yoy=self._safe_float(row.get('assets_yoy')),
                    equity_yoy=self._safe_float(row.get('equity_yoy')),
                    op_income=self._safe_float(row.get('op_income')),
                    profit_dedt=self._safe_float(row.get('profit_dedt')),
                    ebit=self._safe_float(row.get('ebit')),
                    ebitda=self._safe_float(row.get('ebitda')),
                    debt_to_assets=self._safe_float(row.get('debt_to_assets')),
                    current_ratio=self._safe_float(row.get('current_ratio')),
                    quick_ratio=self._safe_float(row.get('quick_ratio')),
                    tangible_asset=self._safe_float(row.get('tangible_asset')),
                    capital_rese_ps=self._safe_float(row.get('capital_rese_ps')),
                    surplus_rese_ps=self._safe_float(row.get('surplus_rese_ps')),
                    undist_profit_ps=self._safe_float(row.get('undist_profit_ps')),
                    update_time=datetime.now(),
                )
                try:
                    db.add(record)
                    db.flush()
                    saved_count += 1
                except IntegrityError:
                    db.rollback()
            try:
                db.commit()
            except IntegrityError:
                db.rollback()
        return saved_count

    @staticmethod
    def _safe_float(value) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
