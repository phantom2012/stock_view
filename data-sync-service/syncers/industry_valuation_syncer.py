"""
行业估值基准同步器
1. 从 Tushare 获取所有上市股票的行业归属，写入 stock_industry 表
2. 批量获取全市场 daily_basic 数据，按行业统计 PE/PB/PS 中位数和均值
3. 写入 industry_valuation 表，用于个股估值评估的行业基准
"""
import logging
from datetime import datetime
from typing import List, Tuple, Optional
from statistics import median

import pandas as pd

from sqlalchemy.exc import IntegrityError

from shared.db import get_session, get_session_ro, StockIndustry, IndustryValuation
from shared.stock_code_convert import to_pure_code
from external_data import get_query_handler
from .base_syncer import BaseSyncer

logger = logging.getLogger(__name__)

SYNC_TYPE = 'industry_valuation'


class IndustryValuationSyncer(BaseSyncer):
    """
    行业估值基准同步器
    按通知表触发执行，同步全市场行业估值基准数据
    """

    def sync(self, stock_codes: Optional[List[str]] = None) -> Tuple[bool, int, int, str]:
        logger.info("===== 开始行业估值基准同步 =====")
        try:
            query_handler = get_query_handler()

            industry_df = query_handler.get_stock_industry_map()
            if industry_df is None or industry_df.empty:
                return False, 0, 0, "获取行业映射失败"

            saved_industries = self._save_stock_industry(industry_df)
            logger.info(f"股票行业映射: 保存 {saved_industries} 条记录")

            latest_trade_date = self._get_latest_trade_date(industry_df)
            if not latest_trade_date:
                return False, 0, 0, "无最新交易日数据"

            logger.info(f"开始获取 {latest_trade_date} 全市场估值数据...")

            basic_df = query_handler.get_daily_basic_batch_df(latest_trade_date)
            if basic_df is None or basic_df.empty:
                return False, 0, 0, "获取全市场估值数据失败"

            merged = self._merge_industry_data(basic_df, industry_df)
            if merged is None or merged.empty:
                return False, 0, 0, "合并行业数据为空"

            industry_stats = self._compute_industry_stats(merged)
            saved_stats = self._save_industry_valuation(industry_stats, latest_trade_date)

            logger.info(f"===== 行业估值基准同步完成 =====")
            logger.info(f"行业数: {len(industry_stats)}, 股票映射: {saved_industries}, 估值记录: {saved_stats}")

            return True, saved_stats, 0, f"同步{len(industry_stats)}个行业, {saved_stats}条记录"

        except Exception as e:
            logger.error(f"行业估值基准同步异常: {e}")
            import traceback; traceback.print_exc()
            return False, 0, 0, str(e)

    def _save_stock_industry(self, df: pd.DataFrame) -> int:
        """保存股票行业映射到 stock_industry 表"""
        count = 0
        with get_session() as db:
            for _, row in df.iterrows():
                ts_code = row.get('ts_code', '')
                name = row.get('name', '')
                industry = row.get('industry', '')

                if not ts_code or not industry:
                    continue

                code = to_pure_code(ts_code)
                existing = db.query(StockIndustry).filter(
                    StockIndustry.code == code
                ).first()

                if existing:
                    if existing.industry != industry or existing.name != name:
                        existing.industry = industry
                        existing.name = name
                        existing.update_time = datetime.now()
                else:
                    record = StockIndustry(
                        code=code,
                        name=name,
                        industry=industry,
                        update_time=datetime.now(),
                    )
                    db.add(record)
                    count += 1

            db.commit()
            total = db.query(StockIndustry).count()
            logger.info(f"stock_industry 表: 新增{count}条, 共{total}条")
            return total

    def _get_latest_trade_date(self, industry_df: pd.DataFrame) -> Optional[str]:
        """获取最新交易日（从行业数据中的date判断）"""
        return datetime.now().strftime('%Y%m%d')

    def _merge_industry_data(self, basic_df: pd.DataFrame, industry_df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """
        将 daily_basic 数据与行业映射合并
        给每只股票打上行业标签
        """
        try:
            basic_df['pure_code'] = basic_df['ts_code'].apply(
                lambda x: x.replace('.SH', '').replace('.SZ', '') if isinstance(x, str) else x
            )
            industry_df['pure_code'] = industry_df['ts_code'].apply(
                lambda x: x.replace('.SH', '').replace('.SZ', '') if isinstance(x, str) else x
            )

            merged = basic_df.merge(
                industry_df[['pure_code', 'industry', 'name']],
                on='pure_code', how='inner'
            )

            logger.info(f"合并后数据: {len(merged)}条, {merged['industry'].nunique()}个行业")
            return merged
        except Exception as e:
            logger.error(f"合并行业数据失败: {e}")
            return None

    def _compute_industry_stats(self, df: pd.DataFrame) -> List[dict]:
        """
        按行业统计 PE/PB/PS 的中位数和均值
        排除 PE<=0 和 PB<=0 的异常值
        """
        stats_list = []

        for industry_name, group in df.groupby('industry'):
            pe_values = group[(group['pe'].notna()) & (group['pe'] > 0) & (group['pe'] < 500)]['pe'].tolist()
            pe_ttm_values = group[(group['pe_ttm'].notna()) & (group['pe_ttm'] > 0) & (group['pe_ttm'] < 500)]['pe_ttm'].tolist()
            pb_values = group[(group['pb'].notna()) & (group['pb'] > 0) & (group['pb'] < 50)]['pb'].tolist()
            ps_values = group[(group['ps'].notna()) & (group['ps'] > 0) & (group['ps'] < 100)]['ps'].tolist()
            pcf_values = group[(group['pcf'].notna()) & (group['pcf'] > 0) & (group['pcf'] < 500)]['pcf'].tolist() if 'pcf' in group.columns else []

            stats = {
                'industry': industry_name,
                'stock_count': len(group),
            }

            if pe_values:
                stats['pe_median'] = round(median(pe_values), 2)
                stats['pe_mean'] = round(sum(pe_values) / len(pe_values), 2)
            if pe_ttm_values:
                stats['pe_ttm_median'] = round(median(pe_ttm_values), 2)
                stats['pe_ttm_mean'] = round(sum(pe_ttm_values) / len(pe_ttm_values), 2)
            if pb_values:
                stats['pb_median'] = round(median(pb_values), 2)
                stats['pb_mean'] = round(sum(pb_values) / len(pb_values), 2)
            if ps_values:
                stats['ps_median'] = round(median(ps_values), 2)
                stats['ps_mean'] = round(sum(ps_values) / len(ps_values), 2)
            if pcf_values:
                stats['pcf_median'] = round(median(pcf_values), 2)
                stats['pcf_mean'] = round(sum(pcf_values) / len(pcf_values), 2)

            stats_list.append(stats)

        stats_list.sort(key=lambda x: x['industry'])
        logger.info(f"统计完成: {len(stats_list)}个行业")

        return stats_list

    def _save_industry_valuation(self, stats_list: List[dict], trade_date: str) -> int:
        """保存行业估值基准到 industry_valuation 表"""
        saved = 0
        with get_session() as db:
            for stats in stats_list:
                trade_date_fmt = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:8]}"
                industry_name = stats['industry']

                existing = db.query(IndustryValuation).filter(
                    IndustryValuation.industry == industry_name,
                    IndustryValuation.trade_date == trade_date_fmt
                ).first()

                if existing:
                    existing.pe_median = stats.get('pe_median')
                    existing.pe_mean = stats.get('pe_mean')
                    existing.pe_ttm_median = stats.get('pe_ttm_median')
                    existing.pe_ttm_mean = stats.get('pe_ttm_mean')
                    existing.pb_median = stats.get('pb_median')
                    existing.pb_mean = stats.get('pb_mean')
                    existing.ps_median = stats.get('ps_median')
                    existing.ps_mean = stats.get('ps_mean')
                    existing.pcf_median = stats.get('pcf_median')
                    existing.pcf_mean = stats.get('pcf_mean')
                    existing.stock_count = stats.get('stock_count')
                    existing.update_time = datetime.now()
                else:
                    record = IndustryValuation(
                        industry=industry_name,
                        trade_date=trade_date_fmt,
                        pe_median=stats.get('pe_median'),
                        pe_mean=stats.get('pe_mean'),
                        pe_ttm_median=stats.get('pe_ttm_median'),
                        pe_ttm_mean=stats.get('pe_ttm_mean'),
                        pb_median=stats.get('pb_median'),
                        pb_mean=stats.get('pb_mean'),
                        ps_median=stats.get('ps_median'),
                        ps_mean=stats.get('ps_mean'),
                        pcf_median=stats.get('pcf_median'),
                        pcf_mean=stats.get('pcf_mean'),
                        stock_count=stats.get('stock_count'),
                        update_time=datetime.now(),
                    )
                    db.add(record)
                saved += 1

            try:
                db.commit()
            except IntegrityError:
                db.rollback()
                logger.warning("批量保存行业估值发生冲突，逐条保存...")
                saved = 0
                for stats in stats_list:
                    try:
                        existing = db.query(IndustryValuation).filter(
                            IndustryValuation.industry == stats['industry'],
                            IndustryValuation.trade_date == trade_date_fmt
                        ).first()
                        if not existing:
                            record = IndustryValuation(
                                industry=stats['industry'],
                                trade_date=trade_date_fmt,
                                **{k: stats.get(k) for k in
                                   ['pe_median','pe_mean','pe_ttm_median','pe_ttm_mean',
                                    'pb_median','pb_mean','ps_median','ps_mean',
                                    'pcf_median','pcf_mean','stock_count']},
                                update_time=datetime.now(),
                            )
                            db.add(record)
                            db.flush()
                            saved += 1
                    except IntegrityError:
                        db.rollback()
                db.commit()

        return saved
