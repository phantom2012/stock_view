from fastapi import APIRouter, Depends
from typing import List, Dict, Any
from datetime import datetime

from models.filter_params import FilterParams
from services.strategy_orchestrator import get_strategy_orchestrator

router = APIRouter(prefix="/api/strategy", tags=["策略"])

strategy_orchestrator = get_strategy_orchestrator()


@router.get("/refresh-exceed-list")
def api_refresh_exceed_list(params: FilterParams = Depends()):
    """刷新超预期列表（type=1 策略选股）"""
    return strategy_orchestrator.filter_type1_stocks(params)


@router.get("/get-exceed-list")
def get_exceed_list():
    """获取超预期列表（type=1）"""
    return _query_filter_results(filter_type=1)


@router.get("/refresh-filter-2-result")
def refresh_filter_2_result(params: FilterParams = Depends()):
    """刷新板块筛选结果（type=2）"""
    return strategy_orchestrator.filter_type2_stocks(params)


@router.get("/get-filter-2-result")
def get_filter_2_result():
    """获取板块筛选结果（type=2）"""
    return _query_filter_results(filter_type=2)


def _query_filter_results(filter_type: int) -> List[Dict[str, Any]]:
    from models import get_session_ro, StockDetail, FilterResult, StockMoneyFlow, StockScore, FilterStock
    try:
        today_date = datetime.now().strftime('%Y-%m-%d')

        with get_session_ro() as db:
            exclude_stocks = db.query(FilterStock).filter(
                FilterStock.is_exclude == 1,
                FilterStock.exclude_date >= today_date
            ).all()
            exclude_codes = {stock.code for stock in exclude_stocks}

        if filter_type == 1:
            with get_session_ro() as db:
                rows = db.query(FilterResult).filter(FilterResult.type == 1).all()

            score_map = {}
            money_flow_map = {}
            if rows:
                trade_dates = {row.trade_date for row in rows if row.trade_date}
                codes = {row.code for row in rows if row.code}
                if trade_dates and codes:
                    score_rows = db.query(StockScore).filter(
                        StockScore.code.in_(codes),
                        StockScore.trade_date.in_(trade_dates)
                    ).all()
                    for sr in score_rows:
                        score_map[(sr.code, sr.trade_date)] = sr

                    mf_rows = db.query(StockMoneyFlow).filter(
                        StockMoneyFlow.code.in_(codes),
                        StockMoneyFlow.trade_date.in_(trade_dates)
                    ).all()
                    for mf in mf_rows:
                        money_flow_map[(mf.code, mf.trade_date)] = {
                            'net_d5_amount': mf.net_d5_amount,
                            'turn_start_net_amount': mf.turn_start_net_amount,
                            'turn_start_net_amount_rate': mf.turn_start_net_amount_rate
                        }

            results = []
            for fr in rows:
                if fr.code in exclude_codes:
                    continue
                fr_dict = {c.name: getattr(fr, c.name) for c in fr.__table__.columns}

                sr = score_map.get((fr.code, fr.trade_date))
                if sr:
                    fr_dict['rising_wave_score'] = sr.rising_wave_score
                    exp_score = round(
                        (sr.interval_rise_score or 0)
                        + (sr.rising_wave_score or 0)
                        + (sr.turn_start_score or 0),
                        2
                    )
                else:
                    fr_dict['rising_wave_score'] = 0.0
                    exp_score = 0.0

                mf_data = money_flow_map.get((fr.code, fr.trade_date), {})
                fr_dict['net_d5_amount'] = mf_data.get('net_d5_amount')
                fr_dict['turn_start_net_amount'] = mf_data.get('turn_start_net_amount')
                fr_dict['turn_start_net_amount_rate'] = mf_data.get('turn_start_net_amount_rate')

                close_price = fr_dict.get('close_price', 0.0)
                next_close_price = fr_dict.get('next_close_price', 0.0)
                if close_price and close_price > 0 and next_close_price:
                    fr_dict['next_day_rise'] = round((next_close_price - close_price) / close_price * 100, 2)
                else:
                    fr_dict['next_day_rise'] = 0.0

                item = StockDetail.model_validate(fr_dict).model_dump()
                item['exp_score'] = exp_score
                results.append(item)
        else:
            with get_session_ro() as db:
                rows = db.query(FilterResult).filter(
                    FilterResult.type == filter_type
                ).order_by(FilterResult.interval_max_rise.desc()).all()

            results = []
            for row in rows:
                if row.code in exclude_codes:
                    continue
                results.append({
                    'code': row.code,
                    'stock_name': row.stock_name,
                    'interval_max_rise': row.interval_max_rise,
                    'max_day_rise': row.max_day_rise
                })

        return results
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"Error reading filter results (type={filter_type}) from database: {str(e)}")
        return []
