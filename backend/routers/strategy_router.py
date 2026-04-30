from fastapi import APIRouter, Depends
from typing import List, Dict, Any

from models.filter_params import FilterParams
from services.strategy_service import get_strategy_service
from services.stock_filter_service import get_stock_filter_service

router = APIRouter(prefix="/api/strategy", tags=["策略"])

strategy_service = get_strategy_service()
stock_filter_service = get_stock_filter_service()


@router.get("/refresh-exceed-list")
def api_refresh_exceed_list(params: FilterParams = Depends()):
    return strategy_service.run_strategy(params)


@router.get("/get-exceed-list")
def get_exceed_list():
    return _query_filter_results(filter_type=1)


@router.get("/refresh-filter-2-result")
def refresh_filter_2_result(params: FilterParams = Depends()):
    return stock_filter_service.filter_stocks(params)


@router.get("/get-filter-2-result")
def get_filter_2_result():
    return _query_filter_results(filter_type=2)


def _query_filter_results(filter_type: int) -> List[Dict[str, Any]]:
    from models import get_session_ro, StockResult, FilterResult, StockMoneyFlow
    try:
        if filter_type == 1:
            with get_session_ro() as db:
                rows = db.query(FilterResult).filter(FilterResult.type == 1).all()

            money_flow_map = {}
            if rows:
                trade_dates = {row.trade_date for row in rows if row.trade_date}
                codes = {row.code for row in rows if row.code}
                if trade_dates and codes:
                    mf_rows = db.query(StockMoneyFlow).filter(
                        StockMoneyFlow.code.in_(codes),
                        StockMoneyFlow.trade_date.in_(trade_dates)
                    ).all()
                    for mf in mf_rows:
                        money_flow_map[(mf.code, mf.trade_date)] = {
                            'net_d5_amount': mf.net_d5_amount,
                            'turn_start_net_amount': mf.turn_start_net_amount
                        }

            results = []
            for fr in rows:
                fr_dict = {c.name: getattr(fr, c.name) for c in fr.__table__.columns}
                fr_dict['exp_score'] = fr_dict.get('rising_wave_score', 0.0)
                mf_data = money_flow_map.get((fr.code, fr.trade_date), {})
                fr_dict['net_d5_amount'] = mf_data.get('net_d5_amount')
                fr_dict['turn_start_net_amount'] = mf_data.get('turn_start_net_amount')

                close_price = fr_dict.get('close_price', 0.0)
                next_close_price = fr_dict.get('next_close_price', 0.0)
                if close_price and close_price > 0 and next_close_price:
                    fr_dict['next_day_rise'] = round((next_close_price - close_price) / close_price * 100, 2)
                else:
                    fr_dict['next_day_rise'] = 0.0

                results.append(StockResult.model_validate(fr_dict).model_dump())
        else:
            with get_session_ro() as db:
                rows = db.query(FilterResult).filter(
                    FilterResult.type == filter_type
                ).order_by(FilterResult.interval_max_rise.desc()).all()

            results = [
                {
                    'code': row.code,
                    'name': row.stock_name,
                    'interval_max_rise': row.interval_max_rise,
                    'max_day_rise': row.max_day_rise
                }
                for row in rows
            ]

        return results
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"Error reading filter results (type={filter_type}) from database: {str(e)}")
        return []
