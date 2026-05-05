import logging
from typing import List
from fastapi import APIRouter
from shared.db import get_session_ro, TradeCalendar

router = APIRouter(prefix="/api/calendar", tags=["交易日历"])

logger = logging.getLogger(__name__)


@router.get("/non-trading-dates")
def get_non_trading_dates() -> List[str]:
    """
    获取所有非交易日日期列表

    Returns:
        非交易日日期字符串列表，格式 ['2026-01-01', '2026-01-02', ...]
    """
    try:
        with get_session_ro() as db:
            rows = db.query(TradeCalendar.calendar_date).filter(
                TradeCalendar.is_trading_day == 0
            ).order_by(TradeCalendar.calendar_date).all()
            return [row[0] for row in rows if row[0]]
    except Exception as e:
        logger.error(f"获取非交易日失败: {e}")
        return []
