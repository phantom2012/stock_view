import logging
from typing import List, Dict, Any
from fastapi import APIRouter

from services.auction_data_service import get_auction_data_service
from services.money_flow_service import get_money_flow_service

router = APIRouter(prefix="/api/data", tags=["数据加载"])

logger = logging.getLogger(__name__)
auction_data_service = get_auction_data_service()
money_flow_service = get_money_flow_service()


@router.post("/load-auction-data")
def load_auction_data(stocks: List[Dict[str, Any]], days: int = 30):
    return auction_data_service.load_auction_data(stocks, days)


@router.post("/load-money-flow")
def load_money_flow(stocks: List[Dict[str, Any]], days: int = 30):
    return money_flow_service.load_money_flow_data(stocks, days)


@router.post("/save-filter-stocks")
def save_filter_stocks(stocks: List[Dict[str, Any]]):
    return auction_data_service.save_filter_stocks(stocks)