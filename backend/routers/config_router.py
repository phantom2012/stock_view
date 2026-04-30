import logging
from typing import List, Dict, Any
from fastapi import APIRouter

from models import get_session_ro, BlockInfo, FilterConfig

router = APIRouter(prefix="/api/config", tags=["配置"])

logger = logging.getLogger(__name__)


@router.get("/get-block-list")
def get_block_list():
    try:
        with get_session_ro() as db:
            rows = db.query(BlockInfo).order_by(BlockInfo.block_code).all()

            blocks = [
                {
                    'code': row.block_code,
                    'name': row.block_name
                }
                for row in rows
            ]

            return blocks
    except Exception as e:
        logger.error(f"Error in get-block-list: {str(e)}")
        import traceback
        traceback.print_exc()
        return []


@router.get("/get-filter-config")
def get_filter_config(config_type: int = 2):
    try:
        with get_session_ro() as db:
            row = db.query(FilterConfig).filter(FilterConfig.type == config_type).first()

            if row:
                return {c.name: getattr(row, c.name) for c in row.__table__.columns}
            return None
    except Exception as e:
        logger.error(f"Error reading filter config: {str(e)}")
        return None
