import logging
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from stock_cache import get_stock_cache
from services.strategy_service import get_strategy_service
from routers import strategy_router, stock_info_router, data_router, config_router, calendar_router

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('server.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

stock_cache = get_stock_cache()

app = FastAPI(title="掘金量化竞价看板后端")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

strategy_service = get_strategy_service()

app.include_router(strategy_router)
app.include_router(stock_info_router)
app.include_router(data_router)
app.include_router(config_router)
app.include_router(calendar_router)


@app.get("/")
def index():
    logger.info("API health check called")
    return {"status": "运行中", "last_run": strategy_service.last_run_time}


if __name__ == "__main__":
    import uvicorn
    logger.info("Starting server...")
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
