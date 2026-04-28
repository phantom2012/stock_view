import os
import pandas as pd
import logging
from datetime import datetime
from typing import List, Dict, Any
from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler

from stock_cache import get_stock_cache
from services.strategy_service import get_strategy_service
from services.stock_filter_service import get_stock_filter_service
from services.auction_data_service import get_auction_data_service
from models.filter_params import FilterParams
from models.db_models.filter_result import FilterResult
from models import get_db, StockResult
from stock_sqlite.database import get_db_cursor
from common.stock_code_convert import to_goldminer_symbol
from gm.api import get_instruments

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
stock_cache.set_api_token("2e664976b46df6a0903672349c30226ac68e7bf3")

app = FastAPI(title="掘金量化竞价看板后端")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

scheduler = BackgroundScheduler(timezone="Asia/Shanghai")

strategy_service = get_strategy_service()
stock_filter_service = get_stock_filter_service()
auction_data_service = get_auction_data_service()


def run_strategy_task():
    strategy_service.run_strategy()

scheduler.add_job(run_strategy_task, "cron", hour=9, minute=25)
try:
    scheduler.start()
    logger.info("Scheduler started")
except Exception as e:
    logger.error(f"Error starting scheduler: {str(e)}")

# 筛选超预期策略股票
@app.get("/refresh-exceed-list")
def api_refresh_exceed_list(params: FilterParams = Depends()):
    logger.info(f"API refresh-exceed-list called with {params.model_dump()}")

    # 保存筛选配置到filter_config表 (type=1)
    stock_filter_service.update_filter_config(
        config_type=1,
        params=params,
        trade_date=params.trade_date
    )

    return strategy_service.run_strategy(params)


def _query_filter_results(filter_type: int) -> List[Dict[str, Any]]:
    """
    通用方法：从 filter_results 表查询筛选结果

    Args:
        filter_type: 筛选类型（1=竞价超预期选股, 2=普通筛选）

    Returns:
        结果字典列表
    """
    try:
        if filter_type == 1:
            db = next(get_db())
            try:
                rows = db.query(FilterResult).filter(FilterResult.type == 1).order_by(FilterResult.higher_score.desc()).all()
            finally:
                db.close()

            results = []
            for fr in rows:
                fr_dict = {c.name: getattr(fr, c.name) for c in fr.__table__.columns}
                results.append(StockResult.model_validate(fr_dict).model_dump())
        else:
            with get_db_cursor() as cursor:
                cursor.execute("""
                    SELECT code, stock_name, interval_max_rise, max_day_rise
                    FROM filter_results
                    WHERE type = ?
                    ORDER BY interval_max_rise DESC
                """, (filter_type,))
                rows = cursor.fetchall()

            results = [
                {
                    'code': row[0],
                    'name': row[1],
                    'interval_max_rise': row[2],
                    'max_day_rise': row[3]
                }
                for row in rows
            ]

        logger.info(f"Returning {len(results)} rows from filter_results (type={filter_type})")
        return results
    except Exception as e:
        logger.error(f"Error reading filter results (type={filter_type}) from database: {str(e)}")
        return []


@app.get("/get-exceed-list")
def get_exceed_list():
    logger.info("API get-exceed-list called")
    return _query_filter_results(filter_type=1)


@app.get("/get-trade-dates")
def get_trade_dates():
    logger.info("API get-trade-dates called")
    csv_path = os.path.join(os.path.dirname(__file__), 'baostock_data', 'a_stock_trade_days_2026.csv')
    if not os.path.exists(csv_path):
        logger.warning(f"Trade dates CSV not found: {csv_path}")
        return []

    try:
        df = pd.read_csv(csv_path, encoding="utf-8")
        trade_days = df["trade_date"].tolist()
        logger.info(f"Returning {len(trade_days)} trade dates")
        return trade_days
    except Exception as e:
        logger.error(f"Error reading trade dates: {str(e)}")
        return []


@app.get("/")
def index():
    logger.info("API health check called")
    return {"status": "运行中", "last_run": strategy_service.last_run_time}


def _build_default_stock_info(code: str) -> Dict[str, Any]:
    return {
        'code': code,
        'stock_name': '未知股票',
        'today_gain': '-',
        'open_price': '-',
        'open_volume': '-',
        'price_diff': '-',
        'interval_max_rise': '-',
        'max_day_rise': '-',
        'next_day_gain': '-',
        'trade_date': datetime.now().strftime('%Y-%m-%d'),
        'higher_score': '-',
        'rising_wave_score': '-',
        'weipan_exceed': False,
        'zaopan_exceed': False,
        'rising_wave': False
    }


@app.get("/get-stock-info")
def get_stock_info(code: str):
    logger.info(f"API get-stock-info called with code={code}")

    result = _build_default_stock_info(code)

    try:
        symbol = to_goldminer_symbol(code)

        stock_name = stock_cache.get_stock_name(symbol)

        if stock_name == '未知':
            try:
                inst = get_instruments(symbols=[symbol], df=True)
                if inst is not None and not inst.empty:
                    stock_name = inst.iloc[0].get('sec_name', '未知')
            except Exception as e:
                logger.error(f"Error fetching stock info: {e}")

        data = stock_cache.get_history_data(symbol, days=2)

        if data is not None and len(data) >= 2:
            today_close = data.iloc[-1]['close']
            prev_close = data.iloc[-2]['close']
            if prev_close and prev_close > 0:
                result['today_gain'] = round((today_close - prev_close) / prev_close * 100, 2)

        result['stock_name'] = stock_name
        return result
    except Exception as e:
        logger.error(f"Error in get-stock-info: {e}")
        return result


@app.get("/get-stock-history")
def get_stock_history(code: str, days: int = 10):
    logger.info(f"API get-stock-history called with code={code}, days={days}")

    try:
        symbol = to_goldminer_symbol(code)

        data = stock_cache.get_history_data(symbol, days)

        if data is not None and len(data) > 0:
            result = []
            prev_avg_price = None

            daily_data = []
            for _, row in data.iterrows():
                close = row['close']
                volume = row['volume']
                amount = row['amount']
                pre_close = row['pre_close']

                avg_price = amount / volume if volume and volume > 0 else 0

                open_amount = 0
                try:
                    trade_date_str = str(row['eob'])[:10]
                    trade_date = datetime.strptime(trade_date_str, '%Y-%m-%d')

                    auction_data = stock_cache.get_auction_data(symbol, trade_date)
                    open_amount = auction_data['open_amount']
                    try:
                        if open_amount is None:
                            open_amount = 0
                        elif isinstance(open_amount, bytes):
                            open_amount = 0
                        else:
                            open_amount = float(open_amount)
                    except (ValueError, TypeError):
                        open_amount = 0
                    except Exception as e:
                        print(f"Error getting open auction amount: {e}")
                except Exception as e:
                    print(f"Error getting auction data: {e}")

                daily_data.append({
                    'date': str(row['eob'])[:10],
                    'open': float(row['open']),
                    'amount': float(amount),
                    'open_amount': float(open_amount),
                    'close': float(close),
                    'avg_price': avg_price,
                    'high': float(row['high']),
                    'low': float(row['low']),
                    'pre_close': float(pre_close)
                })

            for i, day in enumerate(daily_data):
                open_score = None
                if i > 0 and prev_avg_price and prev_avg_price > 0:
                    open_score = ((day['open'] - prev_avg_price) / prev_avg_price * 1000)
                    open_score = round(open_score, 1)

                bias = ((day['close'] - day['avg_price']) / day['avg_price'] * 100) if day['avg_price'] and day['avg_price'] > 0 else 0

                change_pct = ((day['close'] - day['pre_close']) / day['pre_close'] * 100) if day['pre_close'] and day['pre_close'] > 0 else 0

                open_change_pct = 0
                if day['pre_close'] and day['pre_close'] > 0:
                    open_change_pct = ((day['open'] - day['pre_close']) / day['pre_close'] * 100)
                else:
                    open_change_pct = 0

                result.append({
                    'date': day['date'],
                    'open': round(day['open'], 2),
                    'open_score': open_score,
                    'open_change_pct': round(open_change_pct, 2),
                    'open_amount': round(day['open_amount'], 2),
                    'close': round(day['close'], 2),
                    'avg_price': round(day['avg_price'], 2),
                    'high': round(day['high'], 2),
                    'low': round(day['low'], 2),
                    'change_pct': round(change_pct, 2),
                    'bias': round(bias, 2)
                })

                prev_avg_price = day['avg_price']

            result.reverse()
            logger.info(f"Returning {len(result)} days of history data for {code}")
            return result
        else:
            logger.warning(f"No history data found for {code}")
            return []

    except Exception as e:
        logger.error(f"Error getting stock history: {str(e)}")
        import traceback
        traceback.print_exc()
        return []


@app.get("/refresh-filter-2-result")
def refresh_filter_2_result(params: FilterParams = Depends()):
    logger.info(f"API refresh-filter-2-result called with {params.model_dump()}")
    return stock_filter_service.filter_stocks(params)


@app.post("/load-auction-data")
def load_auction_data(stocks: List[Dict[str, Any]], days: int = 30):
    logger.info(f"API load-auction-data called with {len(stocks)} stocks, days={days}")
    return auction_data_service.load_auction_data(stocks, days)


@app.post("/save-filter-stocks")
def save_filter_stocks(stocks: List[Dict[str, Any]]):
    logger.info(f"API save-filter-stocks called with {len(stocks)} stocks")
    return auction_data_service.save_filter_stocks(stocks)


@app.get("/get-filter-2-result")
def get_filter_2_result():
    logger.info("API get-filter-2-result called")
    return _query_filter_results(filter_type=2)


@app.get("/get-block-list")
def get_block_list():
    logger.info("API get-block-list called")

    try:
        with get_db_cursor() as cursor:
            cursor.execute("SELECT block_code, block_name FROM block_info ORDER BY block_code")
            rows = cursor.fetchall()

            blocks = []
            for row in rows:
                blocks.append({
                    'code': row[0],
                    'name': row[1]
                })

            logger.info(f"Returning {len(blocks)} blocks from database")
            return blocks
    except Exception as e:
        logger.error(f"Error in get-block-list: {str(e)}")
        import traceback
        traceback.print_exc()
        return []


@app.get("/get-filter-config")
def get_filter_config(config_type: int = 2):
    logger.info(f"API get-filter-config called with config_type={config_type}")

    try:
        with get_db_cursor() as cursor:
            cursor.execute("""
                SELECT interval_days, interval_max_rise, recent_days, recent_max_day_rise,
                       prev_high_price_rate, select_blocks, trade_date
                FROM filter_config
                WHERE type = ?
            """, (config_type,))
            row = cursor.fetchone()

            if row:
                return {
                    'interval_days': row[0],
                    'interval_max_rise': row[1],
                    'recent_days': row[2],
                    'recent_max_day_rise': row[3],
                    'prev_high_price_rate': row[4],
                    'select_blocks': row[5],
                    'trade_date': row[6],
                    'select_blocks': row[5],
                    'trade_date': row[6]
                }
            return None
    except Exception as e:
        logger.error(f"Error reading filter config: {str(e)}")
        return None


if __name__ == "__main__":
    import uvicorn
    logger.info("Starting server...")
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
