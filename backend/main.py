import os
import pandas as pd
import logging
from datetime import datetime
from typing import List, Dict, Any
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler

from stock_cache import get_stock_cache
from services.strategy_service import get_strategy_service
from services.stock_filter_service import get_stock_filter_service
from services.auction_data_service import get_auction_data_service
from stock_sqlite.database import get_db_connection, get_db_cursor
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
@app.get("/run-strategy")
def api_run_strategy(trade_date: str = None, weipan_exceed: int = 0, zaopan_exceed: int = 0, rising_wave: int = 0, block_codes: str = None):
    logger.info(f"API run-strategy called with trade_date={trade_date}, weipan_exceed={weipan_exceed}, zaopan_exceed={zaopan_exceed}, rising_wave={rising_wave}, block_codes={block_codes}")
    return strategy_service.run_strategy(trade_date, weipan_exceed, zaopan_exceed, rising_wave, block_codes)


@app.get("/get-data")
def get_data():
    logger.info("API get-data called")

    try:
        with get_db_cursor() as cursor:
            cursor.execute("""
                SELECT fr.symbol, fr.code, fr.stock_name, fr.pre_avg_price, fr.pre_close_price, fr.pre_price_gain, fr.auction_start_price, fr.auction_end_price,
                       fr.price_diff, fr.max_gain, fr.max_daily_gain, fr.today_gain, fr.next_day_gain,
                       fr.trade_date, fr.higher_score, fr.rising_wave_score,
                       COALESCE(sa.volume_ratio, 0) as volume_ratio
                FROM filter_results fr
                LEFT JOIN stock_auction sa ON fr.code = sa.code AND fr.trade_date = sa.trade_date
                WHERE fr.type = 1
                ORDER BY fr.higher_score DESC
            """)
            rows = cursor.fetchall()

            results = []
            for row in rows:
                symbol = row[0]
                code = row[1]
                stock_name = row[2]
                pre_avg_price = row[3]
                pre_close_price = row[4]
                pre_price_gain = round(row[5], 2) if row[5] else 0
                auction_start_price = row[6]
                auction_end_price = row[7]
                price_diff = row[8]
                max_gain = row[9]
                max_daily_gain = row[10]
                today_gain = row[11]
                next_day_gain = row[12]
                trade_date = row[13]
                higher_score = row[14]
                rising_wave_score = row[15]
                volume_ratio = round(row[16], 2) if row[16] else 0
                
                # 计算昨乖离率
                yesterday_bias = 0
                if pre_avg_price > 0:
                    yesterday_bias = round((pre_close_price - pre_avg_price) / pre_avg_price * 100, 2)
                
                # 计算今开盘价和开盘涨幅
                today_open = 0
                open_gain = 0
                try:
                    # 获取指定交易日的数据
                    trade_date_obj = datetime.strptime(trade_date, '%Y-%m-%d')
                    data = stock_cache.get_stock_day_data(symbol, trade_date_obj)
                    if data is not None and not data.empty:
                        today_open = data.iloc[-1].get('open', 0)
                        # 开盘涨幅 = (今开盘价 - 昨收盘价) / 昨收盘价 * 100
                        if pre_close_price > 0:
                            open_gain = round((today_open - pre_close_price) / pre_close_price * 100, 2)
                except Exception as e:
                    logger.error(f"Error calculating open price for {symbol}: {e}")
                
                results.append({
                    'symbol': symbol,
                    'code': code,
                    'stock_name': stock_name,
                    'auction_start_price': auction_start_price,
                    'auction_end_price': auction_end_price,
                    'price_diff': price_diff,
                    'max_gain': max_gain,
                    'max_daily_gain': max_daily_gain,
                    'today_gain': today_gain,
                    'next_day_gain': next_day_gain,
                    'trade_date': trade_date,
                    'higher_score': higher_score,
                    'rising_wave_score': rising_wave_score,
                    # 新增字段
                    'yesterday_avg': pre_avg_price,
                    'yesterday_close': pre_close_price,
                    'yesterday_bias': yesterday_bias,
                    'yesterday_gain': pre_price_gain,
                    'today_open': today_open,
                    'open_gain': open_gain,
                    'open_volume_ratio': volume_ratio
                })

            logger.info(f"Returning {len(results)} rows from database")
            return results
    except Exception as e:
        logger.error(f"Error reading from database: {str(e)}")
        return []


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
        'auction_start_price': '-',
        'auction_end_price': '-',
        'price_diff': '-',
        'max_gain': '-',
        'max_daily_gain': '-',
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


@app.get("/filter-stocks")
def filter_stocks(recent_days: int = 10, max_gain: float = 20, daily_gain_days: int = 5, daily_gain_threshold: float = 7, price_ratio: float = 90, block_codes: str = "", only_main_board: bool = False):
    logger.info(f"API filter-stocks called with recent_days={recent_days}, max_gain={max_gain}, daily_gain_days={daily_gain_days}, daily_gain_threshold={daily_gain_threshold}, price_ratio={price_ratio}, block_codes={block_codes}, only_main_board={only_main_board}")
    return stock_filter_service.filter_stocks(recent_days, max_gain, daily_gain_days, daily_gain_threshold, price_ratio, block_codes, only_main_board)


@app.post("/load-auction-data")
def load_auction_data(stocks: List[Dict[str, Any]], days: int = 30):
    logger.info(f"API load-auction-data called with {len(stocks)} stocks, days={days}")
    return auction_data_service.load_auction_data(stocks, days)


@app.post("/save-filter-stocks")
def save_filter_stocks(stocks: List[Dict[str, Any]]):
    logger.info(f"API save-filter-stocks called with {len(stocks)} stocks")
    return auction_data_service.save_filter_stocks(stocks)


@app.get("/get-filter-stocks")
def get_filter_stocks():
    logger.info("API get-filter-stocks called")

    try:
        with get_db_cursor() as cursor:
            cursor.execute("""
                SELECT code, stock_name, max_gain, max_daily_gain
                FROM filter_results
                WHERE type = 2
                ORDER BY max_gain DESC
            """)
            rows = cursor.fetchall()

            results = []
            for row in rows:
                results.append({
                    'code': row[0],
                    'name': row[1],
                    'gain': row[2],
                    'max_daily_gain': row[3]
                })

            logger.info(f"Returning {len(results)} filter stocks from database (type=2)")
            return results
    except Exception as e:
        logger.error(f"Error reading filter stocks from database: {str(e)}")
        return []


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


if __name__ == "__main__":
    import uvicorn
    logger.info("Starting server...")
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
