import os
import pandas as pd
import logging
from datetime import datetime, timedelta
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler

# 导入股票缓存
from stock_cache import get_stock_cache

# 导入股票过滤器
from stock_filter import StockFilter

# 导入数据库模块
from stock_sqlite.database import get_db_connection

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('server.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 初始化股票缓存
stock_cache = get_stock_cache()
stock_cache.set_api_token("2e664976b46df6a0903672349c30226ac68e7bf3")

# 初始化股票过滤器
stock_filter = StockFilter()

# FastAPI应用
app = FastAPI(title="掘金量化竞价看板后端")

# 跨域（前端能访问）
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 全局变量
last_run_time = None

app = FastAPI(title="掘釆量化竞价看板后端")

# 跨域（前端能访问）
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 全局变量
last_run_time = None

# ==============================================
# 1. 运行掘金策略（核心函数）
# ==============================================
def run_strategy(trade_date=None, weipan_exceed=0, zaopan_exceed=0, rising_wave=0, block_codes=None):
    global last_run_time
    try:
        logger.info(f"Starting strategy execution: trade_date={trade_date}, weipan={weipan_exceed}, zaopan={zaopan_exceed}, rising={rising_wave}, block_codes={block_codes}")
        
        # 解析交易日期
        if trade_date:
            target_date = datetime.strptime(trade_date, '%Y-%m-%d')
        else:
            # 默认使用上一个交易日
            today = datetime.now()
            yesterday = today - timedelta(days=1)
            while yesterday.weekday() >= 5:  # 跳过周末
                yesterday -= timedelta(days=1)
            target_date = yesterday
        
        # 从数据库获取股票列表
        stocks_to_filter = set()
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            # 如果指定了板块，从 block_stock 表查询这些板块下的所有股票
            if block_codes:
                # 解析板块代码（支持逗号分隔的字符串）
                if isinstance(block_codes, str):
                    selected_block_codes = [code.strip() for code in block_codes.split(',') if code.strip()]
                else:
                    selected_block_codes = list(block_codes)
                
                placeholders = ','.join(['?' for _ in selected_block_codes])
                query = f"SELECT DISTINCT stock_code FROM block_stock WHERE block_code IN ({placeholders})"
                cursor.execute(query, selected_block_codes)
                rows = cursor.fetchall()
                stocks_to_filter = {row[0] for row in rows}
                logger.info(f"从数据库中获取到 {len(stocks_to_filter)} 只股票（来自 {len(selected_block_codes)} 个板块）")
            else:
                # 如果没有指定板块，获取所有股票
                cursor.execute("SELECT DISTINCT stock_code FROM block_stock")
                rows = cursor.fetchall()
                stocks_to_filter = {row[0] for row in rows}
                logger.info(f"从数据库中获取到 {len(stocks_to_filter)} 只股票（所有板块）")
        finally:
            cursor.close()
            conn.close()
        
        if not stocks_to_filter:
            return {"status": "error", "msg": "未从数据库加载到股票数据"}
        
        # 转换为SHSE/SZSE格式
        stock_symbols = []
        for code in stocks_to_filter:
            if code.startswith('6') or code.startswith('9'):
                symbol = f"SHSE.{code}"
            else:
                symbol = f"SZSE.{code}"
            stock_symbols.append(symbol)
        
        logger.info(f"准备筛选 {len(stock_symbols)} 只股票")
        
        if not stock_symbols:
            return {"status": "error", "msg": "未加载到股票数据"}
        
        # 配置参数（与goldminer/main.py保持一致）
        config = {
            'recent_interval_days': 40,
            'recent_interval_max_gain': 60,
            'day_max_gain_days': 6,
            'day_max_gain': 8,
        }
        
        logger.info(f"Filtering {len(stock_symbols)} stocks...")
        
        # 确保 instruments 缓存已加载，以便正确识别 ST 股票
        print("[DEBUG] 正在强制加载 instruments 缓存...")
        instruments = stock_cache._load_instruments_cache()
        cache_count = len(instruments) if instruments is not None else 0
        print(f"[DEBUG] instruments 缓存加载完成，共 {cache_count} 条数据")
        
        # 执行筛选
        results = stock_filter.filter_stocks(
            symbols=stock_symbols,
            trade_date=target_date,
            weipan_exceed=weipan_exceed,
            zaopan_exceed=zaopan_exceed,
            rising_wave=rising_wave,
            config=config
        )
        
        logger.info(f"Strategy completed, found {len(results)} stocks")
        
        # 保存结果到数据库
        if results:
            save_start = datetime.now()
            conn = get_db_connection()
            cursor = conn.cursor()
            try:
                current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        
                # 清空旧数据（根据type清空）
                cursor.execute("DELETE FROM filter_results WHERE type = 1")
                        
                # 插入新数据
                insert_count = 0
                for stock in results:
                    symbol = stock.get('symbol', '')
                    code = symbol.split('.')[-1] if '.' in symbol else symbol
                    stock_name = stock.get('stock_name', '')
                    auction_start_price = stock.get('auction_start_price', 0)
                    auction_end_price = stock.get('auction_end_price', 0)
                    price_diff = stock.get('price_diff', 0)
                    max_gain = stock.get('max_gain', 0)
                    max_daily_gain = stock.get('max_daily_gain', 0)
                    today_gain = stock.get('today_gain', 0)
                    next_day_gain = stock.get('next_day_gain', 0)
                    trade_date = stock.get('trade_date', '')
                    higher_score = stock.get('higher_score', 0)
                    rising_wave_score = stock.get('rising_wave_score', 0)
                    weipan_exceed = stock.get('weipan_exceed', 0)
                    zaopan_exceed = stock.get('zaopan_exceed', 0)
                    rising_wave = stock.get('rising_wave', 0)
                            
                    cursor.execute(
                        "INSERT OR REPLACE INTO filter_results (type, symbol, code, stock_name, auction_start_price, auction_end_price, price_diff, max_gain, max_daily_gain, today_gain, next_day_gain, trade_date, higher_score, rising_wave_score, weipan_exceed, zaopan_exceed, rising_wave, update_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (1, symbol, code, stock_name, auction_start_price, auction_end_price, price_diff, max_gain, max_daily_gain, today_gain, next_day_gain, trade_date, higher_score, rising_wave_score, weipan_exceed, zaopan_exceed, rising_wave, current_time)
                    )
                    insert_count += 1
                        
                conn.commit()
                save_end = datetime.now()
                save_duration = (save_end - save_start).total_seconds()
                logger.info(f"Results saved to database ({insert_count} records), elapsed time: {save_duration:.3f} seconds")
            except Exception as e:
                conn.rollback()
                logger.error(f"Error saving results to database: {str(e)}")
            finally:
                cursor.close()
                conn.close()
        
        last_run_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return {"status": "success", "msg": f"策略运行完成，选出{len(results)}只股票", "time": last_run_time}
    except Exception as e:
        logger.error(f"Error running strategy: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"status": "error", "msg": str(e)}

# ==============================================
# 2. 定时任务：每天 09:25 自动运行
# ==============================================
scheduler = BackgroundScheduler(timezone="Asia/Shanghai")
scheduler.add_job(run_strategy, "cron", hour=9, minute=25)  # 每天9:25
try:
    scheduler.start()
    logger.info("Scheduler started")
except Exception as e:
    logger.error(f"Error starting scheduler: {str(e)}")

# ==============================================
# 3. 接口1：手动触发运行策略
# ==============================================
@app.get("/run-strategy")
def api_run_strategy(trade_date: str = None, weipan_exceed: int = 0, zaopan_exceed: int = 0, rising_wave: int = 0, block_codes: str = None):
    logger.info(f"API run-strategy called with trade_date={trade_date}, weipan_exceed={weipan_exceed}, zaopan_exceed={zaopan_exceed}, rising_wave={rising_wave}, block_codes={block_codes}")
    return run_strategy(trade_date, weipan_exceed, zaopan_exceed, rising_wave, block_codes)

# ==============================================
# 4. 接口2：读取表格返回给前端
# ==============================================
@app.get("/get-data")
def get_data():
    logger.info("API get-data called")
    
    # 从数据库读取筛选结果
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT symbol, code, stock_name, auction_start_price, auction_end_price, 
                       price_diff, max_gain, max_daily_gain, today_gain, next_day_gain, 
                       trade_date, higher_score, rising_wave_score
                FROM filter_results 
                WHERE type = 1
                ORDER BY higher_score DESC
            """)
            rows = cursor.fetchall()
            
            results = []
            for row in rows:
                results.append({
                    'symbol': row[0],
                    'code': row[1],
                    'stock_name': row[2],
                    'auction_start_price': row[3],
                    'auction_end_price': row[4],
                    'price_diff': row[5],
                    'max_gain': row[6],
                    'max_daily_gain': row[7],
                    'today_gain': row[8],
                    'next_day_gain': row[9],
                    'trade_date': row[10],
                    'higher_score': row[11],
                    'rising_wave_score': row[12]
                })
            
            logger.info(f"Returning {len(results)} rows from database")
            return results
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        logger.error(f"Error reading from database: {str(e)}")
        return []

# ==============================================
# 5. 接口3：获取可用交易日期列表
# ==============================================
@app.get("/get-trade-dates")
def get_trade_dates():
    logger.info("API get-trade-dates called")
    # 从CSV文件读取交易日数据
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

# ==============================================
# 5. 健康检查
# ==============================================
@app.get("/")
def index():
    logger.info("API health check called")
    return {"status": "运行中", "last_run": last_run_time}

# ==============================================
# 6. 获取股票基本信息
# ==============================================
@app.get("/get-stock-info")
def get_stock_info(code: str):
    logger.info(f"API get-stock-info called with code={code}")
    
    try:
        # 构建完整的股票代码
        if code.startswith('6'):
            symbol = f"SHSE.{code}"
        else:
            symbol = f"SZSE.{code}"
        
        # 从缓存获取股票名称
        stock_name = stock_cache.get_stock_name(symbol)
        
        # 如果缓存中没有，尝试通过API单独查询
        if stock_name == '未知':
            try:
                from gm.api import get_instruments
                inst = get_instruments(symbols=[symbol], df=True)
                if inst is not None and not inst.empty:
                    stock_name = inst.iloc[0].get('sec_name', '未知')
            except Exception as e:
                logger.error(f"Error fetching stock info: {e}")
        
        # 获取历史数据来填充其他信息
        data = stock_cache.get_history_data(symbol, days=2)
        
        today_gain = '-'
        if data is not None and len(data) >= 2:
            today_close = data.iloc[-1]['close']
            prev_close = data.iloc[-2]['close']
            if prev_close and prev_close > 0:
                today_gain = round((today_close - prev_close) / prev_close * 100, 2)
        
        return {
            'code': code,
            'stock_name': stock_name,
            'today_gain': today_gain,
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
    except Exception as e:
        logger.error(f"Error in get-stock-info: {e}")
        return {
            'code': code,
            'stock_name': '未知股票',
            'auction_start_price': '-',
            'auction_end_price': '-',
            'price_diff': '-',
            'max_gain': '-',
            'max_daily_gain': '-',
            'today_gain': '-',
            'next_day_gain': '-',
            'trade_date': datetime.now().strftime('%Y-%m-%d'),
            'higher_score': '-',
            'rising_wave_score': '-',
            'weipan_exceed': False,
            'zaopan_exceed': False,
            'rising_wave': False
        }

# ==============================================
# 7. 获取股票最近N日历史数据
# ==============================================
@app.get("/get-stock-history")
def get_stock_history(code: str, days: int = 10):
    logger.info(f"API get-stock-history called with code={code}, days={days}")
    
    try:
        # 构建完整的股票代码
        if code.startswith('6'):
            symbol = f"SHSE.{code}"
        else:
            symbol = f"SZSE.{code}"
        
        # 从缓存获取历史数据
        data = stock_cache.get_history_data(symbol, days)
        
        if data is not None and len(data) > 0:
            result = []
            prev_avg_price = None  # 保存前一天的收盘均价
            
            # 先按日期正序处理，计算每天的收盘均价和开盘成交额
            daily_data = []
            for _, row in data.iterrows():
                close = row['close']
                volume = row['volume']
                amount = row['amount']
                pre_close = row['pre_close']

                # 计算收盘均价 = 成交额 / 成交量
                avg_price = amount / volume if volume and volume > 0 else 0

                # 获取开盘竞价数据（使用新的get_auction_data方法）
                open_amount = 0
                try:
                    # 解析日期
                    trade_date_str = str(row['eob'])[:10]
                    trade_date = datetime.strptime(trade_date_str, '%Y-%m-%d')

                    # 使用新的get_auction_data方法获取竞价数据
                    auction_data = stock_cache.get_auction_data(symbol, trade_date)
                    # 页面的开盘成交额使用9:30分后的第一个金额
                    open_amount = auction_data['open_amount']
                    # 确保open_amount是数字类型
                    try:
                        if open_amount is None:
                            open_amount = 0
                        elif isinstance(open_amount, bytes):
                            # 处理字节串类型
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
            
            # 计算每天的开盘得分和其他指标
            for i, day in enumerate(daily_data):
                # 计算开盘得分 = (开盘价 - 昨日收盘均价) / 昨日收盘均价 * 1000
                open_score = None
                if i > 0 and prev_avg_price and prev_avg_price > 0:
                    open_score = ((day['open'] - prev_avg_price) / prev_avg_price * 1000)
                    open_score = round(open_score, 1)  # 保留1位小数
                
                # 计算乖离率 = (收盘价 - 收盘均价) / 收盘均价 * 100
                bias = ((day['close'] - day['avg_price']) / day['avg_price'] * 100) if day['avg_price'] and day['avg_price'] > 0 else 0
                
                # 计算涨跌幅
                change_pct = ((day['close'] - day['pre_close']) / day['pre_close'] * 100) if day['pre_close'] and day['pre_close'] > 0 else 0
                
                # 计算开盘涨幅 = (开盘价 - 昨日收盘价) / 昨日收盘价 * 100
                open_change_pct = 0
                if day['pre_close'] and day['pre_close'] > 0:
                    open_change_pct = ((day['open'] - day['pre_close']) / day['pre_close'] * 100)
                else:
                    # 如果没有昨日收盘价，使用当天的前收盘价（可能是0）
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
                
                # 更新前一天的收盘均价
                prev_avg_price = day['avg_price']
            
            # 按日期倒序排列（最近的在前）
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

# ==============================================
# 8. 股票筛选接口
# ==============================================
@app.get("/filter-stocks")
def filter_stocks(recent_days: int = 10, max_gain: float = 20, daily_gain_days: int = 5, daily_gain_threshold: float = 7, price_ratio: float = 90, block_codes: str = ""):
    """
    根据条件筛选股票
    :param recent_days: 最近天数（用于计算区间涨幅）
    :param max_gain: 区间最大涨幅百分比
    :param daily_gain_days: 最近N日（用于计算日内最大涨幅）
    :param daily_gain_threshold: 日内最大涨幅阈值百分比
    :param price_ratio: 股价不低于近期高点的百分比
    :param block_codes: 板块代码列表，逗号分隔
    :return: 符合条件的股票列表
    """
    logger.info(f"API filter-stocks called with recent_days={recent_days}, max_gain={max_gain}, daily_gain_days={daily_gain_days}, daily_gain_threshold={daily_gain_threshold}, price_ratio={price_ratio}, block_codes={block_codes}")
    
    try:
        # 解析板块代码
        selected_block_codes = []
        if block_codes:
            selected_block_codes = [code.strip() for code in block_codes.split(',') if code.strip()]
        
        # 从数据库获取股票列表
        stocks_to_filter = set()
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            if selected_block_codes:
                # 如果指定了板块，从 block_stock 表查询这些板块下的所有股票
                placeholders = ','.join(['?' for _ in selected_block_codes])
                query = f"SELECT DISTINCT stock_code FROM block_stock WHERE block_code IN ({placeholders})"
                cursor.execute(query, selected_block_codes)
                rows = cursor.fetchall()
                stocks_to_filter = {row[0] for row in rows}
                logger.info(f"从数据库中获取到 {len(stocks_to_filter)} 只股票（来自 {len(selected_block_codes)} 个板块）")
            else:
                # 如果没有指定板块，获取所有股票
                cursor.execute("SELECT DISTINCT stock_code FROM block_stock")
                rows = cursor.fetchall()
                stocks_to_filter = {row[0] for row in rows}
                logger.info(f"从数据库中获取到 {len(stocks_to_filter)} 只股票（所有板块）")
        finally:
            cursor.close()
            conn.close()
        
        filtered_results = []
        
        # 获取stock_cache实例
        from stock_cache import get_stock_cache
        stock_cache = get_stock_cache()
        
        # 遍历每只股票进行筛选
        for code in stocks_to_filter:
            if not code:
                continue
            
            # 构建完整的股票代码
            if code.startswith('6'):
                symbol = f"SHSE.{code}"
            else:
                symbol = f"SZSE.{code}"
            
            try:
                # 获取股票名称
                stock_name = stock_cache.get_stock_name(symbol)
                
                # 获取历史数据
                history_data = stock_cache.get_history_data(symbol, recent_days)
                
                if history_data is None or len(history_data) == 0:
                    continue
                
                # 计算最近N日的最高价
                high_prices = history_data['high'].dropna()
                if len(high_prices) == 0:
                    continue
                
                recent_high = high_prices.max()
                
                # 获取当前价格（最新收盘价）
                current_price = history_data.iloc[-1]['close']
                
                # 计算股价相对于近期高点的比例
                if recent_high > 0:
                    price_to_high_ratio = (current_price / recent_high) * 100
                else:
                    continue
                
                # 计算最近N日的最大涨幅
                if len(history_data) >= 2:
                    first_close = history_data.iloc[0]['close']
                    last_close = history_data.iloc[-1]['close']
                    if first_close > 0:
                        period_gain = ((last_close - first_close) / first_close) * 100
                    else:
                        continue
                else:
                    continue
                
                # 应用筛选条件
                # 1. 最大涨幅需要大于等于设定值（寻找强势股）
                if abs(period_gain) < max_gain:
                    continue
                
                # 2. 最近N日日内最大涨幅需要大于设定值
                if len(history_data) >= 2:
                    max_daily_gain = 0.0
                    check_days = min(daily_gain_days, len(history_data) - 1)
                    for i in range(1, check_days + 1):
                        current_row = history_data.iloc[-i]
                        prev_row = history_data.iloc[-i - 1]
                        
                        current_high = current_row['high']
                        prev_close = prev_row['close']
                        
                        if prev_close > 0:
                            day_gain = ((current_high - prev_close) / prev_close) * 100
                            if day_gain > max_daily_gain:
                                max_daily_gain = day_gain
                    
                    if max_daily_gain < daily_gain_threshold:
                        continue
                
                # 3. 股价不低于近期高点的设定百分比
                if price_to_high_ratio < price_ratio:
                    continue
                
                # 符合条件，添加到结果中
                filtered_results.append({
                    'code': code,
                    'name': stock_name,
                    'gain': round(period_gain, 2),
                    'max_daily_gain': round(max_daily_gain, 2)
                })
                
            except Exception as e:
                logger.error(f"Error processing stock {code}: {str(e)}")
                continue
        
        logger.info(f"Filter completed, found {len(filtered_results)} stocks")
        return filtered_results
        
    except Exception as e:
        logger.error(f"Error in filter-stocks: {str(e)}")
        import traceback
        traceback.print_exc()
        return []

# ==============================================
# 9. 获取板块列表
# ==============================================
@app.get("/get-block-list")
def get_block_list():
    """
    从数据库读取板块列表
    :return: 板块列表
    """
    logger.info("API get-block-list called")
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
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
            
        finally:
            cursor.close()
            conn.close()
        
    except Exception as e:
        logger.error(f"Error in get-block-list: {str(e)}")
        import traceback
        traceback.print_exc()
        return []

if __name__ == "__main__":
    import uvicorn
    logger.info("Starting server...")

    # ==============================================
    # 热更新配置（开发时使用，生产环境建议关闭）
    # ==============================================
    # 如需热更新功能，使用下面的导入字符串方式
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)

    # 如不需要热更新，使用下面的方式
    # uvicorn.run(app, host="127.0.0.1", port=8000)