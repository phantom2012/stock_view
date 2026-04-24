import sqlite3
import os
from contextlib import contextmanager
from typing import Optional, Generator

DATABASE_PATH = r"F:\gupiao\_sqlite_stock_data\stock.db"


def get_db_connection() -> sqlite3.Connection:
    """获取数据库连接"""
    conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
    conn.execute("PRAGMA cache_size = -512000;")
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    return conn


@contextmanager
def get_db_cursor() -> Generator:
    """获取数据库连接的上下文管理器"""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        yield cursor
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()


def init_database():
    """初始化数据库表结构"""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS stock_info")
    cursor.execute("DROP TABLE IF EXISTS stock_daily")
    cursor.execute("DROP TABLE IF EXISTS stock_minute")
    cursor.execute("DROP TABLE IF EXISTS stock_tick")
    cursor.execute("DROP TABLE IF EXISTS stock_auction")
    cursor.execute("DROP TABLE IF EXISTS block_info")
    cursor.execute("DROP TABLE IF EXISTS block_stock")
    cursor.execute("DROP TABLE IF EXISTS filter_results")

    # 股票基本信息表
    cursor.execute("""
    CREATE TABLE stock_info (
        code TEXT PRIMARY KEY,           -- 股票代码(纯数字, 如: 600666)
        name TEXT,                        -- 股票名称(如: 航天动力)
        exchange TEXT,                    -- 交易所代码(SHSE=上海, SZSE=深圳)
        update_time TEXT                  -- 更新时间
    )
    """)

    cursor.execute("""
    CREATE TABLE stock_daily (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code TEXT NOT NULL,               -- 股票代码(纯数字)
        trade_date TEXT NOT NULL,         -- 交易日期(YYYY-MM-DD)
        open REAL,                        -- 开盘价
        close REAL,                       -- 收盘价
        high REAL,                        -- 最高价
        low REAL,                         -- 最低价
        volume INTEGER,                   -- 成交量(股数)
        amount REAL,                      -- 成交额(元)
        pre_close REAL,                   -- 前收盘价
        eob TEXT,                         -- 行情时间戳(YYYY-MM-DD HH:MM:SS)
        update_time TEXT,                 -- 更新时间
        UNIQUE(code, trade_date, eob)
    )
    """)

    cursor.execute("""
    CREATE INDEX idx_stock_daily_code_date
    ON stock_daily(code, trade_date)
    """)

    # 分钟数据表
    cursor.execute("""
    CREATE TABLE stock_minute (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code TEXT NOT NULL,               -- 股票代码(纯数字)
        trade_date TEXT NOT NULL,         -- 交易日期(YYYY-MM-DD)
        eob TEXT NOT NULL,                -- 时间点(YYYY-MM-DD HH:MM:SS)
        open REAL,                        -- 开盘价
        close REAL,                       -- 收盘价
        high REAL,                        -- 最高价
        low REAL,                         -- 最低价
        volume INTEGER,                   -- 成交量(股数)
        amount REAL,                      -- 成交额(元)
        update_time TEXT,                 -- 更新时间
        UNIQUE(code, trade_date, eob)
    )
    """)

    cursor.execute("""
    CREATE INDEX idx_stock_minute_code_date
    ON stock_minute(code, trade_date)
    """)

    # Tick数据表(分时成交明细)
    cursor.execute("""
    CREATE TABLE stock_tick (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code TEXT NOT NULL,               -- 股票代码(纯数字)
        trade_date TEXT NOT NULL,         -- 交易日期(YYYY-MM-DD)
        created_at TEXT NOT NULL,         -- 成交时间(YYYY-MM-DD HH:MM:SS)
        price REAL,                       -- 成交价格
        volume INTEGER,                   -- 成交量(股数, 即主动买入量)
        cum_amount REAL,                  -- 累计成交额(元)
        cum_volume INTEGER,               -- 累计成交量(股数)
        update_time TEXT,                 -- 更新时间
        UNIQUE(code, trade_date, created_at)
    )
    """)

    cursor.execute("""
    CREATE INDEX idx_stock_tick_code_date
    ON stock_tick(code, trade_date)
    """)

    # 竞价数据表(早盘竞价)
    cursor.execute("""
    CREATE TABLE stock_auction (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code TEXT NOT NULL,               -- 股票代码(纯数字)
        trade_date TEXT NOT NULL,         -- 交易日期(YYYY-MM-DD)
        price REAL,                       -- 竞价价格(早盘竞价成交价)
        amount REAL,                      -- 竞价成交额(元)
        volume INTEGER,                   -- 竞价成交量(股数)
        pre_close REAL,                   -- 前收盘价
        turn_over_rate REAL,              -- 换手率(%)
        volume_ratio REAL,                -- 量比
        float_share REAL,                 -- 流通股本(万股)
        auction_price REAL,               -- 早盘竞价价格(=price)
        auction_amount REAL,              -- 早盘竞价成交额(=amount)
        open_price REAL,                  -- 开盘价(=price)
        open_amount REAL,                 -- 开盘成交额(=amount)
        tail_57_price REAL,               -- 尾盘(14:57)竞价价格
        update_time TEXT,                 -- 更新时间
        UNIQUE(code, trade_date)
    )
    """)

    cursor.execute("""
    CREATE INDEX idx_stock_auction_code_date
    ON stock_auction(code, trade_date)
    """)

    # 板块信息表
    cursor.execute("""
    CREATE TABLE block_info (
        block_code TEXT PRIMARY KEY,      -- 板块代码(如: 880081)
        block_name TEXT NOT NULL,         -- 板块名称(如: 轮动趋势)
        update_time TEXT                  -- 更新时间
    )
    """)

    # 板块股票关系表（板块 -> 股票）
    cursor.execute("""
    CREATE TABLE block_stock (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        block_code TEXT NOT NULL,         -- 板块代码(如: 880081)
        stock_code TEXT NOT NULL,         -- 股票代码(纯数字, 如: 600666)
        update_time TEXT,                 -- 更新时间
        UNIQUE(block_code, stock_code)
    )
    """)

    cursor.execute("""
    CREATE INDEX idx_block_stock_block_code
    ON block_stock(block_code)
    """)

    cursor.execute("""
    CREATE INDEX idx_block_stock_stock_code
    ON block_stock(stock_code)
    """)

    # 筛选结果表（存储超预期选股结果）
    cursor.execute("""
    CREATE TABLE filter_results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        type INTEGER NOT NULL DEFAULT 1,      -- 筛选类型：1=竞价超预期选股
        symbol TEXT NOT NULL,               -- 股票代码(如: SHSE.600666)
        code TEXT NOT NULL,                 -- 纯数字代码(如: 600666)
        stock_name TEXT,                    -- 股票名称
        auction_start_price REAL,           -- 竞价开始价
        auction_end_price REAL,             -- 竞价结束价
        price_diff REAL,                    -- 价格差异
        max_gain REAL,                      -- 区间最大涨幅
        max_daily_gain REAL,                -- 日内最大涨幅
        today_gain REAL,                    -- 今日涨幅
        next_day_gain REAL,                 -- 次日涨幅
        trade_date TEXT,                    -- 交易日期
        higher_score REAL,                  -- 超预期得分
        rising_wave_score INTEGER,          -- 升浪形态得分
        weipan_exceed INTEGER,              -- 尾盘超预期阈值
        zaopan_exceed INTEGER,              -- 早盘超预期阈值
        rising_wave INTEGER,                -- 升浪形态标志
        update_time TEXT,                   -- 更新时间
        UNIQUE(type, symbol)
    )
    """)

    cursor.execute("""
    CREATE INDEX idx_filter_results_symbol
    ON filter_results(symbol)
    """)

    cursor.execute("""
    CREATE INDEX idx_filter_results_code
    ON filter_results(code)
    """)

    conn.commit()
    conn.close()
    print(f"[SQLite] 数据库初始化完成: {DATABASE_PATH}")


if __name__ == "__main__":
    init_database()
    print("SQLite 数据库结构设计:")
    print("=" * 50)
    print("1. stock_info       - 股票基本信息表")
    print("2. stock_daily      - 日线数据表")
    print("3. stock_minute     - 分钟数据表")
    print("4. stock_tick       - Tick数据表")
    print("5. stock_auction    - 竞价数据表")
    print("6. block_info       - 板块信息表")
    print("7. block_stock      - 板块股票关系表")
    print("8. filter_results   - 筛选结果表")
