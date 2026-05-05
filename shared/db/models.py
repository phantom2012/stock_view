"""
数据库 ORM 模型统一入口
包含所有业务表模型，供 backend 和 data-sync-service 共用
"""
from sqlalchemy import Column, Integer, String, Float, DateTime, Text
from sqlalchemy import inspect
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
from typing import Dict, Any

Base = declarative_base()


# ==================== 业务表模型 ====================


class FilterResult(Base):
    """筛选结果表"""
    __tablename__ = 'filter_results'

    type = Column(Integer, nullable=False, primary_key=True)
    code = Column(String(10), nullable=False, primary_key=True)
    symbol = Column(String(20), nullable=False)
    stock_name = Column(String(50), nullable=False)
    pre_avg_price = Column(Float, default=0.0)
    pre_close_price = Column(Float, default=0.0)
    pre_price_gain = Column(Float, default=0.0)
    open_price = Column(Float, default=0.0)
    tail_57_price = Column(Float, default=0.0)
    close_price = Column(Float, default=0.0)
    next_close_price = Column(Float, default=0.0)
    price_diff = Column(Float, default=0.0)
    open_volume = Column(Float, default=0.0)
    open_volume_ratio = Column(Float, default=0.0)
    interval_max_rise = Column(Float, default=0.0)
    max_day_rise = Column(Float, default=0.0)
    trade_date = Column(String(10), default='')
    rising_wave_score = Column(Float, default=0.0)
    exp_score = Column(Float, default=0.0)
    weipan_exceed = Column(Integer, default=0)
    zaopan_exceed = Column(Integer, default=0)
    rising_wave = Column(Integer, default=0)
    update_time = Column(DateTime, default=datetime.now)

    @classmethod
    def model_validate(cls, data: Dict[str, Any]) -> 'FilterResult':
        """
        从字典创建 FilterResult 对象，自动过滤无效字段

        Args:
            data: 包含字段数据的字典

        Returns:
            FilterResult 实例
        """
        mapper = inspect(cls)
        valid_columns = {col.name for col in mapper.columns}

        filtered_data = {}
        for key, value in data.items():
            if key in valid_columns and value is not None and key != 'update_time':
                filtered_data[key] = value

        return cls(**filtered_data)


class StockAuction(Base):
    """竞价数据表"""
    __tablename__ = 'stock_auction'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False)
    trade_date = Column(String(10), nullable=False)
    open_price = Column(Float, default=0.0)
    open_amount = Column(Float, default=0.0)
    open_volume = Column(Integer, default=0)
    pre_close = Column(Float, default=0.0)
    turn_over_rate = Column(Float, default=0.0)
    volume_ratio = Column(Float, default=0.0)
    float_share = Column(Float, default=0.0)
    tail_57_price = Column(Float, default=0.0)
    tail_amount = Column(Float, default=0.0)
    tail_volume = Column(Integer, default=0)
    close_price = Column(Float, default=0.0)
    avg_5d_price = Column(Float, default=0.0)
    avg_10d_price = Column(Float, default=0.0)
    update_time = Column(DateTime, default=datetime.now)


class StockDaily(Base):
    """日线数据表"""
    __tablename__ = 'stock_daily'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False)
    trade_date = Column(String(10), nullable=False)
    open = Column(Float, default=0.0)
    close = Column(Float, default=0.0)
    high = Column(Float, default=0.0)
    low = Column(Float, default=0.0)
    volume = Column(Integer, default=0)
    amount = Column(Float, default=0.0)
    pre_close = Column(Float, default=0.0)
    eob = Column(String(20), default='')
    update_time = Column(DateTime, default=datetime.now)


class StockMoneyFlow(Base):
    """资金流向表"""
    __tablename__ = 'stock_money_flow'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False)
    trade_date = Column(String(10), nullable=False)
    name = Column(String(50))
    pct_change = Column(Float)
    close = Column(Float)
    net_amount = Column(Float)
    net_amount_rate = Column(Float)
    net_d5_amount = Column(Float)
    turn_start_date = Column(String(10))
    turn_start_net_amount = Column(Float)
    turn_start_net_amount_rate = Column(Float)
    buy_elg_amount = Column(Float)
    buy_elg_amount_rate = Column(Float)
    buy_lg_amount = Column(Float)
    buy_lg_amount_rate = Column(Float)
    buy_md_amount = Column(Float)
    buy_md_amount_rate = Column(Float)
    buy_sm_amount = Column(Float)
    buy_sm_amount_rate = Column(Float)
    update_time = Column(DateTime, default=datetime.now)


class BlockInfo(Base):
    """板块信息表"""
    __tablename__ = 'block_info'

    block_code = Column(String(10), primary_key=True)
    block_name = Column(String(50), nullable=False)


class BlockStock(Base):
    """板块股票关系表"""
    __tablename__ = 'block_stock'

    id = Column(Integer, primary_key=True, autoincrement=True)
    block_code = Column(String(10), nullable=False)
    block_name = Column(String(50))
    stock_code = Column(String(10), nullable=False)
    update_time = Column(DateTime)


class FilterConfig(Base):
    """筛选配置表"""
    __tablename__ = 'filter_config'

    type = Column(Integer, primary_key=True)
    interval_days = Column(Integer, default=0)
    interval_max_rise = Column(Float, default=0.0)
    recent_days = Column(Integer, default=0)
    recent_max_day_rise = Column(Float, default=0.0)
    prev_high_price_rate = Column(Float, default=0.0)
    select_blocks = Column(String(500), default='')
    trade_date = Column(String(10), default='')
    update_time = Column(DateTime, default=datetime.now)


class StockInfo(Base):
    """股票基本信息表"""
    __tablename__ = 'stock_info'

    code = Column(String(10), primary_key=True)
    name = Column(String(50), nullable=False)
    exchange = Column(String(10), default='')
    free_share = Column(Float, default=0.0)
    circ_mv = Column(Float, default=0.0)
    need_sync = Column(Integer, default=1)
    list_status = Column(String(1), default='')
    list_date = Column(String(10), default='')
    delist_date = Column(String(10), default='')
    update_time = Column(DateTime, default=datetime.now)


class StockMinute(Base):
    """分钟数据表"""
    __tablename__ = 'stock_minute'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False)
    trade_date = Column(String(10), nullable=False)
    eob = Column(String(20), nullable=False)
    open = Column(Float, default=0.0)
    close = Column(Float, default=0.0)
    high = Column(Float, default=0.0)
    low = Column(Float, default=0.0)
    volume = Column(Integer, default=0)
    amount = Column(Float, default=0.0)
    update_time = Column(DateTime, default=datetime.now)


class StockTick(Base):
    """Tick数据表"""
    __tablename__ = 'stock_tick'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False)
    trade_date = Column(String(10), nullable=False)
    created_at = Column(String(20), nullable=False)
    price = Column(Float, default=0.0)
    volume = Column(Integer, default=0)
    cum_amount = Column(Float, default=0.0)
    cum_volume = Column(Integer, default=0)
    update_time = Column(DateTime, default=datetime.now)


class ClearDataTimer(Base):
    """定时清理数据配置表"""
    __tablename__ = 'config_clear_data_timer'

    id = Column(Integer, primary_key=True, autoincrement=True)
    biz_type = Column(String(50), nullable=False, unique=True)
    biz_name = Column(String(100), nullable=False)
    clear_flag = Column(Integer, default=0)
    retain_days = Column(Integer, default=30)
    enabled = Column(Integer, default=1)
    last_clear_time = Column(DateTime, nullable=True)
    create_time = Column(DateTime, default=datetime.now)
    update_time = Column(DateTime, default=datetime.now, onupdate=datetime.now)


class DataSyncNotify(Base):
    """
    数据同步通知表
    用于 backend 通知 data-sync-service 立即执行数据同步
    每种同步类型只有一条记录，通过 trigger_flag 和 status 控制状态流转
    支持按 priority 优先级有序执行，支持通过 stock_codes 指定同步的股票列表
    """
    __tablename__ = 'data_sync_notify'

    priority = Column(Integer, primary_key=True)            # 优先级: 数值越小优先级越高 (0-10)，作为主键
    sync_type = Column(String(50), unique=True, nullable=False)  # 同步类型: money_flow, stock_info, daily_data, auction_data, clear_data
    trigger_flag = Column(Integer, default=0)               # 触发标志: 0=无任务, 1=需要立即同步
    trigger_time = Column(DateTime)                         # 最近一次触发时间
    stock_codes = Column(Text)                              # 股票代码列表（JSON格式），为空则从 filter_result 表读取
    extra_params = Column(String(500), default='')          # 额外参数（JSON格式）
    status = Column(Integer, default=0)                     # 0=待处理, 1=处理中, 2=已完成, -1=失败
    result_msg = Column(String(200), default='')            # 最近一次执行结果信息
    success_count = Column(Integer, default=0)              # 本次成功同步/处理的条数
    fail_count = Column(Integer, default=0)                 # 本次失败/跳过的条数
    update_time = Column(DateTime, default=datetime.now)


class TradeCalendar(Base):
    """
    交易日历表
    替代 Baostock 在线查询，数据由 data-sync-service 初始化并定期更新
    """
    __tablename__ = 'trade_calendar'

    id = Column(Integer, primary_key=True, autoincrement=True)
    calendar_date = Column(String(10), nullable=False, unique=True)  # 日期 YYYY-MM-DD
    is_trading_day = Column(Integer, default=0)                       # 是否交易日: 1=是, 0=否
    year = Column(Integer)
    month = Column(Integer)
    update_time = Column(DateTime, default=datetime.now)
