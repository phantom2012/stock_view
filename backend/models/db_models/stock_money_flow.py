from sqlalchemy import Column, Integer, String, Float, DateTime
from datetime import datetime

from . import Base


class StockMoneyFlow(Base):
    """
    股票资金流向表的ORM模型
    字段对齐Tushare moneyflow_dc接口返回字段
    """
    __tablename__ = 'stock_money_flow'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False)
    trade_date = Column(String(10), nullable=False)
    name = Column(String(50))
    pct_change = Column(Float)  # 涨跌幅
    close = Column(Float)  # 最新价
    net_amount = Column(Float)  # 今日主力净流入额（万元）
    net_amount_rate = Column(Float)  # 今日主力净流入净占比（%）
    net_d5_amount = Column(Float)  # 5日主力净流入额（万元）
    turn_start_date = Column(String(10))  # 转强启动日期
    turn_start_net_amount = Column(Float)  # 转强累计净流入（万元）
    turn_start_net_amount_rate = Column(Float)  # 转强净流入占比（%）
    buy_elg_amount = Column(Float)  # 今日超大单净流入额（万元）
    buy_elg_amount_rate = Column(Float)  # 今日超大单净流入占比（%）
    buy_lg_amount = Column(Float)  # 今日大单净流入额（万元）
    buy_lg_amount_rate = Column(Float)  # 今日大单净流入占比（%）
    buy_md_amount = Column(Float)  # 今日中单净流入额（万元）
    buy_md_amount_rate = Column(Float)  # 今日中单净流入占比（%）
    buy_sm_amount = Column(Float)  # 今日小单净流入额（万元）
    buy_sm_amount_rate = Column(Float)  # 今日小单净流入占比（%）
    update_time = Column(DateTime, default=datetime.now)
