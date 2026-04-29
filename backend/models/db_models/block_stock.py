from sqlalchemy import Column, String

from . import Base


class BlockStock(Base):
    """
    板块股票关系表的ORM模型
    """
    __tablename__ = 'block_stock'

    block_code = Column(String(10), primary_key=True)
    stock_code = Column(String(10), primary_key=True)
