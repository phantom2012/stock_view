from sqlalchemy import Column, String

from . import Base


class BlockInfo(Base):
    """
    板块信息表的ORM模型
    """
    __tablename__ = 'block_info'

    block_code = Column(String(10), primary_key=True)
    block_name = Column(String(50), nullable=False)
