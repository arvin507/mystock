from sqlalchemy import Column, String, Date, VARCHAR
from src.entities.base import Base

class StockEntity(Base):
    __tablename__ = 't_stock_basic'
    # 股票代码
    ts_code = Column(VARCHAR(20), primary_key=True)

    # 股票代码（数字）
    symbol = Column(VARCHAR(20))

    # 股票名称
    name = Column(VARCHAR(100))

    # 所在地域
    area = Column(VARCHAR(100))

    # 所属行业
    industry = Column(VARCHAR(100))

    # 上市板块（主板/创业板/科创板/北交所）
    market = Column(VARCHAR(50))

    # 上市日期
    list_date = Column(Date)
