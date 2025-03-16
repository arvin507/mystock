from sqlalchemy import Column, String, Float, Integer, Date, VARCHAR
from src.entities.base import Base

class StockDailyHQEntity(Base):
    __tablename__ = 't_stock_daily_hq'
    
    # 自增ID
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # 股票代码
    ts_code = Column(VARCHAR(20))
    
    # 交易日期
    trade_date = Column(Date)
    
    # 开盘价
    open = Column(Float)
    
    # 最高价
    high = Column(Float)
    
    # 最低价
    low = Column(Float)
    
    # 收盘价
    close = Column(Float)
    
    # 前收盘价
    pre_close = Column(Float)
    
    # 涨跌额
    change = Column(Float)
    
    # 涨跌幅
    pct_chg = Column(Float)
    
    # 成交量
    vol = Column(Float)
    
    # 成交额
    amount = Column(Float)
    
    # 5日均线
    ma5 = Column(Float)
    
    # 5日均量
    ma_v_5 = Column(Float)
    
    # 10日均线
    ma10 = Column(Float)
    
    # 10日均量
    ma_v_10 = Column(Float)
    
    # 20日均线
    ma20 = Column(Float)
    
    # 20日均量
    ma_v_20 = Column(Float)
    
    # 30日均线
    ma30 = Column(Float)
    
    # 30日均量
    ma_v_30 = Column(Float)
    
    # 60日均线
    ma60 = Column(Float)
    
    # 60日均量
    ma_v_60 = Column(Float)
    
    # 120日均线
    ma120 = Column(Float)
    
    # 120日均量
    ma_v_120 = Column(Float)
