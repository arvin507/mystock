import pymysql
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.entities.base import Base
from src.entities.stock_entity import StockEntity
from src.entities.stock_daily_hq import StockDailyHQEntity
from src.entities.temp_stock_hq import TempStockHQEntity

DATABASE_URL = "mysql+pymysql://root:xiaoyi@127.0.0.1:3306/stocks_db"

def create_database():
    """
    创建数据库（如果不存在）
    """
    connection = pymysql.connect(
        host='127.0.0.1',
        user='root',
        password='xiaoyi',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute("CREATE DATABASE IF NOT EXISTS stocks_db")
        connection.commit()
    finally:
        connection.close()

# 创建数据库
create_database()

# 创建引擎
engine = create_engine(DATABASE_URL, echo=True, future=True)

# 导入所有模型以确保它们已注册到Base.metadata
# 这对于create_all正确工作是必要的
from src.entities.stock_entity import StockEntity
from src.entities.stock_daily_hq import StockDailyHQEntity
from src.entities.temp_stock_hq import TempStockHQEntity

# 仅在导入所有模型后创建表
Base.metadata.create_all(engine)

# 创建会话
Session = sessionmaker(bind=engine)
session = Session()

def get_connection():
    """
    获取数据库连接
    
    返回:
    pymysql数据库连接对象
    """
    return pymysql.connect(
        host='127.0.0.1',
        user='root',
        password='xiaoyi',
        database='stocks_db',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

def execute_query(query, params=None):
    """
    执行SQL查询
    
    参数:
    query: SQL查询语句
    params: 查询参数
    """
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
        connection.commit()

def fetch_all(query, params=None):
    """
    执行SQL查询并获取所有结果
    
    参数:
    query: SQL查询语句
    params: 查询参数
    
    返回:
    查询结果列表
    """
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            result = cursor.fetchall()
        return result

def initialize_database():
    """
    确保所有表都已创建
    """
    Base.metadata.create_all(engine)

