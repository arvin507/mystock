from sqlalchemy import text, func, desc
from src.db.database import Session
from src.entities.temp_stock_hq import TempStockHQEntity
from datetime import datetime

def get_end_date(session, query_date_column, end_date=None):
    """
    处理结束日期参数，如果未提供则使用数据库中最新日期
    支持多种日期格式：YYYYMMDD, YYYY-MM-DD
    
    参数:
    session: 数据库会话
    query_date_column: 用于查询的日期列
    end_date: 可选的结束日期，如果为None则使用数据库中最新日期
    """
    # 如果未提供结束日期，则使用数据库中最新日期
    if not end_date:
        latest_date = session.query(func.max(query_date_column)).scalar()
        return latest_date
    
    # 如果提供了日期字符串，转换为标准格式
    if isinstance(end_date, str):
        try:
            # 尝试以 %Y%m%d 格式解析（例如：20250306）
            datetime.strptime(end_date, '%Y%m%d')
            # 如果成功，保持原格式并返回
            return end_date
        except ValueError:
            try:
                # 尝试以 %Y-%m-%d 格式解析（例如：2025-03-06）
                date_obj = datetime.strptime(end_date, '%Y-%m-%d')
                # 转换为 YYYY-MM-DD 格式返回
                return date_obj.strftime('%Y-%m-%d')
            except ValueError:
                # 如果两种格式都不匹配，抛出明确的异常
                raise ValueError(f"日期 '{end_date}' 格式无效，请使用 'YYYYMMDD' 或 'YYYY-MM-DD' 格式")
    
    # 如果是日期对象，转换为字符串
    if isinstance(end_date, datetime):
        return end_date.strftime('%Y-%m-%d')
    
    # 其他情况直接返回
    return end_date

def get_trade_date_list(session, query_date_column, end_date, days_count):
    """
    获取到指定结束日期为止的若干个交易日列表
    
    参数:
    session: 数据库会话
    query_date_column: 用于查询的日期列
    end_date: 结束日期
    days_count: 需要获取的交易日数量
    """
    # 处理结束日期
    end_date = get_end_date(session, query_date_column, end_date)
    
    # 查询指定日期之前的若干个交易日
    trade_dates = session.query(query_date_column) \
        .distinct() \
        .filter(query_date_column <= end_date) \
        .order_by(desc(query_date_column)) \
        .limit(days_count) \
        .all()
    
    return trade_dates
