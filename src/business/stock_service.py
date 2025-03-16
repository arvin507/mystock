import tushare as ts
import pandas as pd
import time
from datetime import datetime, timedelta
from src.entities.stock_entity import StockEntity
from src.entities.stock_daily_hq import StockDailyHQEntity
from src.database import engine, execute_query, fetch_all, initialize_database

# 设置Tushare令牌
# ts.set_token('a574d8d3eb4419cebbf1e26f59024b0623d2829456621c48ba25b7ea')

ts.set_token('42f603758aa591c4a8109650c5c69df91e5334236e0d1fd418770d1c')

pro = ts.pro_api()

def write_data(df, table_name, if_exists='append', chunksize=5000):
    """
    将DataFrame写入数据库，并进行适当的数据类型转换
    
    参数:
    df: 要写入的DataFrame
    table_name: 要写入的表名
    if_exists: 如果表已存在，采取的行动，可选值为'append'或'replace'
    chunksize: 每次写入的批量大小
    """
    if df is not None and not df.empty:
        # 如果DataFrame中存在trade_date列，将其转换为日期类型
        if 'trade_date' in df.columns:
            df['trade_date'] = pd.to_datetime(df['trade_date'])
        
        # 确保数据类型与数据库模式匹配
        for col in df.columns:
            # 将NaN值转换为None以便SQL兼容
            if pd.api.types.is_float_dtype(df[col]):
                df[col] = df[col].astype(float)
                df[col] = df[col].where(pd.notnull(df[col]), None)
        
        # 写入数据库
        df.to_sql(table_name, con=engine, if_exists=if_exists, index=False, chunksize=chunksize)
        print(f"成功写入 {len(df)} 条记录到 {table_name}")
    else:
        print(f"没有数据可写入 {table_name}")

def fetch_and_save_stock_basic_data():
    """
    获取并保存股票基本信息
    """
    # 获取股票基本信息
    df = pro.stock_basic()

    # 保存股票数据到数据库
    write_data(df, StockEntity.__tablename__, 'replace')

def get_latest_trade_dates_for_all_stocks():
    """
    一次性获取所有股票的最新交易日期
    
    返回:
    dict: 以股票代码为键，最新交易日期为值的字典
    """
    query = "SELECT ts_code, MAX(trade_date) as latest_date FROM t_stock_daily_hq GROUP BY ts_code"
    results = fetch_all(query)
    
    # 创建一个以ts_code为键，最新日期为值的字典
    latest_dates = {}
    for item in results:
        latest_dates[item['ts_code']] = item['latest_date']
    
    return latest_dates

def fetch_and_save_daily_trade_data(start_date, end_date):
    """
    获取并保存指定日期范围内的股票日线数据
    
    参数:
    start_date: 开始日期，格式为'YYYYMMDD'
    end_date: 结束日期，格式为'YYYYMMDD'
    """
    # 初始化数据库，如果表不存在则创建
    initialize_database()

    # 获取所有股票信息
    stocks = fetch_all("SELECT ts_code FROM t_stock_basic")
    total_stocks = len(stocks)
    processed = 0
    
    # 一次性获取所有股票的最新交易日期
    latest_dates = get_latest_trade_dates_for_all_stocks()
    
    # 将end_date字符串转换为日期对象以便比较
    end_date_dt = datetime.strptime(end_date, '%Y%m%d').date()

    for stock in stocks:
        ts_code = stock['ts_code']
        processed += 1
        print(f"处理进度: {processed}/{total_stocks} - 正在处理股票 {ts_code}...")
        
        # 使用字典获取最新日期，如果没有则为None
        latest_date = latest_dates.get(ts_code)

        if latest_date:
            # 如果最新日期大于或等于结束日期，则跳过获取数据
            if latest_date >= end_date_dt:
                print(f"  股票 {ts_code} 的数据已是最新 (最新: {latest_date}, 结束: {end_date_dt}).")
                continue

            # 如果有现有数据，则从最新日期的下一天开始获取
            start_date = (latest_date + timedelta(days=1)).strftime('%Y%m%d')

        # 从Tushare获取每支股票的日线交易数据
        try:
            df = ts.pro_bar(ts_code=ts_code, start_date=start_date, end_date=end_date, ma=[5, 10, 20, 30, 60, 120])
            
            # 检查df是否为None或空
            if df is None or df.empty:
                print(f"  没有 {ts_code} 从 {start_date} 到 {end_date} 的可用数据")
                continue
            
            # 保存日线交易数据到数据库
            write_data(df, StockDailyHQEntity.__tablename__, 'append')
            
        except Exception as e:
            print(f"  获取 {ts_code} 数据时出错: {e}")
            continue

        # 添加延迟以处理API速率限制
        # time.sleep(0.12)
