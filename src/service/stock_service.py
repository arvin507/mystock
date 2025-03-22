import tushare as ts
import pandas as pd
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import text
from src.entities.stock_entity import StockEntity
from src.entities.stock_daily_hq import StockDailyHQEntity
from src.entities.temp_stock_hq import TempStockHQEntity
from src.db.database import engine, execute_query, fetch_all, initialize_database

# 设置Tushare令牌
# ts.set_token('42f603758aa591c4a8109650c5c69df91e5334236e0d1fd418770d1c')
ts.set_token('a574d8d3eb4419cebbf1e26f59024b0623d2829456621c48ba25b7ea')
pro = ts.pro_api()

def clear_table(table_name):
    """
    清空指定的数据库表
    
    参数:
    table_name: 要清空的表名
    """
    with engine.connect() as connection:
        connection.execute(text(f"TRUNCATE TABLE {table_name}"))
        print(f"已清空表 {table_name}")

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
        if 'trade_date' in df.columns:
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y-%m-%d')
        
        for col in df.columns:
            if pd.api.types.is_float_dtype(df[col]):
                df[col] = df[col].astype(float)
                df[col] = df[col].where(pd.notnull(df[col]), None)
        
        df.to_sql(table_name, con=engine, if_exists=if_exists, index=False, chunksize=chunksize, method='multi')
        print(f"成功写入 {len(df)} 条记录到 {table_name}")
    else:
        print(f"没有数据可写入 {table_name}")

def fetch_and_save_stock_basic_data():
    """
    获取并保存股票基本信息
    """
    df = pro.stock_basic()
    write_data(df, StockEntity.__tablename__, 'replace')

def get_latest_trade_dates_for_all_stocks():
    """
    一次性获取所有股票的最新交易日期
    
    返回:
    dict: 以股票代码为键，最新交易日期为值的字典
    """
    query = "SELECT ts_code, MAX(trade_date) as latest_date FROM t_stock_daily_hq GROUP BY ts_code"
    results = fetch_all(query)
    
    latest_dates = {item['ts_code']: item['latest_date'] for item in results}
    return latest_dates

def fetch_and_save_daily_trade_data(start_date, end_date, max_workers=4):
    """
    获取并保存指定日期范围内的股票日线数据
    
    参数:
    start_date: 开始日期，格式为'YYYYMMDD'
    end_date: 结束日期，格式为'YYYYMMDD'
    max_workers: 最大线程数，默认为4
    """
    initialize_database()
    stocks = fetch_all("SELECT ts_code FROM t_stock_basic")
    total_stocks = len(stocks)
    latest_dates = get_latest_trade_dates_for_all_stocks()
    end_date_dt = datetime.strptime(end_date, '%Y%m%d').date()
    
    def fetch_data(stock, index):
        ts_code = stock['ts_code']
        print(f"正在处理股票 {ts_code} ({index + 1}/{total_stocks})...")
        
        latest_date = latest_dates.get(ts_code)
        if latest_date and latest_date >= end_date_dt:
            print(f"  股票 {ts_code} 的数据已是最新 (最新: {latest_date}, 结束: {end_date_dt}).")
            return

        # 确定实际需要的数据范围
        actual_start_date = start_date
        if latest_date:
            actual_start_date = (latest_date + timedelta(days=1)).strftime('%Y%m%d')
        
        # 确定获取历史数据的起始日期
        fetch_start_date = start_date
        
        if latest_date:
            # 查询该股票的历史交易日期，降序排序
            history_query = f"""
            SELECT trade_date FROM t_stock_daily_hq 
            WHERE ts_code = '{ts_code}' 
            ORDER BY trade_date DESC
            """
            history_dates = fetch_all(history_query)
            
            if history_dates:
                # 尝试获取第121个交易日的日期（如果存在）
                if len(history_dates) >= 121:
                    fetch_start_date_dt = history_dates[120]['trade_date']
                    fetch_start_date = fetch_start_date_dt.strftime('%Y%m%d')
                    print(f"  使用第121个交易日 {fetch_start_date} 作为获取数据的起始日期")
                else:
                    # 如果不足121个交易日，使用最早的日期
                    fetch_start_date_dt = history_dates[-1]['trade_date']
                    fetch_start_date = fetch_start_date_dt.strftime('%Y%m%d')
                    print(f"  不足121个交易日，使用最早日期 {fetch_start_date} 作为获取数据的起始日期")
            else:
                print(f"  未找到 {ts_code} 的历史数据，使用默认起始日期 {fetch_start_date}")
        else:
            print(f"  {ts_code} 没有历史数据，使用默认起始日期 {fetch_start_date}")
        
        try:
            # 获取更长时间范围的数据以确保MA计算准确
            df = ts.pro_bar(ts_code=ts_code, start_date=fetch_start_date, end_date=end_date, ma=[5, 10, 20, 30, 60, 120], adjfactor=True)
            if df is None or df.empty:
                print(f"  没有 {ts_code} 从 {fetch_start_date} 到 {end_date} 的可用数据")
                return
            
            # 转换日期列格式以便比较
            if 'trade_date' in df.columns:
                df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
                
            # 只保留数据库中没有的日期的数据
            if latest_date:
                df = df[df['trade_date'] > latest_date]
            
            if df.empty:
                print(f"  股票 {ts_code} 过滤后没有新数据需要保存")
                return
                
            # 转换回字符串格式以便写入数据库
            df['trade_date'] = df['trade_date'].astype(str)
            
            # 写入过滤后的数据
            write_data(df, StockDailyHQEntity.__tablename__, 'append')
        except Exception as e:
            print(f"  获取 {ts_code} 数据时出错: {e}")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_data, stock, index): index for index, stock in enumerate(stocks)}
        for future in as_completed(futures):
            future.result()

def identify_stocks_with_insufficient_data(min_records=5):
    """
    识别数据库中日线记录少于指定数量的股票
    
    参数:
    min_records (int): 最少应有的记录数
    
    返回:
    list: 日线记录不足的股票代码列表
    """
    query = """
    SELECT ts_code, COUNT(*) as record_count
    FROM t_stock_daily_hq
    GROUP BY ts_code
    HAVING COUNT(*) <= %s
    """
    results = fetch_all(query, (min_records,))
    stocks_with_insufficient_data = [item['ts_code'] for item in results]
    print(f"发现 {len(stocks_with_insufficient_data)} 支股票的日线记录少于 {min_records} 条")
    return stocks_with_insufficient_data

def get_all_stocks_without_data():
    """
    获取数据库中所有没有日线数据的股票
    
    返回:
    list: 没有日线数据的股票代码列表
    """
    all_stocks_query = "SELECT ts_code FROM t_stock_basic"
    all_stocks = fetch_all(all_stocks_query)
    all_stock_codes = [stock['ts_code'] for stock in all_stocks]
    
    stocks_with_data_query = "SELECT DISTINCT ts_code FROM t_stock_daily_hq"
    stocks_with_data = fetch_all(stocks_with_data_query)
    stocks_with_data_codes = [stock['ts_code'] for stock in stocks_with_data]
    
    stocks_without_data = list(set(all_stock_codes) - set(stocks_with_data_codes))
    print(f"发现 {len(stocks_without_data)} 支股票没有日线数据")
    return stocks_without_data

def fetch_and_save_complete_data(stock_list, start_date, end_date):
    """
    为指定股票列表获取并保存完整的历史日线数据
    
    参数:
    stock_list: 需要获取数据的股票代码列表
    start_date: 开始日期，格式为'YYYYMMDD'
    end_date: 结束日期，格式为'YYYYMMDD'
    """
    def fetch_data(ts_code):
        try:
            df = ts.pro_bar(ts_code=ts_code, start_date=start_date, end_date=end_date, ma=[5, 10, 20, 30, 60, 120],adjfactor=True)
            if df is None or df.empty:
                print(f"没有 {ts_code} 从 {start_date} 到 {end_date} 的可用数据")
                return
            write_data(df, StockDailyHQEntity.__tablename__, 'append')
        except Exception as e:
            print(f"获取 {ts_code} 数据时出错: {e}")

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(fetch_data, ts_code) for ts_code in stock_list]
        for future in as_completed(futures):
            future.result()

def complete_stock_data(min_records=5, start_date='20240901', end_date='20250307'):
    """
    主函数: 检查并补充不完整的股票数据
    
    参数:
    min_records: 最少应有的记录数
    start_date: 开始日期，格式为'YYYYMMDD'
    end_date: 结束日期，格式为'YYYYMMDD'
    """
    insufficient_data_stocks = identify_stocks_with_insufficient_data(min_records)
    stocks_without_data = get_all_stocks_without_data()
    stocks_to_process = list(set(insufficient_data_stocks + stocks_without_data))
    
    if not stocks_to_process:
        print("所有股票的日线数据都已完整，无需补充数据。")
        return
    
    print(f"开始为 {len(stocks_to_process)} 支股票获取完整历史数据...")
    fetch_and_save_complete_data(stocks_to_process, start_date, end_date)
    print("数据补充完成！")

def init_temp_stock_hq_data(limit=30):
    """
    从日线表中更新临时表数据
    规则是对每一个股票选取n条记录，n默认为30
    """
    clear_table(TempStockHQEntity.__tablename__)

    query = f"""
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) as row_num
        FROM t_stock_daily_hq
    ) subquery
    WHERE row_num <= {limit}
    """
    df = pd.read_sql(query, con=engine)
    if df is None or df.empty:
        print("没有可用的数据")
        return

    write_data(df, TempStockHQEntity.__tablename__, 'replace')
    print("临时表数据更新完成！更新了", len(df), "条记录")
