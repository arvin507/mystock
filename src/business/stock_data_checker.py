##############################################
# 从Tushare获取股票日线数据的时候，某些股票的数据不完整，
# 为了确保数据的完整性，需要对这些股票进行处理。
# 因为我第一次跑数据的时候拿到的一部分只有5条数据，
# 所以我设置了一个最小记录数，如果股票的记录数少于这个值，
# 就会被认为是不完整的数据，需要重新获取。
# 具体的数量可以根据实际情况调整。
##############################################

import pandas as pd
import time
from datetime import datetime, timedelta
import tushare as ts
from src.database import fetch_all, engine
from src.entities.stock_daily_hq import StockDailyHQEntity

ts.set_token('42f603758aa591c4a8109650c5c69df91e5334236e0d1fd418770d1c')
pro = ts.pro_api()

def identify_stocks_with_insufficient_data(min_records=5):
    """
    识别数据库中日线记录少于指定数量的股票
    
    参数:
    min_records (int): 最少应有的记录数
    
    返回:
    list: 日线记录不足的股票代码列表
    """
    # 查询每支股票的日线记录数
    query = """
    SELECT ts_code, COUNT(*) as record_count
    FROM t_stock_daily_hq
    GROUP BY ts_code
    HAVING COUNT(*) <= %s
    """
    
    results = fetch_all(query, (min_records,))
    
    # 提取代码列表
    stocks_with_insufficient_data = [item['ts_code'] for item in results]
    print(f"发现 {len(stocks_with_insufficient_data)} 支股票的日线记录少于 {min_records} 条")
    
    return stocks_with_insufficient_data

def get_all_stocks_without_data():
    """
    获取数据库中所有没有日线数据的股票
    
    返回:
    list: 没有日线数据的股票代码列表
    """
    # 查询所有股票代码
    all_stocks_query = "SELECT ts_code FROM t_stock_basic"
    all_stocks = fetch_all(all_stocks_query)
    all_stock_codes = [stock['ts_code'] for stock in all_stocks]
    
    # 查询有日线数据的股票代码
    stocks_with_data_query = "SELECT DISTINCT ts_code FROM t_stock_daily_hq"
    stocks_with_data = fetch_all(stocks_with_data_query)
    stocks_with_data_codes = [stock['ts_code'] for stock in stocks_with_data]
    
    # 找出没有日线数据的股票
    stocks_without_data = list(set(all_stock_codes) - set(stocks_with_data_codes))
    print(f"发现 {len(stocks_without_data)} 支股票没有日线数据")
    
    return stocks_without_data

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

def fetch_and_save_complete_data(stock_list, start_date, end_date):
    """
    为指定股票列表获取并保存完整的历史日线数据
    
    参数:
    stock_list: 需要获取数据的股票代码列表
    start_date: 开始日期，格式为'YYYYMMDD'
    end_date: 结束日期，格式为'YYYYMMDD'
    """
    total_stocks = len(stock_list)
    processed = 0
    
    for ts_code in stock_list:
        processed += 1
        print(f"处理进度: {processed}/{total_stocks} - 正在获取 {ts_code} 的历史数据...")
        
        try:
            # 从Tushare获取股票的日线交易数据
            df = ts.pro_bar(ts_code=ts_code, start_date=start_date, end_date=end_date, ma=[5, 10, 20, 30, 60, 120])
            
            # 检查df是否为None或空
            if df is None or df.empty:
                print(f"没有 {ts_code} 从 {start_date} 到 {end_date} 的可用数据")
                continue
            
            # 保存日线交易数据到数据库
            write_data(df, StockDailyHQEntity.__tablename__, 'append')
            
        except Exception as e:
            print(f"获取 {ts_code} 数据时出错: {e}")
            continue
        
        # 添加延迟以处理API速率限制
        time.sleep(0.12)

def complete_stock_data(min_records=5, start_date='20240901', end_date='20250307'):
    """
    主函数: 检查并补充不完整的股票数据
    
    参数:
    min_records: 最少应有的记录数
    start_date: 开始日期，格式为'YYYYMMDD'
    end_date: 结束日期，格式为'YYYYMMDD'
    """
    # 获取记录不足的股票
    insufficient_data_stocks = identify_stocks_with_insufficient_data(min_records)
    
    # 获取没有日线数据的股票
    stocks_without_data = get_all_stocks_without_data()
    
    # 合并需要获取数据的股票列表（去重）
    stocks_to_process = list(set(insufficient_data_stocks + stocks_without_data))
    
    if not stocks_to_process:
        print("所有股票的日线数据都已完整，无需补充数据。")
        return
    
    print(f"开始为 {len(stocks_to_process)} 支股票获取完整历史数据...")
    
    # 获取并保存完整历史数据
    fetch_and_save_complete_data(stocks_to_process, start_date, end_date)
    
    print("数据补充完成！")
