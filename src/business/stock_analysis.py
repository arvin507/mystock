import pandas as pd
from src.database import fetch_all
from src.indicators.macd import calculate_macd
from src.indicators.kdj import calculate_kdj
from src.indicators.rsi import calculate_rsi
from src.indicators.bollinger import calculate_bollinger_bands
from src.indicators.moving_averages import calculate_ma, calculate_ema
from src.indicators.volume import calculate_volume_indicators

def fetch_stock_data_as_df(ts_code, start_date=None, end_date=None, limit=None):
    """
    从数据库获取股票数据并转换为pandas DataFrame
    
    参数:
    ts_code (str): 股票代码，如 '000001.SZ'
    start_date (str): 开始日期，格式为 'YYYY-MM-DD'，默认为None表示不限制
    end_date (str): 结束日期，格式为 'YYYY-MM-DD'，默认为None表示不限制
    limit (int): 返回的最大记录数，默认为None表示不限制
    
    返回:
    DataFrame: 股票历史数据，按日期升序排列
    """
    # 构建查询SQL
    query = "SELECT * FROM t_stock_daily_hq WHERE ts_code = %s"
    params = [ts_code]
    
    if start_date:
        query += " AND trade_date >= %s"
        params.append(start_date)
        
    if end_date:
        query += " AND trade_date <= %s"
        params.append(end_date)
        
    # 按日期升序排序
    query += " ORDER BY trade_date ASC"
    
    if limit:
        query += f" LIMIT {int(limit)}"
    
    # 执行查询并获取数据
    data = fetch_all(query, params)
    
    # 将查询结果转换为DataFrame
    df = pd.DataFrame(data)
    
    # 确保日期列是日期类型，并设为索引
    if 'trade_date' in df.columns:
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        # 将日期设为索引以便时间序列分析
        df.set_index('trade_date', inplace=True)
    
    return df

def apply_indicators(df):
    """
    应用各种技术指标到股票数据DataFrame
    
    参数:
    df (DataFrame): 包含股票历史数据的DataFrame
    
    返回:
    DataFrame: 添加了技术指标的DataFrame
    """
    # 顺序应用各种技术指标
    df = calculate_macd(df)         # 计算MACD指标
    df = calculate_kdj(df)          # 计算KDJ随机指标
    df = calculate_rsi(df)          # 计算相对强弱指数
    df = calculate_bollinger_bands(df)  # 计算布林带
    df = calculate_ma(df)           # 计算简单移动平均线
    df = calculate_ema(df)          # 计算指数移动平均线
    df = calculate_volume_indicators(df)  # 计算成交量相关指标
    return df

def get_stock_with_indicators(ts_code, start_date=None, end_date=None, limit=None):
    """
    获取带有所有技术指标的股票数据
    
    参数:
    ts_code (str): 股票代码，如 '000001.SZ'
    start_date (str): 开始日期，格式为 'YYYY-MM-DD'，默认为None表示不限制
    end_date (str): 结束日期，格式为 'YYYY-MM-DD'，默认为None表示不限制
    limit (int): 返回的最大记录数，默认为None表示不限制
    
    返回:
    DataFrame: 添加了所有技术指标的股票历史数据
    """
    # 获取原始股票数据
    df = fetch_stock_data_as_df(ts_code, start_date, end_date, limit)
    
    # 如果数据为空，则返回空DataFrame
    if df.empty:
        print(f"未找到 {ts_code} 在 {start_date} 和 {end_date} 之间的数据")
        return pd.DataFrame()
    
    # 应用技术指标
    df = apply_indicators(df)
    
    return df
