import pandas as pd
import numpy as np
from src.database import fetch_all, execute_query, engine
from src.entities.temp_stock_hq import TempStockHQEntity

def create_temp_table():
    """
    创建临时表用于存储特定周期的股票价格数据
    """
    # 首先删除临时表（如果存在）
    execute_query("DROP TABLE IF EXISTS temp_stock_hq")
    
    # 创建临时表
    TempStockHQEntity.__table__.create(bind=engine, checkfirst=True)
    
    print("临时表 temp_stock_hq 已创建")

def populate_temp_table(start_date, end_date, period_days):
    """
    从日线数据中提取数据并填充到临时表中，只提取计算RPS所需的数据
    
    参数:
    end_date (str): 结束日期，格式为'YYYY-MM-DD'
    period_days (int): 计算RPS的周期天数
    """
    # 获取最近的交易日期
    latest_date_query = f"SELECT MAX(trade_date) as latest_date FROM t_stock_daily_hq WHERE trade_date <= '{end_date}'"
    latest_date_result = fetch_all(latest_date_query)
    latest_date = latest_date_result[0]['latest_date'] if latest_date_result else None
    
    if not latest_date:
        print("没有找到交易日期数据")
        return
    
    # 计算历史参考日期（period_days天前）
    historical_date_query = f"""
    SELECT MAX(trade_date) as historical_date 
    FROM t_stock_daily_hq 
    WHERE trade_date <= DATE_SUB('{latest_date}', INTERVAL {period_days} DAY)
    """
    historical_date_result = fetch_all(historical_date_query)
    historical_date = historical_date_result[0]['historical_date'] if historical_date_result else None
    
    if not historical_date:
        print(f"没有找到{period_days}天前的数据")
        return
    
    print(f"计算RPS使用的日期: 当前日期 = {latest_date}, 历史日期 = {historical_date}")
    
    # 只插入计算RPS所需的两个日期的数据
    query = f"""
    INSERT INTO temp_stock_hq (`ts_code`, `trade_date`, `open`, `high`, `low`, `close`, `pre_close`, `change`, `pct_chg`, `vol`, `amount`, `ma5`, `ma_v_5`, `ma10`, `ma_v_10`, `ma20`, `ma_v_20`, `ma30`, `ma_v_30`, `ma60`, `ma_v_60`, `ma120`, `ma_v_120`)
    SELECT `ts_code`, `trade_date`, `open`, `high`, `low`, `close`, `pre_close`, `change`, `pct_chg`, `vol`, `amount`, `ma5`, `ma_v_5`, `ma10`, `ma_v_10`, `ma20`, `ma_v_20`, `ma30`, `ma_v_30`, `ma60`, `ma_v_60`, `ma120`, `ma_v_120`
    FROM t_stock_daily_hq
    WHERE `trade_date` IN ('{latest_date}', '{historical_date}')
    """
    
    execute_query(query)
    
    # 检查是否成功插入数据
    count_query = "SELECT COUNT(*) as count FROM temp_stock_hq"
    result = fetch_all(count_query)
    print(f"临时表 temp_stock_hq 中插入了 {result[0]['count']} 条记录")

def calculate_rps(period_days, min_rps=80):
    """
    计算RPS (相对强度指数) 并选出大于指定值的股票
    
    参数:
    period_days (int): 计算RPS的周期天数
    min_rps (float): 最小RPS值
    
    返回:
    DataFrame: 包含RPS值的DataFrame
    """
    # 获取最新日期和历史日期
    dates_query = """
    SELECT MAX(trade_date) as latest_date, MIN(trade_date) as historical_date 
    FROM temp_stock_hq
    """
    dates_result = fetch_all(dates_query)
    
    if not dates_result:
        print("临时表中没有数据")
        return pd.DataFrame()
    
    latest_date = dates_result[0]['latest_date']
    historical_date = dates_result[0]['historical_date']
    
    # 使用更简单的查询获取当前和历史价格 - 使用反引号转义保留关键字
    query = """
    SELECT 
        latest.ts_code, 
        latest.close as `current_close`, 
        latest.trade_date as `current_date`,
        hist.close as `historical_close`, 
        hist.trade_date as `historical_date`,
        ((latest.close / hist.close) - 1) * 100 as `price_change_rate`
    FROM 
        (SELECT ts_code, close, trade_date FROM temp_stock_hq WHERE trade_date = %s) latest
    JOIN 
        (SELECT ts_code, close, trade_date FROM temp_stock_hq WHERE trade_date = %s) hist
    ON latest.ts_code = hist.ts_code
    """
    
    data = fetch_all(query, (latest_date, historical_date))
    
    # 转换为DataFrame
    df = pd.DataFrame(data)
    
    if df.empty:
        print("临时表中没有足够的数据来计算RPS")
        return pd.DataFrame()
    
    # 确保价格变化率是数值类型
    df['price_change_rate'] = pd.to_numeric(df['price_change_rate'], errors='coerce')
    
    # 移除NaN值
    df = df.dropna(subset=['price_change_rate'])
    
    # 按价格变化率降序排序（最高到最低）
    df = df.sort_values('price_change_rate', ascending=False)
    
    # 添加排名列
    df['rank'] = range(1, len(df) + 1)
    
    # 计算RPS值：(总数 - 排名 + 1) / 总数 * 100
    # 或者更简单地说：(1 - (排名-1)/总数) * 100
    total = len(df)
    df['rps'] = ((total - df['rank'] + 1) / total) * 100
    
    # 或者用另一种方式计算：(排名/总数) * 100
    # 这种方式是从高到低排名
    # df['rps'] = (df['rank'] / total) * 100
    
    # 筛选出RPS大于指定值的股票
    df = df[df['rps'] >= min_rps]
    
    # 添加股票名称
    stock_names_query = "SELECT ts_code, name FROM t_stock_basic"
    stock_names_df = pd.DataFrame(fetch_all(stock_names_query))
    if not stock_names_df.empty:
        df = pd.merge(df, stock_names_df, on='ts_code', how='left')
    
    # 提取纯数字的股票代码
    df['stock_code'] = df['ts_code'].str.split('.').str[0]
    
    # 格式化价格变动比率为2位小数
    df['price_change_rate'] = df['price_change_rate'].round(2)
    
    return df

def export_rps_to_csv(df, filename='rps_results.csv'):
    """
    将RPS计算结果导出到CSV文件
    
    参数:
    df (DataFrame): 包含RPS值的DataFrame
    filename (str): CSV文件名
    """
    if df.empty:
        print("没有RPS数据可导出")
        return
    
    # 只选择需要的列并按RPS值降序排序
    output_df = df[['stock_code', 'name', 'price_change_rate', 'rps']].copy()
    output_df = output_df.sort_values('rps', ascending=False)
    
    # 将RPS值四舍五入到整数
    output_df['rps'] = output_df['rps'].round(0).astype(int)
    
    # 导出到CSV
    output_df.to_csv(filename, index=False, encoding='utf-8-sig')  # 使用UTF-8带BOM编码以确保Excel正确显示中文
    print(f"RPS结果已导出到 {filename}")

def run_rps_strategy(start_date, end_date, period_days, min_rps=80, output_csv='rps_results.csv'):
    """
    运行RPS策略
    
    参数:
    start_date (str): 开始日期，格式为'YYYY-MM-DD'
    end_date (str): 结束日期，格式为'YYYY-MM-DD'
    period_days (int): 计算RPS的周期天数
    min_rps (float): 最小RPS值
    output_csv (str): 输出CSV文件名
    """
    # 创建临时表
    create_temp_table()
    
    # 填充临时表数据，指定计算周期
    populate_temp_table(start_date, end_date, period_days)
    
    # 计算RPS
    rps_df = calculate_rps(period_days, min_rps)
    
    # 导出RPS结果到CSV
    export_rps_to_csv(rps_df, output_csv)
    
    # 删除临时表
    execute_query("DROP TABLE IF EXISTS temp_stock_hq")
    
    print("RPS策略运行完成")
