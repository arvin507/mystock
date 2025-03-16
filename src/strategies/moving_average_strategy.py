import pandas as pd
import numpy as np
from src.database import fetch_all, execute_query, engine

def get_latest_trade_date():
    """
    获取数据库中最新的交易日期
    
    返回:
    date: 最新交易日期
    """
    query = "SELECT MAX(trade_date) as latest_date FROM t_stock_daily_hq"
    result = fetch_all(query)
    return result[0]['latest_date'] if result else None

def find_stocks_with_ma_uptrend(ma_periods=[5, 10, 20, 30, 60], days_to_check=5, min_trade_days=60):
    """
    查找趋势向上的股票，基于移动平均线关系
    
    参数:
    ma_periods: 要检查的移动平均线周期列表
    days_to_check: 连续多少天满足条件
    min_trade_days: 最少需要的交易日数据
    
    返回:
    DataFrame: 符合条件的股票列表
    """
    latest_date = get_latest_trade_date()
    
    if not latest_date:
        print("无法获取最新交易日期")
        return pd.DataFrame()
    
    # 构建用于检查移动平均线关系的查询
    # 我们将检查短期均线是否在长期均线之上
    conditions = []
    
    for i in range(len(ma_periods) - 1):
        short_ma = ma_periods[i]
        long_ma = ma_periods[i + 1]
        conditions.append(f"daily.ma{short_ma} > daily.ma{long_ma}")
    
    conditions_str = " AND ".join(conditions)
    
    # 构建查询，查找最近days_to_check天均满足条件的股票
    query = f"""
    WITH ConsistentStocks AS (
        SELECT 
            daily.ts_code,
            COUNT(*) as valid_days
        FROM 
            t_stock_daily_hq daily
        WHERE 
            daily.trade_date <= %s
            AND {conditions_str}
        GROUP BY 
            daily.ts_code
        HAVING 
            COUNT(*) >= %s
    )
    SELECT 
        cs.ts_code,
        latest.close,
        latest.trade_date,
        latest.ma5,
        latest.ma10,
        latest.ma20,
        latest.ma30,
        latest.ma60,
        sb.name
    FROM 
        ConsistentStocks cs
    JOIN 
        t_stock_basic sb ON cs.ts_code = sb.ts_code
    JOIN 
        (SELECT * FROM t_stock_daily_hq WHERE trade_date = %s) latest ON cs.ts_code = latest.ts_code
    WHERE 
        cs.valid_days >= %s
    ORDER BY 
        latest.pct_chg DESC
    """
    
    result = fetch_all(query, (latest_date, min_trade_days, latest_date, days_to_check))
    
    if not result:
        print(f"没有找到符合条件的股票")
        return pd.DataFrame()
    
    # 转换为DataFrame
    df = pd.DataFrame(result)
    
    # 添加额外的分析数据
    # 计算MA趋势强度：短期均线相对长期均线的百分比差距
    if not df.empty:
        # 计算MA5相对MA60的优势百分比
        df['ma_strength'] = ((df['ma5'] / df['ma60']) - 1) * 100
        df['ma_strength'] = df['ma_strength'].round(2)
        
        # 提取纯数字的股票代码
        df['stock_code'] = df['ts_code'].str.split('.').str[0]
    
    return df

def export_ma_stocks_to_csv(df, filename='ma_uptrend_stocks.csv'):
    """
    将移动平均线策略结果导出到CSV文件
    
    参数:
    df (DataFrame): 包含符合条件股票的DataFrame
    filename (str): CSV文件名
    """
    if df.empty:
        print("没有数据可导出")
        return
    
    # 选择需要的列并排序
    output_df = df[['stock_code', 'name', 'close', 'ma5', 'ma10', 'ma20', 'ma30', 'ma60', 'ma_strength']].copy()
    output_df = output_df.sort_values('ma_strength', ascending=False)
    
    # 导出到CSV
    output_df.to_csv(filename, index=False, encoding='utf-8-sig')
    print(f"移动平均线策略结果已导出到 {filename}")

def run_ma_strategy(output_csv='ma_uptrend_stocks.csv', ma_periods=[5, 10, 20, 30, 60], days_to_check=5):
    """
    运行移动平均线策略，寻找趋势向上的股票
    
    参数:
    output_csv (str): 输出CSV文件名
    ma_periods (list): 要检查的移动平均线周期列表
    days_to_check (int): 连续多少天满足条件
    """
    print(f"开始运行移动平均线策略...")
    print(f"查找移动平均线呈上升趋势的股票（{' > '.join(['MA'+str(p) for p in ma_periods])}）")
    print(f"要求连续{days_to_check}天满足条件")
    
    # 查找符合条件的股票
    ma_stocks = find_stocks_with_ma_uptrend(ma_periods, days_to_check)
    
    # 导出结果到CSV
    if not ma_stocks.empty:
        print(f"找到 {len(ma_stocks)} 只符合条件的股票")
        export_ma_stocks_to_csv(ma_stocks, output_csv)
    else:
        print("没有找到符合条件的股票")
    
    print("移动平均线策略运行完成")
