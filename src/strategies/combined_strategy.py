import pandas as pd
from src.database import fetch_all, execute_query
from src.strategies.rps_strategy import run_rps_strategy, calculate_rps, create_temp_table, populate_temp_table
from src.strategies.moving_average_strategy import find_stocks_with_ma_uptrend

def combine_rps_and_ma_strategy(start_date, end_date, period_days, min_rps=80, ma_periods=[5, 10, 20], days_to_check=3, output_csv='combined_results.csv'):
    """
    结合RPS策略和移动平均线策略，选择既有高RPS又具备上升趋势的股票
    
    参数:
    start_date (str): 开始日期，格式为'YYYY-MM-DD'
    end_date (str): 结束日期，格式为'YYYY-MM-DD'
    period_days (int): 计算RPS的周期天数
    min_rps (float): 最小RPS值
    ma_periods (list): 要检查的移动平均线周期列表
    days_to_check (int): 移动平均线连续满足条件的天数
    output_csv (str): 输出CSV文件名
    """
    print("正在执行结合RPS和移动平均线的选股策略...")
    
    # 第一步：获取高RPS股票
    print("\n第一步: 计算RPS并筛选高RPS股票...")
    
    # 创建临时表
    create_temp_table()
    
    # 填充临时表数据，指定计算周期
    populate_temp_table(start_date, end_date, period_days)
    
    # 计算RPS
    rps_df = calculate_rps(period_days, min_rps=0)  # 设置min_rps=0获取所有股票的RPS
    
    # 删除临时表
    execute_query("DROP TABLE IF EXISTS temp_stock_hq")
    
    if rps_df.empty:
        print("未找到RPS数据，策略终止")
        return
    
    # 筛选出高RPS股票
    high_rps_stocks = rps_df[rps_df['rps'] >= min_rps].copy()
    print(f"找到 {len(high_rps_stocks)} 只RPS大于等于{min_rps}的股票")
    
    # 第二步：获取满足移动平均线条件的股票
    print("\n第二步: 寻找满足移动平均线条件的股票...")
    ma_stocks = find_stocks_with_ma_uptrend(ma_periods=ma_periods, days_to_check=days_to_check)
    
    if ma_stocks.empty:
        print("未找到满足移动平均线条件的股票，策略终止")
        return
    
    print(f"找到 {len(ma_stocks)} 只满足移动平均线条件的股票")
    
    # 第三步：取两个结果集的交集
    print("\n第三步: 寻找同时满足两种条件的股票...")
    # 将ts_code列转为集合，找出交集
    high_rps_codes = set(high_rps_stocks['ts_code'])
    ma_codes = set(ma_stocks['ts_code'])
    common_codes = high_rps_codes.intersection(ma_codes)
    
    if not common_codes:
        print("没有找到同时满足两种条件的股票")
        return
    
    print(f"找到 {len(common_codes)} 只同时满足高RPS和移动平均线条件的股票")
    
    # 从RPS结果中筛选出交集股票，并合并MA相关信息
    combined_df = high_rps_stocks[high_rps_stocks['ts_code'].isin(common_codes)].copy()
    ma_info = ma_stocks[ma_stocks['ts_code'].isin(common_codes)][['ts_code', 'close', 'ma5', 'ma10', 'ma20', 'ma_strength']]
    
    # 合并数据
    result_df = pd.merge(combined_df, ma_info, on='ts_code')
    
    # 排序和格式化
    result_df = result_df.sort_values('rps', ascending=False)
    result_df['rps'] = result_df['rps'].round(0).astype(int)
    result_df['price_change_rate'] = result_df['price_change_rate'].round(2)
    result_df['ma_strength'] = result_df['ma_strength'].round(2)
    
    # 选择需要输出的列
    output_df = result_df[['stock_code', 'name', 'close', 'price_change_rate', 'rps', 
                           'ma5', 'ma10', 'ma20', 'ma_strength']].copy()
    
    # 导出到CSV
    output_df.to_csv(output_csv, index=False, encoding='utf-8-sig')
    print(f"结合策略结果已导出到 {output_csv}")
    
    print("\n结合策略执行完成!")
    return result_df
