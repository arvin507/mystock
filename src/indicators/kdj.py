import pandas as pd
import numpy as np

def calculate_kdj(df, high_col='high', low_col='low', close_col='close', n=9, m1=3, m2=3):
    """
    计算KDJ随机指标
    
    参数:
    df (DataFrame): 包含价格数据的DataFrame
    high_col (str): 包含最高价的列名
    low_col (str): 包含最低价的列名
    close_col (str): 包含收盘价的列名
    n (int): 计算RSV的周期
    m1 (int): K值平滑系数
    m2 (int): D值平滑系数
    
    返回:
    DataFrame: 添加了KDJ值的原始DataFrame
    """
    # 复制DataFrame以避免修改原始数据
    df_copy = df.copy()
    
    # 计算过去n个周期的最低价和最高价
    low_list = df_copy[low_col].rolling(window=n, min_periods=1).min()
    high_list = df_copy[high_col].rolling(window=n, min_periods=1).max()
    
    # 计算RSV (Raw Stochastic Value) - 未成熟随机值
    # RSV = (收盘价 - 最低价) / (最高价 - 最低价) * 100
    rsv = 100 * ((df_copy[close_col] - low_list) / (high_list - low_list))
    rsv = rsv.fillna(0)  # 处理除以0的情况
    
    # 计算K值 (K以50为初值)
    # K = 前一日K × (m1-1)/m1 + 今日RSV × 1/m1
    k = pd.Series(50, index=rsv.index)
    for i in range(1, len(rsv)):
        k[i] = (m1 - 1) / m1 * k[i-1] + 1 / m1 * rsv[i]
    
    # 计算D值 (D以50为初值)
    # D = 前一日D × (m2-1)/m2 + 今日K × 1/m2
    d = pd.Series(50, index=k.index)
    for i in range(1, len(k)):
        d[i] = (m2 - 1) / m2 * d[i-1] + 1 / m2 * k[i]
    
    # 计算J值
    # J = 3×K - 2×D
    j = 3 * k - 2 * d
    
    # 将KDJ值添加到数据框中
    df_copy['kdj_k'] = k
    df_copy['kdj_d'] = d
    df_copy['kdj_j'] = j
    
    return df_copy
