import pandas as pd
import numpy as np

def calculate_ma(df, close_col='close', periods=[5, 10, 20, 30, 60, 120]):
    """
    计算多个周期的简单移动平均线
    
    参数:
    df (DataFrame): 包含价格数据的DataFrame
    close_col (str): 包含收盘价的列名
    periods (list): 移动平均线的周期列表
    
    返回:
    DataFrame: 添加了简单移动平均线值的原始DataFrame
    """
    # 复制DataFrame以避免修改原始数据
    df_copy = df.copy()
    
    # 计算每个周期的简单移动平均线
    for period in periods:
        # 简单移动平均线 = 过去N日收盘价的算术平均值
        df_copy[f'ma_{period}'] = df_copy[close_col].rolling(window=period).mean()
    
    return df_copy

def calculate_ema(df, close_col='close', periods=[5, 10, 20, 30, 60, 120]):
    """
    计算多个周期的指数移动平均线
    
    参数:
    df (DataFrame): 包含价格数据的DataFrame
    close_col (str): 包含收盘价的列名
    periods (list): 指数移动平均线的周期列表
    
    返回:
    DataFrame: 添加了指数移动平均线值的原始DataFrame
    """
    # 复制DataFrame以避免修改原始数据
    df_copy = df.copy()
    
    # 计算每个周期的指数移动平均线
    for period in periods:
        # 指数移动平均线，更重视近期数据
        df_copy[f'ema_{period}'] = df_copy[close_col].ewm(span=period, adjust=False).mean()
    
    return df_copy
