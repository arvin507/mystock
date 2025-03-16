import pandas as pd
import numpy as np

def calculate_macd(df, close_col='close', fast_period=12, slow_period=26, signal_period=9):
    """
    计算MACD(移动平均收敛发散)指标
    
    参数:
    df (DataFrame): 包含价格数据的DataFrame
    close_col (str): 包含收盘价的列名
    fast_period (int): 快线EMA周期
    slow_period (int): 慢线EMA周期
    signal_period (int): 信号线周期
    
    返回:
    DataFrame: 添加了MACD值的原始DataFrame
    """
    # 复制DataFrame以避免修改原始数据
    df_copy = df.copy()
    
    # 计算短期和长期的指数移动平均线
    # EMA快线 - 通常为12日指数移动平均线
    ema_fast = df_copy[close_col].ewm(span=fast_period, adjust=False).mean()
    # EMA慢线 - 通常为26日指数移动平均线
    ema_slow = df_copy[close_col].ewm(span=slow_period, adjust=False).mean()
    
    # 计算MACD线 (DIF) = 快线EMA - 慢线EMA
    df_copy['macd_line'] = ema_fast - ema_slow
    
    # 计算信号线 (DEA) = MACD线的9日指数移动平均线
    df_copy['macd_signal'] = df_copy['macd_line'].ewm(span=signal_period, adjust=False).mean()
    
    # 计算MACD柱状图 (MACD Histogram) = MACD线 - 信号线
    df_copy['macd_hist'] = df_copy['macd_line'] - df_copy['macd_signal']
    
    return df_copy
