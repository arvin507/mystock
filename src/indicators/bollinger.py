import pandas as pd
import numpy as np

def calculate_bollinger_bands(df, close_col='close', window=20, num_std=2):
    """
    计算布林带(Bollinger Bands)
    
    参数:
    df (DataFrame): 包含价格数据的DataFrame
    close_col (str): 包含收盘价的列名
    window (int): 移动平均线的窗口大小
    num_std (int): 标准差的倍数，用于计算上下轨
    
    返回:
    DataFrame: 添加了布林带值的原始DataFrame
    """
    # 复制DataFrame以避免修改原始数据
    df_copy = df.copy()
    
    # 计算中轨(简单移动平均线)
    # 中轨 = N日移动平均线
    df_copy['bb_middle'] = df_copy[close_col].rolling(window=window).mean()
    
    # 计算标准差
    rolling_std = df_copy[close_col].rolling(window=window).std()
    
    # 计算上轨和下轨
    # 上轨 = 中轨 + K倍标准差
    df_copy['bb_upper'] = df_copy['bb_middle'] + (rolling_std * num_std)
    # 下轨 = 中轨 - K倍标准差
    df_copy['bb_lower'] = df_copy['bb_middle'] - (rolling_std * num_std)
    
    # 计算带宽(Bandwidth)
    # 带宽 = (上轨 - 下轨) / 中轨
    df_copy['bb_bandwidth'] = (df_copy['bb_upper'] - df_copy['bb_lower']) / df_copy['bb_middle']
    
    # 计算百分比B(Percent B)
    # 百分比B = (收盘价 - 下轨) / (上轨 - 下轨)
    df_copy['bb_percent_b'] = (df_copy[close_col] - df_copy['bb_lower']) / (df_copy['bb_upper'] - df_copy['bb_lower'])
    
    return df_copy
