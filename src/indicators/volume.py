import pandas as pd
import numpy as np

def calculate_volume_indicators(df, volume_col='vol', close_col='close', periods=[5, 10, 20]):
    """
    计算基于成交量的指标
    
    参数:
    df (DataFrame): 包含价格和成交量数据的DataFrame
    volume_col (str): 包含成交量数据的列名
    close_col (str): 包含收盘价的列名
    periods (list): 成交量指标计算的周期列表
    
    返回:
    DataFrame: 添加了成交量指标值的原始DataFrame
    """
    # 复制DataFrame以避免修改原始数据
    df_copy = df.copy()
    
    # 计算每个周期的成交量移动平均线
    for period in periods:
        df_copy[f'vol_ma_{period}'] = df_copy[volume_col].rolling(window=period).mean()
    
    # 计算OBV(能量潮指标)
    # 如果收盘价上涨，OBV = 前一日OBV + 今日成交量
    # 如果收盘价下跌，OBV = 前一日OBV - 今日成交量
    # 如果收盘价不变，OBV = 前一日OBV
    obv = pd.Series(0, index=df_copy.index)
    for i in range(1, len(df_copy)):
        if df_copy[close_col].iloc[i] > df_copy[close_col].iloc[i-1]:
            obv[i] = obv[i-1] + df_copy[volume_col].iloc[i]
        elif df_copy[close_col].iloc[i] < df_copy[close_col].iloc[i-1]:
            obv[i] = obv[i-1] - df_copy[volume_col].iloc[i]
        else:
            obv[i] = obv[i-1]
    
    df_copy['obv'] = obv
    
    # 计算成交量变化率
    # 成交量变化率 = (当期成交量 - N期前成交量) / N期前成交量 * 100
    for period in periods:
        df_copy[f'vol_roc_{period}'] = df_copy[volume_col].pct_change(periods=period) * 100
    
    return df_copy
