import pandas as pd
import numpy as np

def calculate_rsi(df, close_col='close', periods=14):
    """
    计算RSI(相对强弱指数)
    
    参数:
    df (DataFrame): 包含价格数据的DataFrame
    close_col (str): 包含收盘价的列名
    periods (int): RSI计算周期
    
    返回:
    DataFrame: 添加了RSI值的原始DataFrame
    """
    # 复制DataFrame以避免修改原始数据
    df_copy = df.copy()
    
    # 计算价格变化
    diff = df_copy[close_col].diff()
    
    # 创建上涨和下跌变动的列表
    # 上涨为正值，下跌为0
    up = diff.copy()
    up[up < 0] = 0
    
    # 下跌为正值(取负值的绝对值)，上涨为0
    down = -diff.copy()
    down[down < 0] = 0
    
    # 计算EWMA (指数加权移动平均线)
    # 计算上涨和下跌的平均值
    avg_gain = up.ewm(com=periods-1, adjust=False).mean()
    avg_loss = down.ewm(com=periods-1, adjust=False).mean()
    
    # 计算RS(相对强度)
    # RS = 平均上涨幅度 / 平均下跌幅度
    rs = avg_gain / avg_loss
    
    # 计算RSI
    # RSI = 100 - (100 / (1 + RS))
    df_copy[f'rsi_{periods}'] = 100 - (100 / (1 + rs))
    
    return df_copy
