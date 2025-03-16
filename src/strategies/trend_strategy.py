import pandas as pd
import numpy as np
from src.database import fetch_all, execute_query, engine
from src.strategies.rps_strategy import run_rps_strategy, calculate_rps, create_temp_table, populate_temp_table

def get_stocks_with_high_rps(start_date, end_date, period_days, min_rps=80):
    """
    获取RPS高于指定值的股票
    
    参数:
    start_date (str): 开始日期，格式为'YYYY-MM-DD'
    end_date (str): 结束日期，格式为'YYYY-MM-DD'
    period_days (int): 计算RPS的周期天数
    min_rps (float): 最小RPS值
    
    返回:
    DataFrame: 包含高RPS股票的DataFrame
    """
    print("\n步骤1: 筛选RPS强势股...")
    
    # 创建临时表
    create_temp_table()
    
    # 填充临时表数据，指定计算周期
    populate_temp_table(start_date, end_date, period_days)
    
    # 计算RPS
    rps_df = calculate_rps(period_days, min_rps=min_rps)
    
    # 删除临时表
    execute_query("DROP TABLE IF EXISTS temp_stock_hq")
    
    if (rps_df.empty):
        print(f"未找到RPS > {min_rps}的股票")
        return pd.DataFrame()
    
    print(f"找到 {len(rps_df)} 只RPS > {min_rps}的股票")
    return rps_df

def get_ma_trend_stocks(max_ma5_ma10_diff_pct=5.0, max_price_ma5_diff_pct=3.0):
    """
    筛选符合均线趋势条件的股票：
    短期均线多头排列：5日 > 10日 > 20日
    股价在5日线上方
    同时排除:
    1. MA5与MA10差距过大的股票 (>max_ma5_ma10_diff_pct%)
    2. 价格与MA5差距过大的股票 (>max_price_ma5_diff_pct%)
    
    参数:
    max_ma5_ma10_diff_pct (float): MA5与MA10最大允许差距百分比
    max_price_ma5_diff_pct (float): 价格与MA5最大允许差距百分比
    
    返回:
    DataFrame: 符合均线趋势条件的股票
    """
    print("\n步骤2: 筛选均线趋势向上的股票...")
    
    latest_date_query = "SELECT MAX(trade_date) as latest_date FROM t_stock_daily_hq"
    latest_date_result = fetch_all(latest_date_query)
    latest_date = latest_date_result[0]['latest_date'] if latest_date_result else None
    
    if not latest_date:
        print("无法获取最新交易日期")
        return pd.DataFrame()
    
    # 构建查询，查找满足条件的股票
    query = """
    WITH StockMA AS (
        SELECT 
            t1.ts_code,
            t1.trade_date,
            t1.close,
            t1.ma5,
            t1.ma10,
            t1.ma20,
            t1.vol,
            t1.amount,
            AVG(t5.vol) as avg_vol_20,
            ((t1.ma5 / t1.ma10) - 1) * 100 as ma5_ma10_diff_pct,
            ((t1.close / t1.ma5) - 1) * 100 as price_ma5_diff_pct
        FROM 
            t_stock_daily_hq t1
        LEFT JOIN
            (SELECT ts_code, trade_date, vol FROM t_stock_daily_hq 
             WHERE trade_date BETWEEN DATE_SUB(%s, INTERVAL 30 DAY) AND %s) t5
        ON t1.ts_code = t5.ts_code
        WHERE 
            t1.trade_date = %s
        GROUP BY
            t1.ts_code, t1.trade_date, t1.close, t1.ma5, t1.ma10, t1.ma20, t1.vol, t1.amount
    )
    SELECT 
        s.*,
        b.name
    FROM 
        StockMA s
    JOIN
        t_stock_basic b ON s.ts_code = b.ts_code
    WHERE 
        -- 条件: 短期均线多头排列
        s.ma5 > s.ma10 AND s.ma10 > s.ma20
        -- 条件: 股价在5日线上方
        AND s.close > s.ma5
        -- 排除MA5与MA10差距过大的
        AND s.ma5_ma10_diff_pct <= %s
        -- 排除价格与MA5差距过大的
        AND s.price_ma5_diff_pct <= %s
    ORDER BY 
        (s.ma5 / s.ma20 - 1) * 100 DESC
    """
    
    result = fetch_all(query, (latest_date, latest_date, latest_date, max_ma5_ma10_diff_pct, max_price_ma5_diff_pct))
    
    df = pd.DataFrame(result)
    
    if df.empty:
        print("未找到符合均线趋势条件的股票")
        return pd.DataFrame()
    
    # 计算均线趋势强度
    df['ma_trend_strength'] = ((df['ma5'] / df['ma20']) - 1) * 100
    df['ma_trend_strength'] = df['ma_trend_strength'].round(2)
    
    print(f"找到 {len(df)} 只符合均线趋势条件的股票")
    return df

def get_volume_confirmed_stocks(lookback_days=10, vol_surge_ratio=1.5, max_vol_ratio=5.0, max_daily_vol_increase=3.0):
    """
    筛选满足成交量条件的股票：
    1. 最近交易日内出现放量上涨（成交量显著高于前期）
    2. 近期调整时成交量萎缩
    3. 排除成交量异常巨大的股票
    4. 排除当天成交量大于前一天成交量3倍的股票
    
    参数:
    lookback_days (int): 向前查看的交易日数
    vol_surge_ratio (float): 成交量相对20日均量的最小倍数阈值
    max_vol_ratio (float): 成交量相对20日均量的最大倍数阈值，超过此值视为异常巨量
    max_daily_vol_increase (float): 当天成交量相对前一天最大允许增幅倍数
    
    返回:
    DataFrame: 符合成交量条件的股票
    """
    print(f"\n步骤3: 筛选满足成交量条件的股票...")
    
    latest_date_query = "SELECT MAX(trade_date) as latest_date FROM t_stock_daily_hq"
    latest_date_result = fetch_all(latest_date_query)
    latest_date = latest_date_result[0]['latest_date'] if latest_date_result else None
    
    if not latest_date:
        print("无法获取最新交易日期")
        return pd.DataFrame()
    
    # 查找最近交易日内有放量上涨的股票，同时排除巨量和日间暴涨股票
    query = """
    WITH RecentVolume AS (
        SELECT 
            d1.ts_code,
            d1.trade_date,
            d1.close,
            d1.vol,
            d1.pct_chg,
            d1.ma_v_20,
            LAG(d1.vol, 1) OVER (PARTITION BY d1.ts_code ORDER BY d1.trade_date) as prev_vol,
            ROW_NUMBER() OVER (PARTITION BY d1.ts_code ORDER BY d1.trade_date DESC) as day_rank
        FROM 
            t_stock_daily_hq d1
        WHERE 
            d1.trade_date <= %s
            AND d1.trade_date >= DATE_SUB(%s, INTERVAL %s DAY)
    )
    SELECT 
        rv.ts_code,
        MAX(CASE WHEN rv.vol > rv.ma_v_20 * %s AND rv.pct_chg > 0 THEN 1 ELSE 0 END) as has_volume_surge,
        MAX(CASE WHEN rv.vol < rv.ma_v_20 AND rv.pct_chg < 0 THEN 1 ELSE 0 END) as has_volume_contraction,
        MAX(rv.vol / rv.ma_v_20) as max_vol_ratio,
        AVG(rv.vol / rv.ma_v_20) as avg_vol_ratio,
        MAX(rv.pct_chg) as max_daily_gain,
        MAX(CASE WHEN rv.prev_vol IS NOT NULL THEN rv.vol / rv.prev_vol ELSE 0 END) as max_daily_vol_increase,
        b.name
    FROM 
        RecentVolume rv
    JOIN
        t_stock_basic b ON rv.ts_code = b.ts_code
    GROUP BY 
        rv.ts_code, b.name
    HAVING 
        -- 至少有一天放量上涨
        has_volume_surge = 1
        -- 最大成交量放大倍数范围（排除巨量）
        AND max_vol_ratio >= %s AND max_vol_ratio <= %s
        -- 至少有一天量减价跌（筹码锁定）
        AND has_volume_contraction = 1
        -- 排除日间成交量暴增的股票
        AND (max_daily_vol_increase <= %s)
    ORDER BY 
        max_vol_ratio DESC
    """
    
    result = fetch_all(query, (latest_date, latest_date, lookback_days * 2, vol_surge_ratio, vol_surge_ratio, max_vol_ratio, max_daily_vol_increase))
    
    df = pd.DataFrame(result)
    
    if df.empty:
        print("未找到符合成交量条件的股票")
        return pd.DataFrame()
    
    # 提取纯数字的股票代码
    df['stock_code'] = df['ts_code'].str.split('.').str[0]
    
    print(f"找到 {len(df)} 只符合成交量条件的股票")
    return df

def get_technical_pattern_stocks():
    """
    筛选具有良好技术形态的股票：
    1. 突破整理平台或前高
    2. 回踩均线企稳反弹
    3. 排除长上影线的股票（最高价格-收盘价>收盘价-开盘价）
    
    返回:
    DataFrame: 符合技术形态的股票
    """
    print("\n步骤4: 筛选具有良好技术形态的股票...")
    
    latest_date_query = "SELECT MAX(trade_date) as latest_date FROM t_stock_daily_hq"
    latest_date_result = fetch_all(latest_date_query)
    latest_date = latest_date_result[0]['latest_date'] if latest_date_result else None
    
    if not latest_date:
        print("无法获取最新交易日期")
        return pd.DataFrame()
    
    # 查找突破前期高点或回踩MA20后企稳的股票，同时排除长上影线的股票
    query = """
    WITH PriceHistory AS (
        SELECT 
            d1.ts_code,
            d1.trade_date,
            d1.close,
            d1.open,
            d1.high,
            d1.low,
            d1.ma20,
            d1.vol,
            d1.ma_v_20,
            ROW_NUMBER() OVER (PARTITION BY d1.ts_code ORDER BY d1.trade_date DESC) as day_rank,
            MAX(d1.high) OVER (PARTITION BY d1.ts_code ORDER BY d1.trade_date ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) as recent_high,
            MIN(d1.low) OVER (PARTITION BY d1.ts_code ORDER BY d1.trade_date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) as recent_low,
            (d1.high - d1.close) as upper_shadow,
            (d1.close - d1.open) as body_size
        FROM 
            t_stock_daily_hq d1
        WHERE 
            d1.trade_date <= %s
            AND d1.trade_date >= DATE_SUB(%s, INTERVAL 60 DAY)
    )
    SELECT 
        ph1.ts_code,
        ph1.close,
        ph1.open,
        ph1.high,
        ph1.low,
        ph1.ma20,
        ph1.recent_high,
        ph1.recent_low,
        ph1.upper_shadow,
        ph1.body_size,
        ph1.vol / ph1.ma_v_20 as vol_ratio,
        -- 判断是否突破前高（近1-2天突破）
        CASE WHEN ph1.close > ph1.recent_high AND ph1.day_rank <= 2 THEN 1 ELSE 0 END as is_breakout,
        -- 判断是否回踩MA20企稳（近期低点接近MA20且现在收盘价上涨）
        CASE WHEN 
            ph1.recent_low < ph1.ma20 * 1.03 AND ph1.recent_low > ph1.ma20 * 0.97 
            AND ph1.close > ph1.ma20 AND ph1.day_rank <= 3
        THEN 1 ELSE 0 END as is_ma_bounce,
        b.name
    FROM 
        PriceHistory ph1
    JOIN
        t_stock_basic b ON ph1.ts_code = b.ts_code
    WHERE 
        ph1.day_rank = 1
        -- 排除长上影线的股票（上影线长度不大于实体长度）
        AND (ph1.high - ph1.close) <= (ph1.close - ph1.open)
        AND (
            -- 突破前期高点
            (ph1.close > ph1.recent_high AND ph1.vol > ph1.ma_v_20)
            OR
            -- 回踩均线企稳
            (ph1.recent_low < ph1.ma20 * 1.03 AND ph1.recent_low > ph1.ma20 * 0.97 
             AND ph1.close > ph1.ma20)
        )
    ORDER BY 
        (ph1.close / ph1.recent_high) DESC
    """
    
    result = fetch_all(query, (latest_date, latest_date))
    
    df = pd.DataFrame(result)
    
    if df.empty:
        print("未找到符合技术形态条件的股票")
        return pd.DataFrame()
    
    # 提取纯数字的股票代码
    df['stock_code'] = df['ts_code'].str.split('.').str[0]
    
    # 计算突破强度
    df['breakout_strength'] = ((df['close'] / df['recent_high']) - 1) * 100
    df['breakout_strength'] = df['breakout_strength'].round(2)
    
    print(f"找到 {len(df)} 只符合技术形态条件的股票")
    return df

def run_trend_strategy(start_date, end_date, period_days=20, min_rps=80, 
                      max_ma5_ma10_diff_pct=5.0, max_price_ma5_diff_pct=3.0,
                      vol_surge_ratio=1.5, max_vol_ratio=5.0, max_daily_vol_increase=3.0,
                      output_csv='trend_stocks.csv'):
    """
    运行综合趋势选股策略
    
    参数:
    start_date (str): 开始日期，格式为'YYYY-MM-DD'
    end_date (str): 结束日期，格式为'YYYY-MM-DD'
    period_days (int): 计算RPS的周期天数
    min_rps (float): 最小RPS值
    max_ma5_ma10_diff_pct (float): MA5与MA10最大允许差距百分比
    max_price_ma5_diff_pct (float): 价格与MA5最大允许差距百分比
    vol_surge_ratio (float): 成交量相对20日均量的最小倍数阈值
    max_vol_ratio (float): 成交量相对20日均量的最大倍数阈值
    max_daily_vol_increase (float): 当天成交量相对前一天最大允许增幅倍数
    output_csv (str): 输出CSV文件名
    """
    print("\n开始执行综合趋势选股策略...")
    
    # 1. 获取高RPS股票
    rps_stocks = get_stocks_with_high_rps(start_date, end_date, period_days, min_rps)
    if rps_stocks.empty:
        print("策略终止：没有找到高RPS股票")
        return
    
    # 2. 获取均线趋势向上的股票
    ma_stocks = get_ma_trend_stocks(max_ma5_ma10_diff_pct, max_price_ma5_diff_pct)
    if ma_stocks.empty:
        print("策略终止：没有找到均线趋势向上的股票")
        return
    
    # 3. 获取成交量确认的股票，排除巨量和异常放量股票
    vol_stocks = get_volume_confirmed_stocks(lookback_days=10, vol_surge_ratio=vol_surge_ratio, 
                                            max_vol_ratio=max_vol_ratio, max_daily_vol_increase=max_daily_vol_increase)
    if vol_stocks.empty:
        print("策略终止：没有找到成交量确认的股票")
        return
    
    # 4. 获取技术形态良好的股票
    pattern_stocks = get_technical_pattern_stocks()
    if pattern_stocks.empty:
        print("策略终止：没有找到技术形态良好的股票")
        return
    
    # 5. 取四个条件的交集的股票
    rps_codes = set(rps_stocks['ts_code'])
    ma_codes = set(ma_stocks['ts_code'])
    vol_codes = set(vol_stocks['ts_code'])
    pattern_codes = set(pattern_stocks['ts_code'])
    
    # 先找出符合至少3个条件的股票
    common_codes = set()
    
    # 满足所有4个条件的股票
    all_conditions = rps_codes.intersection(ma_codes).intersection(vol_codes).intersection(pattern_codes)
    common_codes.update(all_conditions)
    
    # 如果满足所有条件的股票不足，则找满足3个条件的
    if len(common_codes) < 5:
        # RPS + MA + VOL
        condition_set1 = rps_codes.intersection(ma_codes).intersection(vol_codes) - all_conditions
        # RPS + MA + PATTERN
        condition_set2 = rps_codes.intersection(ma_codes).intersection(pattern_codes) - all_conditions
        # RPS + VOL + PATTERN
        condition_set3 = rps_codes.intersection(vol_codes).intersection(pattern_codes) - all_conditions
        
        common_codes.update(condition_set1)
        common_codes.update(condition_set2)
        common_codes.update(condition_set3)
    
    if not common_codes:
        print("未找到符合综合趋势条件的股票")
        return
    
    print(f"\n找到 {len(common_codes)} 只符合综合趋势条件的股票")
    
    # 整合结果
    result_list = []
    for code in common_codes:
        stock_info = {
            'ts_code': code,
            'name': '',
            'rps': 0,
            'ma_trend_strength': 0,
            'vol_ratio': 0,
            'breakout_strength': 0,
            'conditions_met': []
        }
        
        # 填充各项指标值
        if code in rps_codes:
            stock_data = rps_stocks[rps_stocks['ts_code'] == code].iloc[0]
            stock_info['name'] = stock_data['name']
            stock_info['rps'] = round(stock_data['rps'])
            stock_info['conditions_met'].append('RPS')
        
        if code in ma_codes:
            stock_data = ma_stocks[ma_stocks['ts_code'] == code].iloc[0]
            if not stock_info['name']:
                stock_info['name'] = stock_data['name']
            stock_info['ma_trend_strength'] = stock_data['ma_trend_strength']
            stock_info['conditions_met'].append('MA')
        
        if code in vol_codes:
            stock_data = vol_stocks[vol_stocks['ts_code'] == code].iloc[0]
            if not stock_info['name']:
                stock_info['name'] = stock_data['name']
            stock_info['vol_ratio'] = round(stock_data['max_vol_ratio'], 2)
            stock_info['conditions_met'].append('VOL')
        
        if code in pattern_codes:
            stock_data = pattern_stocks[pattern_stocks['ts_code'] == code].iloc[0]
            if not stock_info['name']:
                stock_info['name'] = stock_data['name']
            stock_info['breakout_strength'] = stock_data['breakout_strength'] if 'breakout_strength' in stock_data else 0
            stock_info['conditions_met'].append('PATTERN')
        
        # 提取纯数字的股票代码
        stock_info['stock_code'] = code.split('.')[0]
        stock_info['conditions_met'] = '+'.join(stock_info['conditions_met'])
        
        result_list.append(stock_info)
    
    # 转换为DataFrame并按RPS降序排序
    result_df = pd.DataFrame(result_list)
    result_df = result_df.sort_values('rps', ascending=False)
    
    # 选择要导出的列
    output_cols = ['stock_code', 'name', 'rps', 'ma_trend_strength', 'vol_ratio', 'breakout_strength', 'conditions_met']
    output_df = result_df[output_cols]
    
    # 导出到CSV
    output_df.to_csv(output_csv, index=False, encoding='utf-8-sig')
    print(f"趋势选股结果已导出到 {output_csv}")
    
    return result_df
