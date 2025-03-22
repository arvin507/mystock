import os
import pandas as pd
from sqlalchemy import text
from src.entities.temp_stock_hq import TempStockHQEntity
from src.entities.stock_entity import StockEntity
from src.utils.data_processing import get_end_date

def calculate_cross_ma_indicator(session, end_date, lookback_days=3):
    """
    计算股价在指定周期内上穿MA5或MA10的指标
    
    参数:
    session: 数据库会话
    end_date: 结束日期
    lookback_days: 回看的天数
    """
    # 处理日期格式
    query_date_column = TempStockHQEntity.trade_date
    end_date = get_end_date(session, query_date_column, end_date)
    table_name = TempStockHQEntity.__tablename__
    
    # SQL查询获取指定周期内的数据
    sql = f"""
    WITH date_range AS (
        SELECT DISTINCT trade_date
        FROM {table_name}
        WHERE trade_date <= :end_date
        ORDER BY trade_date DESC
        LIMIT :lookback_days
    )
    SELECT t.ts_code, t.trade_date, t.open, t.close,
           t.ma5, t.ma10,
           LAG(t.close) OVER (PARTITION BY t.ts_code ORDER BY t.trade_date) as prev_close,
           LAG(t.ma5) OVER (PARTITION BY t.ts_code ORDER BY t.trade_date) as prev_ma5,
           LAG(t.ma10) OVER (PARTITION BY t.ts_code ORDER BY t.trade_date) as prev_ma10
    FROM {table_name} t
    INNER JOIN date_range d ON t.trade_date = d.trade_date
    WHERE t.ts_code NOT LIKE '%BJ%'
    ORDER BY t.ts_code, t.trade_date DESC
    """
    
    result = session.execute(text(sql), {
        'end_date': end_date,
        'lookback_days': lookback_days
    }).fetchall()
    
    # 按股票代码分组处理数据
    stock_data = {}
    for row in result:
        ts_code = row[0]
        if ts_code not in stock_data:
            stock_data[ts_code] = []
        stock_data[ts_code].append(row)
    
    # 分析每只股票的均线上穿情况
    cross_ma_stocks = []
    for ts_code, data in stock_data.items():
        if len(data) < 2:  # 至少需要两天的数据来判断上穿
            continue
            
        # 检查是否在回看期内上穿MA5或MA10
        crossed_ma = []
        
        # 检查当天是否上穿
        today = data[0]
        if (today.prev_close is not None and today.close is not None and
            today.prev_ma5 is not None and today.ma5 is not None and
            today.prev_close < today.prev_ma5 and today.close > today.ma5):
            crossed_ma.append('MA5')
        
        if (today.prev_close is not None and today.close is not None and
            today.prev_ma10 is not None and today.ma10 is not None and
            today.ma5 is not None and
            today.prev_close < today.prev_ma10 and today.close > today.ma10 and
            today.close > today.ma5):
            crossed_ma.append('MA10')
        
        # 遍历每一天的数据，检查是否有上穿
        for i in range(len(data)-1):
            today = data[i]
            yesterday = data[i+1]
            
            # 检查MA5上穿
            if (all(v is not None for v in [yesterday.close, today.close, yesterday.ma5, today.ma5]) and
                yesterday.close < yesterday.ma5 and today.close > today.ma5 and
                'MA5' not in crossed_ma):
                crossed_ma.append('MA5')
            
            # 检查MA10上穿
            if (all(v is not None for v in [yesterday.close, today.close, yesterday.ma10, today.ma10, today.ma5]) and
                yesterday.close < yesterday.ma10 and today.close > today.ma10 and
                today.close > today.ma5 and
                'MA10' not in crossed_ma):
                crossed_ma.append('MA10')
        
        # 如果有上穿均线，添加到结果列表
        if crossed_ma:
            cross_ma_stocks.append((ts_code, ','.join(crossed_ma)))
    
    # 获取股票基本信息
    if cross_ma_stocks:
        code_list = [item[0] for item in cross_ma_stocks]
        base_entity_list = session.query(StockEntity).filter(StockEntity.ts_code.in_(code_list)).all()
        code_info_map = {entity.ts_code: (entity.name, entity.industry) for entity in base_entity_list}
        
        # 整理输出数据
        output_data = []
        for ts_code, cross_count in cross_ma_stocks:
            name, industry = code_info_map.get(ts_code, ('', ''))
            pure_code = ts_code.split('.')[0]
            output_data.append((pure_code, name, crossed_ma, industry))
        
        # 创建输出目录
        output_dir = os.path.join(os.getcwd(), 'res')
        os.makedirs(output_dir, exist_ok=True)
        
        # 输出结果到CSV文件
        df = pd.DataFrame(output_data, columns=['股票代码', '股票名称', '上穿均线', '所属行业'])
        output_file = os.path.join(output_dir, f"{end_date}-cross-ma.csv")
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        
        return output_data
    
    return []