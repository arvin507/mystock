import os
import pandas as pd
from sqlalchemy import text
from datetime import datetime
from src.entities.temp_stock_hq import TempStockHQEntity
from src.entities.stock_entity import StockEntity
from src.utils.data_processing import get_end_date, get_trade_date_list

def _format_date(date_str):
    """Convert date from YYYYMMDD to YYYY-MM-DD format if needed."""
    if len(date_str) == 8 and date_str.isdigit():
        return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
    return date_str

def calculate_rps_indicator(session, end_date, rps_interval, rps_threshold, use_pre_close=False):
    """Calculate RPS indicator for stocks.
    
    Args:
        session: Database session
        end_date: End date in YYYYMMDD or YYYY-MM-DD format
        rps_interval: Interval in days
        rps_threshold: RPS threshold percentage
        use_pre_close: If True, use pre_close instead of open price for calculation
    """
    # Convert date format if needed
    formatted_end_date = _format_date(end_date)
    
    # 处理日期格式
    query_date_column = TempStockHQEntity.trade_date
    end_date = get_end_date(session, query_date_column, formatted_end_date)
    
    # 获取指定区间内的交易日期列表
    trade_date_list = get_trade_date_list(session, query_date_column, end_date, rps_interval)
    start_date = trade_date_list[-1][0]
    table_name = TempStockHQEntity.__tablename__
    
    # Choose which calculation method to use based on use_pre_close parameter
    if use_pre_close:
        rps_sql = f'''
        SELECT start.ts_code, ((end.close - start.pre_close) / start.pre_close) as price_change 
        FROM (SELECT * FROM {table_name} WHERE trade_date = :start_date) start, 
             (SELECT * FROM {table_name} WHERE trade_date = :end_date) end 
        WHERE start.ts_code = end.ts_code 
          AND start.ts_code NOT LIKE :exclude_pattern 
        ORDER BY price_change DESC
        '''
    else:
        rps_sql = f'''
        SELECT start.ts_code, ((end.close - start.open) / start.open) as price_change 
        FROM (SELECT * FROM {table_name} WHERE trade_date = :start_date) start, 
             (SELECT * FROM {table_name} WHERE trade_date = :end_date) end 
        WHERE start.ts_code = end.ts_code 
          AND start.ts_code NOT LIKE :exclude_pattern 
        ORDER BY price_change DESC
        '''
    
    rps_result = session.execute(text(rps_sql), {'start_date': start_date, 'end_date': end_date, 'exclude_pattern': '%BJ%'}).fetchall()
    count = len(rps_result)

    code_list = [value[0] for value in rps_result]
    
    # 添加调试信息
    print(f"Found {len(code_list)} stocks with RPS data")
    
    # 确保使用正确的查询方式获取股票信息
    base_entity_list = session.query(StockEntity).filter(StockEntity.ts_code.in_(code_list)).all()
    
    print(f"Found {len(base_entity_list)} matching stocks in StockEntity table")
    
    # 记录一些不匹配的例子以便调试
    if len(base_entity_list) < len(code_list):
        missing_codes = set(code_list) - set(entity.ts_code for entity in base_entity_list)
        print(f"Missing {len(missing_codes)} codes in database. Examples: {list(missing_codes)[:5]}")
    
    code_info_map = {entity.ts_code: (entity.name, entity.industry) for entity in base_entity_list}

    rps_output = []
    for index, value in enumerate(rps_result, start=1):
        ts_code, change = value
        name, industry = code_info_map.get(ts_code, ('未知', '未知'))  # 使用默认值而不是空字符串
        rps = (1 - index / count) * 100
        if rps < rps_threshold:
            break
        # 提取纯数字代码
        pure_code = ts_code.split('.')[0]
        rps_output.append((pure_code, name, change * 100, rps, industry))
    
    # 创建输出目录
    output_dir = os.path.join(os.getcwd(), 'res')
    os.makedirs(output_dir, exist_ok=True)
    
    # 输出结果到CSV文件
    df = pd.DataFrame(rps_output, columns=['股票代码', '股票名称', '区间涨跌幅', 'RPS值', '所属行业'])
    output_file = os.path.join(output_dir, f"{end_date}-rps-{rps_interval}days.csv")
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    return rps_output
