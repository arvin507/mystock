import os
import pandas as pd
from sqlalchemy import text
from src.entities.temp_stock_hq import TempStockHQEntity
from src.entities.stock_entity import StockEntity
from src.utils.data_processing import get_end_date

def calculate_ma_indicator(session, end_date, ma_interval):
    # 处理日期格式
    query_date_column = TempStockHQEntity.trade_date
    end_date = get_end_date(session, query_date_column, end_date)
    table_name = TempStockHQEntity.__tablename__
    ma_sql = f'''
    SELECT ts_code, close, 
           COALESCE(ma5, 0) as ma5, 
           COALESCE(ma10, 0) as ma10, 
           COALESCE(ma20, 0) as ma20,
           COALESCE(ma30, 0) as ma30,
           COALESCE(ma60, 0) as ma60,
           COALESCE(ma120, 0) as ma120
    FROM {table_name}
    WHERE trade_date = :end_date
      AND ts_code NOT LIKE '%BJ%'
      AND close > COALESCE(ma5, 0)
      AND COALESCE(ma5, 0) >= COALESCE(ma10, 0)
      AND COALESCE(ma10, 0) >= COALESCE(ma20, 0)
      AND COALESCE(ma20, 0) >= COALESCE(ma30, 0)
      AND COALESCE(ma30, 0) >= COALESCE(ma120, 0)
    '''
    ma_result = session.execute(text(ma_sql), {'end_date': end_date}).fetchall()
    
    code_list = [value[0] for value in ma_result]
    base_entity_list = session.query(StockEntity).filter(StockEntity.ts_code.in_(code_list)).all()
    code_info_map = {entity.ts_code: (entity.name, entity.industry) for entity in base_entity_list}

    ma_output = []
    for value in ma_result:
        ts_code, close, ma5, ma10, ma20, ma30, ma60, ma120 = value
        name, industry = code_info_map.get(ts_code, ('', ''))
        
        # 提取纯数字代码
        pure_code = ts_code.split('.')[0]
        ma_output.append((pure_code, name, close, ma5, ma10, ma20, ma30, ma60, ma120, industry))
    
    # 创建输出目录
    output_dir = os.path.join(os.getcwd(), 'res')
    os.makedirs(output_dir, exist_ok=True)
    
    # 输出结果到CSV文件
    df = pd.DataFrame(ma_output, columns=['股票代码', '股票名称', '收盘价', 'MA5', 'MA10', 'MA20', 'MA30', 'MA60', 'MA120', '所属行业'])
    output_file = os.path.join(output_dir, f"{end_date}-ma.csv")
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    return ma_output


def get_stock_ma_by_date(session, ts_code, end_date):
    # 处理日期格式
    query_date_column = TempStockHQEntity.trade_date
    end_date = get_end_date(session, query_date_column, end_date)
    table_name = TempStockHQEntity.__tablename__
    ma_sql = f'''
    SELECT ts_code, close, 
           COALESCE(ma5, 0) as ma5, 
           COALESCE(ma10, 0) as ma10, 
           COALESCE(ma20, 0) as ma20,
           COALESCE(ma30, 0) as ma30,
           COALESCE(ma60, 0) as ma60,
           COALESCE(ma120, 0) as ma120
    FROM {table_name}
    WHERE trade_date = :end_date
      AND ts_code = :ts_code
      AND ts_code NOT LIKE '%BJ%'
    '''
    ma_result = session.execute(text(ma_sql), {'end_date': end_date, 'ts_code': ts_code}).fetchone()
    
    if ma_result is None:
        return None
    
    # 获取股票基本信息
    base_entity = session.query(StockEntity).filter(StockEntity.ts_code == ts_code).first()
    name = base_entity.name if base_entity else ''
    industry = base_entity.industry if base_entity else ''
    
    # 提取纯数字代码
    pure_code = ts_code.split('.')[0]
    
    # 创建输出目录
    output_dir = os.path.join(os.getcwd(), 'res')
    os.makedirs(output_dir, exist_ok=True)
    
    # 准备输出数据
    ma_output = [(pure_code, name, ma_result[1], ma_result[2], ma_result[3], ma_result[4], ma_result[5], ma_result[6], ma_result[7], industry)]
    
    # 输出结果到CSV文件
    df = pd.DataFrame(ma_output, columns=['股票代码', '股票名称', '收盘价', 'MA5', 'MA10', 'MA20', 'MA30', 'MA60', 'MA120', '所属行业'])
    output_file = os.path.join(output_dir, f"{end_date}-{pure_code}-ma.csv")
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    return {
        'ts_code': ma_result[0],
        'close': ma_result[1],
        'ma5': ma_result[2],
        'ma10': ma_result[3],
        'ma20': ma_result[4],
        'ma30': ma_result[5],
        'ma60': ma_result[6],
        'ma120': ma_result[7]
    }
