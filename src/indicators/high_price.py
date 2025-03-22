from sqlalchemy import text
import os
import pandas as pd
from src.entities.stock_entity import StockEntity
from src.entities.temp_stock_hq import TempStockHQEntity
from src.utils.data_processing import get_end_date, get_trade_date_list

def calculate_high_price_indicator(session, end_date, interval):
    """计算指定周期内收盘价创新高的股票
    
    参数:
    session: 数据库会话
    end_date: 结束日期
    interval: 周期天数
    """
    # 处理日期格式
    query_date_column = TempStockHQEntity.trade_date
    end_date = get_end_date(session, query_date_column, end_date)
    
    # 获取指定区间内的交易日期列表
    trade_date_list = get_trade_date_list(session, query_date_column, end_date, interval)
    start_date = trade_date_list[-1][0]
    table_name = TempStockHQEntity.__tablename__
    
    # SQL查询获取指定区间内的最高收盘价和当前收盘价
    high_sql = f'''
    WITH max_prices AS (
        SELECT ts_code, MAX(close) as max_close
        FROM {table_name}
        WHERE trade_date BETWEEN :start_date AND :end_date
        GROUP BY ts_code
    )
    SELECT t.ts_code, t.close
    FROM (SELECT * FROM {table_name} WHERE trade_date = :end_date) t
    JOIN max_prices m ON t.ts_code = m.ts_code
    WHERE t.ts_code NOT LIKE :exclude_pattern
    AND t.close = m.max_close
    '''
    
    high_result = session.execute(
        text(high_sql),
        {'start_date': start_date, 'end_date': end_date, 'exclude_pattern': '%BJ%'}
    ).fetchall()
    
    # 获取股票基本信息
    code_list = [value[0] for value in high_result]
    base_entity_list = session.query(StockEntity).filter(StockEntity.ts_code.in_(code_list)).all()
    code_info_map = {entity.ts_code: (entity.name, entity.industry) for entity in base_entity_list}
    
    # 准备输出数据
    high_output = []
    for ts_code, close in high_result:
        name, industry = code_info_map.get(ts_code, ('', ''))
        pure_code = ts_code.split('.')[0]
        high_output.append((pure_code, name, close, industry))
    
    # 创建输出目录
    output_dir = os.path.join(os.getcwd(), 'res')
    os.makedirs(output_dir, exist_ok=True)
    
    # 输出结果到CSV文件
    df = pd.DataFrame(
        high_output,
        columns=['股票代码', '股票名称', '当前价', '所属行业']
    )
    output_file = os.path.join(output_dir, f"{end_date}-high-price-{interval}days.csv")
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    return high_output