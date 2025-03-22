import os
import pandas as pd
from sqlalchemy import text
from src.entities.temp_stock_hq import TempStockHQEntity
from src.entities.stock_entity import StockEntity
from src.utils.data_processing import get_end_date, get_trade_date_list

def calculate_price_rise_indicator(session, end_date, rise_interval, min_rise=None, max_rise=None):
    """
    计算指定周期内的价格涨幅指标，并可选择过滤特定涨幅范围内的股票
    
    参数:
    session: 数据库会话
    end_date: 结束日期
    rise_interval: 统计周期天数
    min_rise: 最小涨幅百分比，如 -10 表示下跌不超过10%
    max_rise: 最大涨幅百分比，如 10 表示上涨不超过10%
    """
    # 处理日期格式
    query_date_column = TempStockHQEntity.trade_date
    end_date = get_end_date(session, query_date_column, end_date)
    
    # 获取指定区间内的交易日期列表
    trade_date_list = get_trade_date_list(session, query_date_column, end_date, rise_interval)
    start_date = trade_date_list[-1][0]
    table_name = TempStockHQEntity.__tablename__
    
    # SQL查询获取指定区间内的最低价和当前收盘价
    rise_sql = f'''
    WITH min_prices AS (
        SELECT ts_code, MIN(low) as min_price
        FROM {table_name}
        WHERE trade_date BETWEEN :start_date AND :end_date
        GROUP BY ts_code
    )
    SELECT t.ts_code, t.close, m.min_price
    FROM (SELECT * FROM {table_name} WHERE trade_date = :end_date) t
    JOIN min_prices m ON t.ts_code = m.ts_code
    WHERE t.ts_code NOT LIKE :exclude_pattern
    '''
    
    rise_result = session.execute(
        text(rise_sql),
        {'start_date': start_date, 'end_date': end_date, 'exclude_pattern': '%BJ%'}
    ).fetchall()
    
    # 计算涨幅并排序
    rise_data = []
    for ts_code, close, min_price in rise_result:
        if min_price > 0:  # 避免除以0
            rise_percent = round((close - min_price) / min_price * 100, 2)
            
            # 如果指定了涨幅范围，则过滤数据
            if (min_rise is None or rise_percent >= min_rise) and (max_rise is None or rise_percent <= max_rise):
                rise_data.append((ts_code, close, min_price, rise_percent))
    
    # 按涨幅从高到低排序
    rise_data.sort(key=lambda x: x[3], reverse=True)
    
    # 获取股票基本信息
    code_list = [value[0] for value in rise_data]
    base_entity_list = session.query(StockEntity).filter(StockEntity.ts_code.in_(code_list)).all()
    code_info_map = {entity.ts_code: (entity.name, entity.industry) for entity in base_entity_list}
    
    # 准备输出数据
    rise_output = []
    for ts_code, close, min_price, rise_percent in rise_data:
        name, industry = code_info_map.get(ts_code, ('', ''))
        pure_code = ts_code.split('.')[0]
        rise_output.append((pure_code, name, close, min_price, rise_percent, industry))
    
    # 创建输出目录
    output_dir = os.path.join(os.getcwd(), 'res')
    os.makedirs(output_dir, exist_ok=True)
    
    # 构建文件名，包含涨幅范围信息
    range_info = ""
    if min_rise is not None or max_rise is not None:
        min_str = str(min_rise) if min_rise is not None else "-inf"
        max_str = str(max_rise) if max_rise is not None else "inf"
        range_info = f"-range-{min_str}-to-{max_str}"
    
    # 输出结果到CSV文件
    df = pd.DataFrame(
        rise_output,
        columns=['股票代码', '股票名称', '当前价', '最低价', '涨幅%', '所属行业']
    )
    output_file = os.path.join(output_dir, f"{end_date}-price-rise-{rise_interval}days{range_info}.csv")
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    print(f"已生成涨幅报告，共有{len(rise_output)}只股票在指定范围内")
    return df