from sqlalchemy import text
from src.entities.temp_stock_hq import TempStockHQEntity
from src.entities.stock_entity import StockEntity

def calculate_vol_indicator(session, start_date, end_date, lookback_days, vol_surge_ratio, max_vol_ratio, max_daily_vol_increase):
    table_name = TempStockHQEntity.__tablename__
    vol_sql = f'''
    SELECT ts_code, trade_date, vol, AVG(vol) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN {lookback_days} PRECEDING AND 1 PRECEDING) as avg_vol
    FROM {table_name}
    WHERE trade_date BETWEEN :start_date AND :end_date
    '''
    vol_result = session.execute(text(vol_sql), {'start_date': start_date, 'end_date': end_date}).fetchall()

    vol_code_list = list(set([value[0] for value in vol_result]))
    vol_base_entity_list = session.query(StockEntity).filter(StockEntity.ts_code.in_(vol_code_list)).all()
    vol_code_name_map = {entity.ts_code: entity.name for entity in vol_base_entity_list}

    vol_output = []
    for ts_code in vol_code_list:
        stock_data = [value for value in vol_result if value[0] == ts_code]
        vol_values = [value[2] for value in stock_data]
        avg_vol_values = [value[3] for value in stock_data]

        # 筛选满足成交量条件的股票
        if vol_values[-1] is not None and avg_vol_values[-1] is not None and vol_values[-2] is not None:
            if vol_values[-1] > vol_surge_ratio * avg_vol_values[-1] and vol_values[-1] < max_vol_ratio * avg_vol_values[-1] and vol_values[-1] < max_daily_vol_increase * vol_values[-2]:
                name = vol_code_name_map.get(ts_code, '')
                vol_output.append((name, ts_code))
    
    return vol_output
