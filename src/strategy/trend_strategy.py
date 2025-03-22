import os
import pandas as pd
from sqlalchemy import text
from src.entities.temp_stock_hq import TempStockHQEntity
from src.entities.stock_entity import StockEntity
from src.utils.data_processing import get_end_date
from src.indicators.rps import calculate_rps_indicator
from src.indicators.ma import calculate_ma_indicator
from src.indicators.cross_ma import calculate_cross_ma_indicator
from src.indicators.high_price import calculate_high_price_indicator
from typing import List, Tuple

def calculate_trend_strategy(session, end_date, rps_interval=3, rps_threshold=90, ma_interval=3, lookback_days=4, high_price_interval=60):
    # 获取RPS指标结果
    rps_result = calculate_rps_indicator(session, end_date, rps_interval, rps_threshold)
    rps_codes = set(item[0] for item in rps_result)
    
    # 获取均线指标结果
    ma_result = calculate_ma_indicator(session, end_date, ma_interval)
    ma_codes = set(item[0] for item in ma_result)
    
    # 获取均线上穿指标结果
    cross_ma_result = calculate_cross_ma_indicator(session, end_date, lookback_days)
    cross_ma_codes = set(item[0] for item in cross_ma_result)
    
    # 获取创新高指标结果
    high_price_result = calculate_high_price_indicator(session, end_date, high_price_interval)
    high_price_codes = set(item[0] for item in high_price_result)
    
    # 取四个指标的交集
    trend_codes = rps_codes.intersection(ma_codes).intersection(cross_ma_codes).intersection(high_price_codes)
    
    # 获取股票基本信息
    code_list = [f"{code}.SZ" if code.startswith('0') or code.startswith('3') else f"{code}.SH" for code in trend_codes]
    base_entity_list = session.query(StockEntity).filter(StockEntity.ts_code.in_(code_list)).all()
    code_info_map = {entity.ts_code.split('.')[0]: (entity.name, entity.industry) for entity in base_entity_list}
    
    # 获取最新价格和涨跌幅
    table_name = TempStockHQEntity.__tablename__
    price_sql = f"""
    SELECT ts_code, close, pct_chg
    FROM {table_name}
    WHERE trade_date = :end_date
      AND ts_code IN :code_list
    """
    price_result = session.execute(text(price_sql), {'end_date': end_date, 'code_list': tuple(code_list)}).fetchall()
    price_map = {row[0].split('.')[0]: (row[1], row[2]) for row in price_result}
    
    # 整理输出结果
    trend_output = []
    for code in trend_codes:
        name, industry = code_info_map.get(code, ('', ''))
        close, pct_chg = price_map.get(code, (0, 0))
        # 获取RPS值
        rps_value = next((item[3] for item in rps_result if item[0] == code), 0)
        trend_output.append((code, name, close, pct_chg, rps_value, industry))
    
    # 按RPS值排序
    trend_output.sort(key=lambda x: x[4], reverse=True)
    
    # 创建日期目录
    date_dir = os.path.join(os.getcwd(), 'res', end_date)
    os.makedirs(date_dir, exist_ok=True)
    
    # 输出结果到CSV文件
    df = pd.DataFrame(trend_output, columns=['股票代码', '股票名称', '最新价', '涨跌幅', 'RPS值', '所属行业'])
    output_file = os.path.join(date_dir, 'trend.csv')
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    # 生成股票代码SVG图片
    generate_stock_codes_svg(trend_output, date_dir)
    
    return trend_output

def generate_stock_codes_svg(trend_output: List[Tuple], date_dir: str) -> None:
    """生成股票代码图片
    
    Args:
        trend_output: 趋势策略输出结果
        date_dir: 日期目录
    """
    from PIL import Image, ImageDraw, ImageFont
    import os
    
    # 图片配置
    width = 100
    height = 40
    font_size = 24
    margin = 10
    
    # 计算总高度
    total_height = height * len(trend_output)
    
    # 创建图片
    image = Image.new('RGB', (width, total_height), 'white')
    draw = ImageDraw.Draw(image)
    
    # 尝试加载Arial字体，如果不存在则使用默认字体
    try:
        font = ImageFont.truetype('Arial', font_size)
    except:
        font = ImageFont.load_default()
    
    # 添加每个股票代码
    for i, (code, _, _, _, _, _) in enumerate(trend_output):
        y_pos = height * i + height/2 - font_size/2
        draw.text((margin, y_pos), code, font=font, fill='black')
    
    # 保存JPEG文件
    output_file = os.path.join(date_dir, 'stock_codes.jpg')
    image.save(output_file, 'JPEG', quality=95)