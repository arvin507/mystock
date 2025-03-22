from re import S
from requests import get
from src.indicators.rps import calculate_rps_indicator
from src.db.database import session
from src.indicators.ma import calculate_ma_indicator
from src.indicators.ma import get_stock_ma_by_date
from src.indicators.cross_ma import calculate_cross_ma_indicator
from src.indicators.price_rise import calculate_price_rise_indicator
from src.indicators.high_price import calculate_high_price_indicator
from src.indicators.cross_ma_analysis import analyze_cross_ma_failure
from src.analysis.rps_analysis import generate_rps_industry_report

from src.strategy.trend_strategy import calculate_trend_strategy



end_date = '20250321'    # 修改为你的结束日期
rps_interval = 1
rps_threshold = 90

# calculate_rps_indicator(session=session,end_date=end_date,rps_interval=3,rps_threshold=90)

# 生成RPS行业分析报告
generate_rps_industry_report(session=session, end_date=end_date, rps_interval=rps_interval, rps_threshold=rps_threshold)

# calculate_ma_indicator(session=session,end_date=end_date,ma_interval=3)

# get_stock_ma_by_date(session=session,end_date=end_date,ts_code='002105.SZ')

# calculate_cross_ma_indicator(session=session,end_date=end_date,lookback_days=3)


# calculate_trend_strategy(session=session,end_date=end_date)

# calculate_price_rise_indicator(session=session,end_date=end_date,rise_interval=60,min_rise=20,max_rise=40)

# calculate_high_price_indicator(session=session,end_date=end_date,interval=60)

# analyze_cross_ma_failure(session=session,ts_code='002105.SZ',end_date=end_date,lookback_days=4)

