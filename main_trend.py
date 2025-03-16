from src.database import initialize_database
from src.strategies.trend_strategy import run_trend_strategy

# 初始化数据库
initialize_database()

# 设置参数
start_date = '20240901'  # 修改为你的开始日期
end_date = '20250314'    # 修改为你的结束日期
period_days = 20         # RPS计算周期天数
min_rps = 90             # 最小RPS值
max_ma5_ma10_diff_pct = 5.0  # MA5与MA10最大允许差距百分比
max_price_ma5_diff_pct = 5.0  # 价格与MA5最大允许差距百分比
vol_surge_ratio = 1.5    # 成交量相对20日均量的最小倍数阈值
max_vol_ratio = 5.0      # 成交量相对20日均量的最大倍数阈值
max_daily_vol_increase = 3.0  # 当天成交量相对前一天最大允许增幅倍数
output_csv = 'trend_stocks.csv'  # 输出CSV文件名

# 运行综合趋势选股策略
run_trend_strategy(
    start_date, 
    end_date, 
    period_days, 
    min_rps,
    max_ma5_ma10_diff_pct,
    max_price_ma5_diff_pct,
    vol_surge_ratio,
    max_vol_ratio,
    max_daily_vol_increase,
    output_csv
)
