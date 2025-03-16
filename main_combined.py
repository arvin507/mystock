from src.database import initialize_database
from src.strategies.combined_strategy import combine_rps_and_ma_strategy

# 初始化数据库
initialize_database()

# 设置参数
start_date = '20240901'  # 修改为你的开始日期
end_date = '20250314'    # 修改为你的结束日期
period_days = 30         # RPS计算周期天数
min_rps = 70             # 最小RPS值
ma_periods = [5, 10, 20]  # 移动平均线周期
days_to_check = 3        # 连续满足条件的天数
output_csv = 'combined_results.csv'  # 输出CSV文件名

# 运行结合RPS和MA的策略
combine_rps_and_ma_strategy(start_date, end_date, period_days, min_rps, ma_periods, days_to_check, output_csv)
