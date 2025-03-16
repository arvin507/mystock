import pandas as pd
from src.database import initialize_database
from src.strategies.rps_strategy import run_rps_strategy

# 初始化数据库
initialize_database()

# 运行RPS策略
start_date = '20240901'  # 修改为你的开始日期
end_date = '20250314'    # 修改为你的结束日期
period_days = 3         # RPS计算周期天数
min_rps = 95             # 最小RPS值
output_csv = 'rps_results.csv'  # 输出CSV文件名

run_rps_strategy(start_date, end_date, period_days, min_rps, output_csv)
