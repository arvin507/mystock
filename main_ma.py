import pandas as pd
from src.database import initialize_database
from src.strategies.moving_average_strategy import run_ma_strategy

# 初始化数据库
initialize_database()

# 定义策略参数
ma_periods = [5, 10, 20, 30]  # 移动平均线周期 - 只使用5、10、20日均线
days_to_check = 3  # 连续满足条件的天数
output_csv = 'ma_uptrend_stocks.csv'  # 输出文件名

# 运行移动平均线策略
run_ma_strategy(output_csv, ma_periods, days_to_check)
