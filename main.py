import os
from src.business.stock_service import fetch_and_save_stock_basic_data, fetch_and_save_daily_trade_data
from src.business.stock_data_checker import complete_stock_data
from src.database import initialize_database

# 初始化数据库
initialize_database()

# 示例用法
# 获取股票基本信息
# fetch_and_save_stock_basic_data()

# 获取并保存特定日期范围内的日线交易数据
start_date = '20240901'  # 修改为你的开始日期
end_date = '20250314'    # 修改为你的结束日期

# 常规数据获取 执行之前需要打开注释 执行数据获取的操作
# fetch_and_save_daily_trade_data(start_date, end_date)

# 检查并补充记录少于5条的股票数据
# complete_stock_data(min_records=5, start_date=start_date, end_date=end_date)