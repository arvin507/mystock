from sqlalchemy import text
from ..entities.temp_stock_hq import TempStockHQEntity
from ..utils.data_processing import get_end_date
import os

def analyze_cross_ma_failure(session, ts_code, end_date, lookback_days=3):
    """
    分析指定股票在回看期内每一天是否满足cross_ma条件
    
    参数:
    session: 数据库会话
    ts_code: 股票代码
    end_date: 结束日期
    lookback_days: 回看的天数
    """
    # 处理日期格式
    query_date_column = TempStockHQEntity.trade_date
    end_date = get_end_date(session, query_date_column, end_date)
    table_name = TempStockHQEntity.__tablename__
    
    # SQL查询获取指定股票在指定周期内的数据
    sql = f"""
    WITH date_range AS (
        SELECT DISTINCT trade_date
        FROM {table_name}
        WHERE trade_date <= :end_date
        ORDER BY trade_date DESC
        LIMIT :lookback_days
    )
    SELECT t.ts_code, t.trade_date, t.open, t.close,
           t.ma5, t.ma10,
           LAG(t.close) OVER (PARTITION BY t.ts_code ORDER BY t.trade_date) as prev_close,
           LAG(t.ma5) OVER (PARTITION BY t.ts_code ORDER BY t.trade_date) as prev_ma5,
           LAG(t.ma10) OVER (PARTITION BY t.ts_code ORDER BY t.trade_date) as prev_ma10
    FROM {table_name} t
    INNER JOIN date_range d ON t.trade_date = d.trade_date
    WHERE t.ts_code = :ts_code
    ORDER BY t.trade_date DESC
    """
    
    result = session.execute(text(sql), {
        'end_date': end_date,
        'lookback_days': lookback_days,
        'ts_code': ts_code
    }).fetchall()
    
    if not result:
        return f"未找到股票{ts_code}在{end_date}前{lookback_days}天的数据"
    
    analysis = []
    # 分析每一天的数据
    for i in range(len(result)):
        day = result[i]
        analysis_day = {
            'trade_date': day.trade_date,
            'open': day.open,
            'close': day.close,
            'ma5': day.ma5,
            'ma10': day.ma10,
            'cross_ma5': False,
            'cross_ma10': False,
            'reasons': []
        }
        
        # 检查当天数据是否完整
        if None in [day.close, day.ma5, day.ma10, day.open]:
            analysis_day['reasons'].append("数据不完整，无法进行分析")
            analysis.append(analysis_day)
            continue
        
        # 检查MA5上穿条件 - 开盘价低于MA5且收盘价高于MA5
        if day.ma5 is not None:
            if day.open < day.ma5 and day.close > day.ma5:
                analysis_day['cross_ma5'] = True
                analysis_day['reasons'].append(f"满足MA5上穿条件: 开盘价({day.open:.2f}) < MA5({day.ma5:.2f}), 收盘价({day.close:.2f}) > MA5({day.ma5:.2f})")
            else:
                if day.open >= day.ma5:
                    analysis_day['reasons'].append(f"开盘价({day.open:.2f})已在MA5({day.ma5:.2f})之上，不满足上穿条件")
                if day.close <= day.ma5:
                    analysis_day['reasons'].append(f"收盘价({day.close:.2f})未突破MA5({day.ma5:.2f})")
        
        # 检查MA10上穿条件 - 开盘价低于MA10且收盘价高于MA10和MA5
        if day.ma10 is not None:
            if day.open < day.ma10 and day.close > day.ma10 and day.close > day.ma5:
                analysis_day['cross_ma10'] = True
                analysis_day['reasons'].append(f"满足MA10上穿条件: 开盘价({day.open:.2f}) < MA10({day.ma10:.2f}), 收盘价({day.close:.2f}) > MA10({day.ma10:.2f})且>MA5({day.ma5:.2f})")
            else:
                if day.open >= day.ma10:
                    analysis_day['reasons'].append(f"开盘价({day.open:.2f})已在MA10({day.ma10:.2f})之上，不满足上穿条件")
                if day.close <= day.ma10:
                    analysis_day['reasons'].append(f"收盘价({day.close:.2f})未突破MA10({day.ma10:.2f})")
                elif day.close <= day.ma5:
                    analysis_day['reasons'].append(f"收盘价({day.close:.2f})未突破MA5({day.ma5:.2f})，不满足MA10上穿条件")
        
        # 同时保留前一日收盘与当日均线的对比分析
        if day.prev_close is not None and day.prev_ma5 is not None:
            if day.prev_close < day.prev_ma5 and day.close > day.ma5 and not analysis_day['cross_ma5']:
                analysis_day['reasons'].append(f"满足前收盘上穿MA5: 前收盘({day.prev_close:.2f}) < 前MA5({day.prev_ma5:.2f}), 当前收盘({day.close:.2f}) > 当前MA5({day.ma5:.2f})")
        
        if day.prev_close is not None and day.prev_ma10 is not None:
            if day.prev_close < day.prev_ma10 and day.close > day.ma10 and day.close > day.ma5 and not analysis_day['cross_ma10']:
                analysis_day['reasons'].append(f"满足前收盘上穿MA10: 前收盘({day.prev_close:.2f}) < 前MA10({day.prev_ma10:.2f}), 当前收盘({day.close:.2f}) > 当前MA10({day.ma10:.2f})")
        
        if not analysis_day['reasons']:
            analysis_day['reasons'].append("无法完成上穿分析（可能缺少数据）")
        
        analysis.append(analysis_day)
    
    # 生成HTML分析报告
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <title>均线上穿分析报告</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            h1 {{ color: #333; }}
            table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
            th, td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
            th {{ background-color: #f5f5f5; }}
            tr:nth-child(even) {{ background-color: #f9f9f9; }}
            .success {{ color: #28a745; }}
            .warning {{ color: #dc3545; }}
            .reasons {{ margin-top: 8px; }}
            .reason-item {{ margin: 4px 0; }}
            .cross-yes {{ background-color: #d4edda; }}
            .cross-no {{ background-color: #f8d7da; }}
            .highlight {{ font-weight: bold; }}
            .summary {{ margin: 20px 0; padding: 15px; background-color: #f8f9fa; border-left: 5px solid #007bff; }}
        </style>
    </head>
    <body>
        <h1>股票{ts_code}在{end_date}前{lookback_days}天的均线上穿分析报告</h1>
        <div class="summary">
            <p>本报告分析了股票在回看期内每一天是否满足均线上穿条件。上穿条件定义为：</p>
            <ul>
                <li><span class="highlight">MA5上穿</span>: 当日开盘价低于MA5，收盘价高于MA5</li>
                <li><span class="highlight">MA10上穿</span>: 当日开盘价低于MA10，收盘价高于MA10且高于MA5</li>
            </ul>
        </div>
        <table>
            <tr>
                <th>交易日期</th>
                <th>开盘价</th>
                <th>收盘价</th>
                <th>MA5</th>
                <th>MA10</th>
                <th>MA5上穿</th>
                <th>MA10上穿</th>
                <th>分析结果</th>
            </tr>
    """
    
    for day in analysis:
        reasons_html = "<div class='reasons'>"
        for reason in day['reasons']:
            color_class = 'success' if '满足' in reason else 'warning'
            reasons_html += f"<div class='reason-item {color_class}'>{reason}</div>"
        reasons_html += "</div>"
        
        ma5_class = 'cross-yes' if day['cross_ma5'] else 'cross-no'
        ma10_class = 'cross-yes' if day['cross_ma10'] else 'cross-no'
        
        # 处理可能为空的值
        open_val = 'N/A' if day['open'] is None else f"{day['open']:.2f}"
        close_val = 'N/A' if day['close'] is None else f"{day['close']:.2f}"
        ma5_val = 'N/A' if day['ma5'] is None else f"{day['ma5']:.2f}"
        ma10_val = 'N/A' if day['ma10'] is None else f"{day['ma10']:.2f}"
        
        html += f"""
            <tr>
                <td>{day['trade_date']}</td>
                <td>{open_val}</td>
                <td>{close_val}</td>
                <td>{ma5_val}</td>
                <td>{ma10_val}</td>
                <td class="{ma5_class}">{'是' if day['cross_ma5'] else '否'}</td>
                <td class="{ma10_class}">{'是' if day['cross_ma10'] else '否'}</td>
                <td>{reasons_html}</td>
            </tr>
        """
    
    html += """
        </table>
    </body>
    </html>
    """
    
    # 创建分析报告目录
    output_dir = os.path.join(os.getcwd(), 'res', 'analysis')
    os.makedirs(output_dir, exist_ok=True)
    
    # 生成分析报告文件
    output_file = os.path.join(output_dir, f"{end_date}-{ts_code}-cross-ma-analysis.html")
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html)
    
    print(f"分析报告已生成：{output_file}")
    return html