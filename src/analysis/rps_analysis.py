import os
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from src.indicators.rps import calculate_rps_indicator
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

def generate_rps_industry_report(session, end_date, rps_interval=3, rps_threshold=90, use_pre_close=True):
    """
    Generate an HTML report analyzing RPS data by industry.
    
    Args:
        session: Database session
        end_date: End date in YYYYMMDD or YYYY-MM-DD format
        rps_interval: Interval in days
        rps_threshold: RPS threshold percentage
        use_pre_close: If True, use end.close - start.pre_close for calculation; otherwise use default calculation
        
    Returns:
        Path to the generated HTML report
    """
    # Get RPS data using the existing function with modified calculation method
    rps_results = calculate_rps_indicator(session, end_date, rps_interval, rps_threshold, use_pre_close=use_pre_close)
    
    # Convert to DataFrame for easier manipulation - use only the columns that are actually returned
    df = pd.DataFrame(rps_results, columns=['股票代码', '股票名称', '区间涨跌幅', 'RPS值', '所属行业'])
    
    # Replace 未知 industries with "其他" for better categorization
    df['所属行业'] = df['所属行业'].replace('未知', '其他')
    
    # Calculate start date - adjusting to include the end date in the interval count
    if len(end_date) == 10:  # YYYY-MM-DD format
        end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
    else:  # YYYYMMDD format
        end_date_obj = datetime.strptime(end_date, '%Y%m%d')
    
    # Subtract (interval-1) days instead of the full interval
    # This way, rps_interval=1 means just the end date, rps_interval=3 means end date plus 2 previous days
    start_date_obj = end_date_obj - timedelta(days=(rps_interval - 1))
    
    if len(end_date) == 10:  # YYYY-MM-DD format
        start_date = start_date_obj.strftime('%Y-%m-%d')
        display_end_date = end_date
    else:  # YYYYMMDD format
        start_date = start_date_obj.strftime('%Y%m%d')
        display_end_date = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:]}"
    
    display_start_date = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:]}" if len(start_date) == 8 else start_date
    
    # Group by industry and analyze
    industry_counts = df.groupby('所属行业').size().sort_values(ascending=False)
    industry_avg_rps = df.groupby('所属行业')['RPS值'].mean().sort_values(ascending=False)
    industry_avg_rise = df.groupby('所属行业')['区间涨跌幅'].mean().sort_values(ascending=False)
    
    # Prepare data for pie chart - top 5 industries + "其他"
    top_5_industries = industry_counts.head(5)
    other_industries_count = industry_counts.iloc[5:].sum() if len(industry_counts) > 5 else 0
    
    pie_labels = list(top_5_industries.index)
    pie_values = list(top_5_industries.values)
    
    if other_industries_count > 0:
        pie_labels.append("其他")
        pie_values.append(other_industries_count)
    
    # Create output directory
    output_dir = os.path.join(os.getcwd(), 'res', 'reports')
    os.makedirs(output_dir, exist_ok=True)
    
    # Get top performing stocks (top 20)
    top_stocks = df.sort_values('区间涨跌幅', ascending=False).head(20)
    
    # Generate HTML report
    html_content = []
    html_content.append(f"""
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>RPS行业分析报告 - {display_end_date}</title>
        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
        <style>
            body {{
                font-family: Arial, sans-serif;
                line-height: 1.6;
                margin: 0;
                padding: 20px;
                color: #333;
            }}
            h1, h2, h3 {{
                color: #2c3e50;
            }}
            table {{
                border-collapse: collapse;
                width: 100%;
                margin-bottom: 20px;
            }}
            th, td {{
                border: 1px solid #ddd;
                padding: 8px;
                text-align: left;
            }}
            th {{
                background-color: #f2f2f2;
            }}
            tr:nth-child(even) {{
                background-color: #f9f9f9;
            }}
            tr:hover {{
                background-color: #f1f1f1;
            }}
            .summary-box {{
                background-color: #f8f9fa;
                border-radius: 5px;
                padding: 15px;
                margin-bottom: 20px;
                box-shadow: 0 1px 3px rgba(0,0,0,0.1);
            }}
            .chart-container {{
                margin: 20px 0;
                height: 400px;
            }}
        </style>
    </head>
    <body>
        <h1>RPS行业分析报告</h1>
        <div class="summary-box">
            <h2>报告概览</h2>
            <p>开始日期: {display_start_date}</p>
            <p>结束日期: {display_end_date}</p>
            <p>分析周期: {rps_interval}天</p>
            <p>RPS阈值: {rps_threshold}</p>
            <p>符合条件的股票总数: {len(df)}</p>
            <p>覆盖行业数: {len(industry_counts)}</p>
        </div>
        
        <h2>股票涨幅榜 (Top 20)</h2>
        <table>
            <tr>
                <th>排名</th>
                <th>股票代码</th>
                <th>股票名称</th>
                <th>所属行业</th>
                <th>开始日期</th>
                <th>结束日期</th>
                <th>区间涨跌幅(%)</th>
                <th>RPS值</th>
            </tr>
    """)
    
    # Add rows for top performing stocks
    for i, (_, row) in enumerate(top_stocks.iterrows(), 1):
        html_content.append(f"""
            <tr>
                <td>{i}</td>
                <td>{row['股票代码']}</td>
                <td>{row['股票名称']}</td>
                <td>{row['所属行业']}</td>
                <td>{display_start_date}</td>
                <td>{display_end_date}</td>
                <td>{row['区间涨跌幅']:.2f}</td>
                <td>{row['RPS值']:.2f}</td>
            </tr>
        """)
    
    html_content.append("</table>")
    
    html_content.append(f"""
        <h2>行业分布饼图</h2>
        <div id="industry-pie-chart" class="chart-container"></div>
        
        <h2>行业分布</h2>
        <div id="industry-chart" class="chart-container"></div>
        
        <h2>行业平均RPS值</h2>
        <div id="rps-chart" class="chart-container"></div>
        
        <h2>行业平均涨幅</h2>
        <div id="rise-chart" class="chart-container"></div>
        
        <h2>行业股票分布</h2>
        <table>
            <tr>
                <th>行业</th>
                <th>股票数量</th>
                <th>平均RPS值</th>
                <th>平均涨幅(%)</th>
            </tr>
    """)
    
    # Add rows for each industry
    for industry in industry_counts.index:
        html_content.append(f"""
            <tr>
                <td>{industry}</td>
                <td>{industry_counts[industry]}</td>
                <td>{industry_avg_rps[industry]:.2f}</td>
                <td>{industry_avg_rise[industry]:.2f}</td>
            </tr>
        """)
    
    html_content.append("</table>")
    
    # Add tables for each industry
    for industry in industry_counts.index:
        industry_df = df[df['所属行业'] == industry].sort_values('RPS值', ascending=False)
        html_content.append(f"""
        <h2>{industry}行业股票列表</h2>
        <table>
            <tr>
                <th>股票代码</th>
                <th>股票名称</th>
                <th>区间涨跌幅(%)</th>
                <th>RPS值</th>
            </tr>
        """)
        
        for _, row in industry_df.iterrows():
            html_content.append(f"""
            <tr>
                <td>{row['股票代码']}</td>
                <td>{row['股票名称']}</td>
                <td>{row['区间涨跌幅']:.2f}</td>
                <td>{row['RPS值']:.2f}</td>
            </tr>
            """)
        
        html_content.append("</table>")
    
    # Add plotly charts JavaScript
    html_content.append("""
        <script>
            // Industry distribution chart
            var industryData = [
    """)
    
    for industry, count in industry_counts.items():
        html_content.append(f"{{ x: '{industry}', y: {count} }},")
    
    html_content.append("""
            ];
            
            var industryPlot = {
                x: industryData.map(item => item.x),
                y: industryData.map(item => item.y),
                type: 'bar',
                marker: {
                    color: 'rgba(60, 200, 120, 0.8)'  // Brighter green
                }
            };
            
            Plotly.newPlot('industry-chart', [industryPlot], {
                title: '行业股票数量分布',
                xaxis: { title: '行业' },
                yaxis: { title: '股票数量' }
            });
            
            // Industry Pie Chart
            var pieData = [{
                values: [""")
    
    # Add pie chart values
    html_content.append(",".join(str(value) for value in pie_values))
    
    html_content.append("""],
                labels: [""")
    
    # Add pie chart labels
    html_content.append(",".join(f"'{label}'" for label in pie_labels))
    
    html_content.append("""],
                type: 'pie',
                hole: 0.4,
                marker: {
                    colors: ['rgb(255, 99, 132)', 'rgb(54, 162, 235)', 'rgb(255, 206, 86)', 
                             'rgb(75, 192, 192)', 'rgb(153, 102, 255)', 'rgb(255, 159, 64)']  // Vibrant color palette
                }
            }];
            
            Plotly.newPlot('industry-pie-chart', pieData, {
                title: '前五大行业分布',
                height: 400,
                width: 700
            });
            
            // RPS chart
            var rpsData = [
    """)
    
    for industry, rps in industry_avg_rps.items():
        html_content.append(f"{{ x: '{industry}', y: {rps:.2f} }},")
    
    html_content.append("""
            ];
            
            var rpsPlot = {
                x: rpsData.map(item => item.x),
                y: rpsData.map(item => item.y),
                type: 'bar',
                marker: {
                    color: 'rgba(65, 105, 225, 0.8)'  // Brighter blue
                }
            };
            
            Plotly.newPlot('rps-chart', [rpsPlot], {
                title: '行业平均RPS值',
                xaxis: { title: '行业' },
                yaxis: { title: 'RPS值' }
            });
            
            // Rise chart
            var riseData = [
    """)
    
    for industry, rise in industry_avg_rise.items():
        html_content.append(f"{{ x: '{industry}', y: {rise:.2f} }},")
    
    html_content.append("""
            ];
            
            var risePlot = {
                x: riseData.map(item => item.x),
                y: riseData.map(item => item.y),
                type: 'bar',
                marker: {
                    color: 'rgba(255, 69, 0, 0.8)'  // Bright orange-red
                }
            };
            
            Plotly.newPlot('rise-chart', [risePlot], {
                title: '行业平均涨幅(%)',
                xaxis: { title: '行业' },
                yaxis: { title: '涨幅(%)' }
            });
        </script>
    </body>
    </html>
    """)
    
    # Write the report to a file
    output_filename = f"rps_industry_report_{end_date}_{rps_interval}days.html"
    output_path = os.path.join(output_dir, output_filename)
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write("".join(html_content))
    
    print(f"Report generated: {output_path}")
    return output_path
