import os

from py_module.config import Configuration
from py_module.strategies import Strategy13F
import dash
from dash import html
from dash import dcc
from dash import dash_table
from dash.dependencies import Output, Input, State, ALL
from dash.exceptions import PreventUpdate
# from dash_table.Format import Format, Group
import plotly.express as px
import plotly.graph_objs as go

import pymssql
import pandas as pd
import json
# import ast
import time
import datetime
from datetime import timedelta
import numpy as np

from pages import (
    self_style,
    query_sentence,
)

from flask import Flask

server = Flask(__name__)  # object to be referenced by WSGI handler

app = dash.Dash(server=server, suppress_callback_exceptions=True)#, external_stylesheets=external_stylesheets)
app.config.suppress_callback_exceptions = True
app.title = '13F Report Order Strategy'


# arrow_img = 'assets/arrow_img.png'
# clear_img = 'assets/all_clear_unclicked.png'
# start_img = 'assets/start_unclicked.png'

config_obj = Configuration()
# strategy_obj = Strategy13F()


def create_query_get_all_hedge_name(table):

    query = '''
    SELECT DISTINCT [HEDGE_FUND]
    FROM {}
    '''.format(table)
    return query

def get_all_price_date(price_table):

    query = '''
    SELECT DISTINCT [date]
    FROM {}
    '''.format(price_table)
    return query

def create_query_get_all_quarter(table):

    query = '''
    SELECT DISTINCT [QUARTER], [DATE_FILED]
    FROM {}
    WHERE [HEDGE_FUND] LIKE '%reinvest%'
    '''.format(table)
    return query



def sql_execute(query):

    if config_obj.LOCAL_FLAG:
        conn = pymssql.connect(host='localhost', user = 'myfirstjump', password='myfirstjump', database='US_DB')
    else:
        conn = pymssql.connect(host='localhost', user = 'stock_search', password='1qazZAQ!', database='STOCK_SKILL_DB')
    cursor = conn.cursor(as_dict=True)
    cursor.execute(query)
    # data = [row for row in cursor]
    data = []
    for row in cursor:
        data.append(row)
    cursor.close()
    conn.close()
    return data

query = create_query_get_all_hedge_name(config_obj.customized_holdings_data_table)
targets_hedge = sql_execute(query)
targets_hedge = pd.DataFrame(targets_hedge)['HEDGE_FUND'].values

query = get_all_price_date(config_obj.us_stock_price_table) # 為了取得時間欄位
all_date_list = sql_execute(query)
all_date_list = pd.DataFrame(all_date_list)['date'].values
us_sorted_dates = sorted(all_date_list)
us_sorted_dates = pd.to_datetime(us_sorted_dates)
min_date = min(us_sorted_dates)
max_date = max(us_sorted_dates)
print('美股歷史價格從{}到{}'.format(min_date, max_date))


query = create_query_get_all_quarter(config_obj.customized_fund_portfolio_table)
quarters_list = sql_execute(query)
quarters_list = pd.DataFrame(quarters_list)
# print(quarters_list)
quarters_list = quarters_list.sort_values(by=['DATE_FILED'], ascending=False)
quarters_list = quarters_list['QUARTER'].values
quarters_list = list(quarters_list)

app.layout = html.Div([
                html.Div([
                    html.H1('13F報告下單策略', style=self_style.header_text_style),
                ],style=self_style.header_div_style), # header-div
                
                dcc.Tabs([
                    dcc.Tab(label='下單建議', children=[
                        html.Div([
                            html.Div([
                                html.Div(['範圍選擇'], style=self_style.frame_text_style),
                                html.Div([
                                    dcc.Dropdown(
                                        id='hedge-picker',
                                        options=targets_hedge,
                                        value=targets_hedge[0],
                                        placeholder=targets_hedge[0],
                                        style=self_style.large_dropdown_style,
                                        clearable=False),
                                    ], style=self_style.dp_div_style),
                                html.Div([
                                    dcc.Dropdown(
                                        id='quarter-picker',
                                        options=quarters_list[:-1],
                                        value=quarters_list[0],
                                        placeholder=quarters_list[0], 
                                        style=self_style.large_dropdown_style,
                                        clearable=False),
                                    ], style=self_style.dp_div_style),
                            ]),
                            html.Div([
                                html.Div(['購買金額'], style=self_style.frame_text_style),
                                html.Div([
                                        dcc.Input(required = True,
                                            id='enter-cost',
                                            type='number',
                                            min=0,
                                            max=999999999,
                                            value=1000000,
                                            placeholder='1000000',
                                            style=self_style.large_input_style),
                                    ], style=self_style.dp_div_style),
                            ])
                        ]),#,style=self_style.header_div_style), # header-div


                        html.Div([
                                    html.Div(['下單建議'], style=self_style.frame_text_style),
                                    html.Div([
                                            html.Div(['原持股數量(上一季)'], id='ori-text',style=self_style.frame_text_style),
                                            html.Div(
                                                children=[], 
                                                id='original-stocks-list',
                                                style=self_style.result_content),
                                        ], style=self_style.content_div_style),
                                    html.Div([
                                            html.Div(['本次建議持股數量(本季)'], id='current-text',style=self_style.frame_text_style),
                                            html.Div(
                                                children=[], 
                                                id='recommand-stocks-list',
                                                style=self_style.result_content),
                                        ], style=self_style.content_div_style),
                                    html.Div([
                                            html.Div(['變化量'], style=self_style.frame_text_style),
                                            html.Div(
                                                children=[],
                                                id='stock-shares-differences',
                                                style=self_style.result_content),


                                        ], style=self_style.content_div_style),
                        ],style=self_style.header_div_style), # header-div
                        ]),
                    dcc.Tab(label='爬蟲狀態', children=[
                    ]),
                ]),
            ], style=self_style.top_div_style) # canvas-div

'''
Callback 1: 查詢建議買入額

'''
@app.callback(
    Output('original-stocks-list', 'children'),
    Output('recommand-stocks-list', 'children'),
    Output('stock-shares-differences', 'children'),
    Output('ori-text', 'children'),
    Output('current-text', 'children'),
    Input('hedge-picker', 'value'),
    Input('quarter-picker', 'value'),
    Input('enter-cost', 'value'),
    # Input('confirm-btn', 'n_clicks'),
)
def reaction(hedge_str, quarter_str, enter_cost):

    idx = quarters_list.index(quarter_str)
    ori_idx = idx+1 # 季度為DESC排序，所以越後面越舊
    ori_quarter_str = quarters_list[ori_idx]

    ori_invest_time = get_invest_time(ori_quarter_str).strftime('%Y-%m-%d')
    invest_time = get_invest_time(quarter_str).strftime('%Y-%m-%d')

    query = create_query_get_holdings_data(config_obj.customized_holdings_data_table, config_obj.us_stock_price_table, hedge_str, ori_quarter_str, ori_invest_time)
    ori_data = get_holdings_data(query)
    ori_data = adjust_shares_by_enter_cost(ori_data, enter_cost)
    ori_data = add_sequence_column(ori_data)

    query = create_query_get_holdings_data(config_obj.customized_holdings_data_table, config_obj.us_stock_price_table, hedge_str, quarter_str, invest_time)
    current_data = get_holdings_data(query)
    current_data = adjust_shares_by_enter_cost(current_data, enter_cost)
    current_data = add_sequence_column(current_data)

    shares_difference_table = shares_difference_between_quarters(ori_data, current_data)
    shares_difference_table = add_sequence_column(shares_difference_table)

    # 
    ori_text = ['原持股數量(上一季)\n13F報告:{}, 投資時間{}'.format(ori_quarter_str, ori_invest_time)]
    current_text = ['本次建議持股數量(本季)\n13F報告:{}, 投資時間{}'.format(quarter_str, invest_time)]
    return generate_table(ori_data), generate_table(current_data), generate_table(shares_difference_table), html.P(ori_text, style={'whiteSpace': 'pre-line'}), html.P(current_text, style={'whiteSpace': 'pre-line'}) #顯示上換行

def adjust_holdings_time(holdings_time, sorted_dates, next_day=True):
    '''
    function:
        在輸入時間點為13F報告公布時間時，該日不一定有開市，所以依據時間調整。
    input:
        -. holdings_time(string):  該季資料之報告日期
        -. sorted_dates(pd.Series(pd.datetime)):  price data所有日期，即有開市日期
    '''
    index = sorted_dates.searchsorted(holdings_time) # 找到日期在排序後的列表中的位置
    adjust_date  = sorted_dates[index] if index < len(sorted_dates) else sorted_dates[-1] # 如果日期正好在列表中，返回該日期；否則返回下一個最接近的日期
    # print('原始日期:', holdings_time)
    # print('index: ', index)
    # print('修正日期:', adjust_date)
    # print(sorted_dates[index-1], sorted_dates[index], sorted_dates[index+1], )
    '''依照實際情況，13F報告公布後隔天買入，故應使用index+1日(sorted_dates[index+1])；而若本來就沒有開市，則使用下個開市日(adjust_date)'''
    if next_day:
        if holdings_time != adjust_date:
            result_date = adjust_date
        else:
            result_date = sorted_dates[index+1]
    else:
        result_date = adjust_date
    # print('使用日期:', result_date)
    return result_date    

def get_invest_time(quarter):
    year = int(quarter.split()[1])
    if 'Q1' in quarter:
        invest_time =  datetime.datetime(year, 5, 15)
    elif 'Q2' in quarter:
        invest_time =  datetime.datetime(year, 8, 14)
    elif 'Q3' in quarter:
        invest_time =  datetime.datetime(year, 11, 14)
    elif 'Q4' in quarter:
        invest_time =  datetime.datetime(year+1, 2, 14)
    
    return adjust_holdings_time(invest_time, us_sorted_dates, next_day=True)

def create_query_get_holdings_data(table_customized_holdings, price_table, hedge_fund, quarter, date):

    query = '''
    SELECT tb_ch.[SYM], tb_ch.[Percentile], tb_ch.[SHARES], tb_price.[Open]
    FROM {} tb_ch WITH(NOLOCK)
	INNER JOIN {} tb_price WITH(NOLOCK) ON tb_ch.[SYM] = tb_price.[stock_id]
    WHERE tb_ch.[HEDGE_FUND] = '{}'
    AND tb_ch.[QUARTER] = '{}'
	AND tb_price.[date] = '{}'
    '''.format(table_customized_holdings, price_table, hedge_fund, quarter, date)
    return query

def get_holdings_data(query):
    data = sql_execute(query)
    data = pd.DataFrame(data)
    data = data[['SYM', 'Percentile','SHARES', 'Open']]
    data = data.sort_values(by=['SYM'], axis=0)
    # data['Percentile'] = round(data['Percentile'], 2)
    
    data = data.rename(columns={'SYM':'股票', 'Percentile':'佔比%', 'Open':'價格'})
    data = data.drop(columns=['SHARES'])

    return data

def add_sequence_column(df):
    """
    在 DataFrame 中添加一列名为 '項次' 的序列列，从 1 开始递增。

    参数:
    df (pd.DataFrame): 要处理的 DataFrame。

    返回:
    pd.DataFrame: 新的包含 '項次' 列的 DataFrame。
    """
    df['項次'] = range(1, len(df) + 1)
    cols = df.columns.tolist()  # 获取列名列表
    cols = ['項次'] + cols[:-1]
    df = df[cols]
    return df

def adjust_shares_by_enter_cost(data, enter_cost):

    data['股數'] = enter_cost * data['佔比%'] / data['價格']
    data['佔比%'] = round(data['佔比%'] * 100, 2)
    data = data.astype({'佔比%': str})
    data['佔比%'] = data['佔比%'] + '%'
    data['股數'] = np.floor(data['股數'])
    return data


def shares_difference_between_quarters(previous_holdings, holdings_data):

    previous_holdings = previous_holdings[['股票', '股數']]
    holdings_data = holdings_data[['股票', '股數']]

    # 將兩個資料表合併
    merged_data = holdings_data.merge(previous_holdings, on=['股票'], how='outer', suffixes=('_current', '_previous'))
    merged_data.fillna({'股數_current': 0, '股數_previous': 0}, inplace=True)
    merged_data = merged_data.astype({'股數_current': int, '股數_previous': int})
    # 計算持股數量變化
    merged_data['股數變化'] = merged_data['股數_current'] - merged_data['股數_previous']
    merged_data = merged_data[['股票', '股數變化']]
    return merged_data

def generate_table(stock_data, max_rows=5000):
    return dash_table.DataTable(
        columns=[{"name": i, "id": i} for i in stock_data.columns],
        data=stock_data.to_dict('records'),
        fixed_rows={'headers': True},  # 固定表頭

        # style_header : header, style_data : data, style_cell : cells & header一起調整
        style_header={
            'backgroundColor': 'grey',
            'fontWeight': 'bold',
        },
        style_data={},
        style_cell={'fontSize': '20px', 'height': 'auto', 'whiteSpace': 'normal'},
        style_table={'overflowX': 'auto', 'minWidth': '100%'},
        
        # 設置欄位自動調整大小
        style_cell_conditional=[
            {'if': {'column_id': c},
             'minWidth': '50px', 'maxWidth': '100px', 'width': '80px',
             'textAlign': 'right'}
            for c in stock_data.columns
        ],
        
        # 設置表頭寬度
        style_header_conditional=[
            {'if': {'column_id': c},
             'minWidth': '50px', 'maxWidth': '100px', 'width': '80px',
             'textAlign': 'center'}
            for c in stock_data.columns
        ],

        filter_action='native',
        sort_action='native',
    )



if __name__ == "__main__":
    app.run_server(host='127.0.0.1', debug=True, dev_tools_hot_reload=True)