import os

from py_module.config import Configuration
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

from pages import (
    self_style,
)

from flask import Flask

server = Flask(__name__)  # object to be referenced by WSGI handler

app = dash.Dash(server=server, suppress_callback_exceptions=True)#, external_stylesheets=external_stylesheets)
app.config.suppress_callback_exceptions = True
app.title = '13F Report Order Strategy'


arrow_img = 'assets/arrow_img.png'
clear_img = 'assets/all_clear_unclicked.png'
start_img = 'assets/start_unclicked.png'

config_obj = Configuration()
hedge_fund_portfolio_table = '[US_DB].[dbo].[HEDGE_FUND_PORTFOLIO]'
holdings_data_table = '[US_DB].[dbo].[HOLDINGS_DATA]'
us_stock_info_table = '[US_DB].[dbo].[USStockInfo]'
us_stock_price_table = '[US_DB].[dbo].[USStockPrice]'
us_stock_gics_table = '[US_DB].[dbo].[Company_GICS]'
tw_stock_price_table = '[STOCK_SKILL_DB].[dbo].[TW_STOCK_PRICE_Daily]'
customized_fund_portfolio_table = '[US_DB].[dbo].[CUSTOMIZED_HEDGE_FUND_PORTFOLIO]'
customized_holdings_data_table = '[US_DB].[dbo].[CUSTOMIZED_HOLDINGS_DATA]'

targets_hedge = [
    'I3C3_ctm_reinvest_0526',
    'I3C2_ctm_reinvest_0526',
    'I3C1_ctm_reinvest_0526',
    'I2C3_ctm_reinvest_0526',
    'I2C2_ctm_reinvest_0526',
    'I2C1_ctm_reinvest_0526',
    'I1C3_ctm_reinvest_0526',
    'I1C2_ctm_reinvest_0526',
    'I1C1_ctm_reinvest_0526',
    'I3C3_ctm_share_0526',
    'I3C2_ctm_share_0526',
    'I3C1_ctm_share_0526',
    'I2C3_ctm_share_0526',
    'I2C2_ctm_share_0526',
    'I2C1_ctm_share_0526',
    'I1C3_ctm_share_0526',
    'I1C2_ctm_share_0526',
    'I1C1_ctm_share_0526',
]


def get_all_price_date(price_table):

    query = '''
    SELECT DISTINCT [date]
    FROM {}
    '''.format(price_table)
    return query

def get_holdings_data(table_name, hedge_fund, quarter):

    query = '''
    SELECT *
    FROM {}
    WHERE [HEDGE_FUND] = '{}'
    AND [QUARTER] = '{}'
    '''.format(table_name, hedge_fund, quarter)
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

def generate_table(stock_data, max_rows=5000):
    return dash_table.DataTable(
                    columns = [{"name": i, "id": i} for i in stock_data.columns],
                    data=stock_data.to_dict('records'),
                    fixed_rows={'headers': True}, #固定表頭
                     
                    #style_header : header, style_data : data, style_cell : cells & header一起調整
                    style_header={
                        'backgroundColor': 'grey',
                        'fontWeight': 'bold',
                    }, 
                    style_data={}, 
                    style_cell={'fontSize': '20px', 'height': 'auto', 'whiteSpace': 'normal'}, 
                    style_table={'overflowX': 'auto', 'minWidth': '100%'},
                    # style_as_list_view=True, #移除column分隔線
                    # fill_width = False,
                    style_cell_conditional=[
                        {'if': {'column_id': 'Remark'},
                        'width': '15%'},
                        {'if': {'column_id': '產業別'},
                        'width': '20%'},
                    ],
                    filter_action='native',
                    sort_action='native',
                )




query = get_holdings_data(holdings_data_table, 'Abingworth LLP', 'Q3 2019') # 為了取得時間欄位
hedge_data = sql_execute(query)
hedge_data = pd.DataFrame(hedge_data)
hedge_data = hedge_data[['SYM', 'SHARES']]

query = get_all_price_date(us_stock_price_table) # 為了取得時間欄位
all_date_list = sql_execute(query)
all_date_list = pd.DataFrame(all_date_list)['date'].values
us_sorted_dates = sorted(all_date_list)
us_sorted_dates = pd.to_datetime(us_sorted_dates)
min_date = min(us_sorted_dates)
max_date = max(us_sorted_dates)


app.layout = html.Div([
    html.Div([
                html.H1('13F報告下單策略', style=self_style.header_text_style),
                dcc.Store(
                    id='stored_data',
                    storage_type='session',
                ),
        ],style=self_style.header_div_style), # header-div
    
    html.Div([
                html.Div(['範圍選擇'], style=self_style.frame_text_style),
                html.Div([
                        dcc.Dropdown(
                            id={'type':'dd',
                                'index': '0101'},
                            options=targets_hedge,
                            value=['I3C3_ctm_reinvest_0526'],
                            placeholder='基金選擇',
                            style=self_style.large_dropdown_style,
                            clearable=True),
                    ], style=self_style.dp_div_style),
                html.Div([
                        dcc.DatePickerSingle(
                            id='date-picker',
                            min_date_allowed=min_date,
                            max_date_allowed=max_date,
                            date=max_date,
                            placeholder='日期選擇',
                            display_format='YYYY年 第Q季',
                        ),
                    ], style=self_style.dp_div_style),
    ],style=self_style.header_div_style), # header-div


    html.Div([
                html.Div(['下單建議'], style=self_style.frame_text_style),
                html.Div([
                        html.Div(['原持股比例'], style=self_style.frame_text_style),
                        html.Div(generate_table(hedge_data), style=self_style.result_content),
                    ], style=self_style.dp_div_style),
                html.Div([
                        html.Div(['本次建議持股比例'], style=self_style.frame_text_style),
                        html.Div(generate_table(hedge_data), style=self_style.result_content),
                    ], style=self_style.dp_div_style),
                html.Div([
                        html.Div(['變化量'], style=self_style.frame_text_style),
                        html.Div(generate_table(hedge_data), style=self_style.result_content),


                    ], style=self_style.dp_div_style),
    ],style=self_style.header_div_style), # header-div
    
], style=self_style.top_div_style) # canvas-div

    
#     html.Div([
#         dcc.Tabs([
#             dcc.Tab(label='股票篩選', children=[
#                 # 工具1: 篩選股票
#                 html.Div([

#                     html.Div([
#                         html.Div([ # menu-1
#                             html.Button(
#                                 ["基本資訊　＞",],
#                                 id='01-btn',
#                                 n_clicks=0,
#                                 title='展開基本資訊選項',
#                                 style=self_style.menu_btn,
#                             ),                        
#                         ],  
#                         style=self_style.link_div_style),
#                         # html.Br(),
#                         html.Div([ # menu-2
#                             html.Button(
#                                 ["股價條件　＞"],
#                                 id='02-btn',
#                                 title='展開股價條件選項',
#                                 className='menu-btn'
#                             ),                        
#                         ],
#                         style=self_style.link_div_style),
#                         # html.Br(),
#                         html.Div([ # menu-3
#                             html.Button(
#                                 ["成交量值　＞"],
#                                 id='03-btn',
#                                 title='展開成交量值選項',
#                                 className='menu-btn'
#                             ),                        
#                         ],
#                         style=self_style.link_div_style),
#                         # html.Br(),
#                         html.Div([ # menu-4
#                             html.Button(
#                                 ["法人籌碼　＞"],
#                                 id='04-btn',
#                                 title='展開法人籌碼選項',
#                                 className='menu-btn'
#                             ),                        
#                         ],
#                         style=self_style.link_div_style),
#                         # html.Br(),
#                         html.Div([ # menu-5
#                             html.Button(
#                                 ["信用交易　＞"],
#                                 id='05-btn',
#                                 title='展開信用交易選項',
#                                 className='menu-btn'
#                             ),                        
#                         ],
#                         style=self_style.link_div_style),
#                         # html.Br(),
#                         html.Div([ # menu-6
#                             html.Button(
#                                 ["公司營收　＞"],
#                                 id='06-btn',
#                                 title='展開公司營收選項',
#                                 className='menu-btn'
#                             ),                        
#                         ],                                
#                         style=self_style.link_div_style),
#                     ], style=self_style.menu_style), # menu

#                     html.Div([

#                         html.Div([
#                             html.Div([ # filter-frame
#                                 html.Div('請由左方加入篩選類別', style=self_style.frame_text_style),
#                                 html.Div([], id="filter-content"),
#                             ],style=self_style.filter_frame),

#                             html.Div([ # condition-frame
#                                 html.Div('您的選股條件', style=self_style.frame_text_style),
#                                 html.Div([],
#                                     id='dynamic-output-container',
#                                     style=self_style.dynamic_output_container_style),
#                                 html.Div([
#                                     html.Button(['開始選股'],
#                                         id='selection-btn',
#                                         style=self_style.selection_btn,
#                                         className='selection-btn'),
#                                     html.Button(['全部清除'],
#                                         id='clear-all-btn',
#                                         style=self_style.selection_btn,
#                                         className='clear-btn')
#                                 ], self_style.selection_btn_div_style),
#                             ], style=self_style.condition_frame),
#                         ], style=self_style.cs_l21),

#                         html.Div([
#                             html.Div([
#                                 html.Div(['篩選結果'], style=self_style.frame_text_style),
                                
#                                 dcc.Tabs(id='results-tabs', value='dynamic-selection-result-twse', # value是預設顯示值
#                                     children=[
#                                         dcc.Tab(label='台灣證券交易所 TWSE (上市)', id='dynamic-selection-result-twse', value='dynamic-selection-result-twse', style=self_style.result_words, selected_style=self_style.result_words_onclick),
#                                         dcc.Tab(label='櫃買中心 TPEX (上櫃)', id='dynamic-selection-result-tpex', value='dynamic-selection-result-tpex', style=self_style.result_words, selected_style=self_style.result_words_onclick),
#                                         dcc.Tab(label='上市 ETF', id='dynamic-selection-result-twse-etf', value='dynamic-selection-result-twse-etf', style=self_style.result_words, selected_style=self_style.result_words_onclick),
#                                         dcc.Tab(label='上櫃 ETF', id='dynamic-selection-result-tpex-etf', value='dynamic-selection-result-tpex-etf', style=self_style.result_words, selected_style=self_style.result_words_onclick),
#                                 ]),
#                                 dcc.Loading(
#                                     id='result-content-loading',
#                                     type='default',
#                                     children=html.Div([], style=self_style.result_content),
#                                     color='red',
#                                 ),
#                             ], style=self_style.result_frame) # Results
#                         ], style=self_style.cs_l22),
                            
#                     ], style=self_style.inner_frame_style), # inner-frame
#                 ], style=self_style.top_frame_style), # top-frame
#             ], style=self_style.top_tab, selected_style=self_style.top_tab_onclick),

#         ]),
#     ]),
# ], style=self_style.top_div_style) # canvas-div




if __name__ == "__main__":
    app.run_server(host='127.0.0.1', debug=True, dev_tools_hot_reload=True)