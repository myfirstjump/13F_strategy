import dash
# import dash_core_components as dcc
from dash import dcc
# import dash_html_components as html
from dash import html
from dash.dependencies import Output, Input, State, ALL
from dash.exceptions import PreventUpdate
# import dash_table
from dash import dash_table
import plotly.express as px
import pandas as pd
import json
import ast
import time
import datetime
from datetime import timedelta

from pages import self_style

class DashBuilder(object):

    def __init__(self, stock_data):
        
        # self.df = pd.read_csv('https://gist.githubusercontent.com/chriddyp/5d1ea79569ed194d432e56108a04d188/raw/a9f9e8076b837d541398e999dcbac2b2826a81f8/gdp-life-exp-2007.csv')

        self.app = dash.Dash(__name__, suppress_callback_exceptions=True)#, external_stylesheets=self.external_stylesheets)
        self.app.config.suppress_callback_exceptions = True
        # self.app = dash.Dash(__name__)
        self.app.title = 'Stock Target Selection'
        self.colors = {
            'background': '#ffffff',
            'text': '#111111'
        }

        self.arrow_img = 'assets/arrow_img.png'
        self.clear_img = 'assets/all_clear_unclicked.png'
        self.start_img = 'assets/start_unclicked.png'

        self.app.layout = html.Div([
            html.Div([
                        html.H1('股票篩選器', style=self_style.header_text_style),
                        dcc.Store(
                            id='stored_data',
                            storage_type='memory',
                        ),
                        dcc.Store(
                            id='download_data',
                            storage_type='memory',
                        ),
                ],style=self_style.header_div_style), # header-div
            
            html.Div([
                dcc.Tabs([
                    dcc.Tab(label='股票篩選', children=[
                        # 工具1: 篩選股票
                        html.Div([

                            html.Div([
                                html.Div([ # menu-1
                                    html.Button(
                                        ["基本資訊　＞",],
                                        id='01-btn',
                                        n_clicks=0,
                                        title='展開基本資訊選項',
                                        style=self_style.menu_btn,
                                    ),                        
                                ],  
                                style=self_style.link_div_style),
                                # html.Br(),
                                html.Div([ # menu-2
                                    html.Button(
                                        ["股價條件　＞"],
                                        id='02-btn',
                                        title='展開股價條件選項',
                                        className='menu-btn'
                                    ),                        
                                ],
                                style=self_style.link_div_style),
                                # html.Br(),
                                html.Div([ # menu-3
                                    html.Button(
                                        ["成交量值　＞"],
                                        id='03-btn',
                                        title='展開成交量值選項',
                                        className='menu-btn'
                                    ),                        
                                ],
                                style=self_style.link_div_style),
                                # html.Br(),
                                html.Div([ # menu-4
                                    html.Button(
                                        ["法人籌碼　＞"],
                                        id='04-btn',
                                        title='展開法人籌碼選項',
                                        className='menu-btn'
                                    ),                        
                                ],
                                style=self_style.link_div_style),
                                # html.Br(),
                                html.Div([ # menu-5
                                    html.Button(
                                        ["信用交易　＞"],
                                        id='05-btn',
                                        title='展開信用交易選項',
                                        className='menu-btn'
                                    ),                        
                                ],
                                style=self_style.link_div_style),
                                # html.Br(),
                                html.Div([ # menu-6
                                    html.Button(
                                        ["公司營收　＞"],
                                        id='06-btn',
                                        title='展開公司營收選項',
                                        className='menu-btn'
                                    ),                        
                                ],                                
                                style=self_style.link_div_style),
                            ], style=self_style.menu_style), # menu

                            html.Div([

                                html.Div([ # filter-frame
                                    html.Div('請由左方加入篩選類別', style=self_style.frame_text_style),
                                    html.Div([], id="filter-content"),
                                ],style=self_style.filter_frame),
                                html.Div([ # condition-frame
                                    html.Div('您的選股條件', style=self_style.frame_text_style),
                                    html.Div([],
                                        id='dynamic-output-container',
                                        style=self_style.dynamic_output_container_style),
                                    html.Div([
                                        html.Img(src=self.start_img,
                                            id='selection-btn',
                                            style=self_style.selection_btn,
                                            className='selection-btn'),
                                        html.Img(src=self.clear_img,
                                            id='clear-all-btn',
                                            style=self_style.selection_btn,
                                            className='clear-btn')
                                    ], self_style.selection_btn_div_style),
                                ], style=self_style.condition_frame),

                                html.Div([
                                    html.Div(['篩選結果'], style=self_style.frame_text_style),
                                    
                                    dcc.Tabs(id='results-tabs', value='dynamic-selection-result-twse', # value是預設顯示值
                                        children=[
                                            dcc.Tab(label='台灣證券交易所 TWSE (上市)', id='dynamic-selection-result-twse', value='dynamic-selection-result-twse', style=self_style.result_words, selected_style=self_style.result_words_onclick),
                                            dcc.Tab(label='櫃買中心 TPEX (上櫃)', id='dynamic-selection-result-tpex', value='dynamic-selection-result-tpex', style=self_style.result_words, selected_style=self_style.result_words_onclick),
                                            dcc.Tab(label='上市 ETF', id='dynamic-selection-result-twse-etf', value='dynamic-selection-result-twse-etf', style=self_style.result_words, selected_style=self_style.result_words_onclick),
                                            dcc.Tab(label='上櫃 ETF', id='dynamic-selection-result-tpex-etf', value='dynamic-selection-result-tpex-etf', style=self_style.result_words, selected_style=self_style.result_words_onclick),
                                    ]),
                                    dcc.Loading(
                                        id='result-content-loading',
                                        type='default',
                                        children=html.Div([],id='result-content', style=self_style.result_content),
                                        color='red',
                                    ),
                                ], style=self_style.result_frame) # Results
                            ], style=self_style.inner_frame_style), # inner-frame
                        ], style=self_style.top_frame_style), # top-frame
                    ], style=self_style.top_tab, selected_style=self_style.top_tab_onclick),

                    dcc.Tab(label='個股查詢', children=[

                        # # 工具2: 個股查詢
                        # html.Div([
                        #         html.Div([
                        #                 dcc.Input(
                        #                         id='query_input',
                        #                         type='text',
                        #                         placeholder='請輸入股票代號或公司名稱',
                        #                         style=self_style.query_input_style
                        #                     ),
                        #                 html.Button(
                        #                     children=['查詢'],
                        #                     id='query-btn',
                        #                     style=self_style.query_btn,
                        #                     ),
                        #             ], style=self_style.query_block_style),
                        #         html.Div([], style=self_style.query_content_style),
                        # ], style=self_style.query_div),
                        
                    ], style=self_style.top_tab, selected_style=self_style.top_tab_onclick),
                ]),
            ]),

        ], style=self_style.top_div_style) # canvas-div

        self.app.run_server(host='127.0.0.1', debug=True, dev_tools_hot_reload=True)