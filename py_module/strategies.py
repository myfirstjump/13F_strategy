from py_module.config import Configuration

import pymssql
import pandas as pd
import numpy as np
import sys
# np.set_printoptions(threshold=sys.maxsize)
import copy
import os
import datetime

from pyxirr import xirr
# from backtesting import Backtest, Strategy
# from backtesting.lib import crossover

# from backtesting.test import SMA, GOOG


# class SmaCross(Strategy):
#     def init(self):
#         price = self.data.Close
#         self.ma1 = self.I(SMA, price, 10)
#         self.ma2 = self.I(SMA, price, 20)

#     def next(self):
#         if crossover(self.ma1, self.ma2):
#             self.buy()
#         elif crossover(self.ma2, self.ma1):
#             self.sell()


# bt = Backtest(GOOG, SmaCross, cash=20_000, commission=.002,
#               exclusive_orders=True)
# stats = bt.run()
# print(stats)
# bt.plot()

# print(GOOG.head(10))

class Strategy13F(object):

    def __init__(self):
        self.config_obj = Configuration()
        self.hedge_fund_portfolio_table = '[US_DB].[dbo].[HEDGE_FUND_PORTFOLIO]'
        self.holdings_data_table = '[US_DB].[dbo].[HOLDINGS_DATA]'
        self.us_stock_info_table = '[US_DB].[dbo].[USStockInfo]'
        self.us_stock_price_table = '[US_DB].[dbo].[USStockPrice]'
        self.us_stock_gics_table = '[US_DB].[dbo].[Company_GICS]'
        self.tw_stock_price_table = '[STOCK_SKILL_DB].[dbo].[TW_STOCK_PRICE_Daily]'

        # 找到price data中的date欄位，對日期進行排序，找到最大的日期
        query = self.get_all_price_date(self.us_stock_price_table) # 為了取得時間欄位
        all_date_list = self.sql_execute(query)
        all_date_list = pd.DataFrame(all_date_list)['date'].values
        us_sorted_dates = sorted(all_date_list)
        self.us_sorted_dates = pd.to_datetime(us_sorted_dates)
        self.min_date = min(self.us_sorted_dates)
        self.max_date = max(self.us_sorted_dates)
        print('美股歷史價格從{}到{}'.format(self.min_date, self.max_date))

        query = self.get_all_price_date(self.tw_stock_price_table) # 為了取得時間欄位
        all_date_list = self.sql_execute(query)
        all_date_list = pd.DataFrame(all_date_list)['date'].values
        tws_sorted_dates = sorted(all_date_list)
        self.tws_sorted_dates = pd.to_datetime(tws_sorted_dates)
        self.tws_min_date = min(self.tws_sorted_dates)
        self.tws_max_date = max(self.tws_sorted_dates)
        print('TWS歷史價格從{}到{}'.format(self.tws_min_date, self.tws_max_date))

    def back_test_flow(self):
        '''
        美股13F投資策略回測程序
            0. 定義Output obj、統計用obj:
                -. summary_table: pd.DataFrame
                    同summary_data的資料，但是更方便處理欄位關係。
                -. corr_analysis_data: list of dict
                    用來儲存需要進行相關性分析資料 {'stock_id': None, 'spread_ratio': None, 'scaling': None}
                -. null_sym_counter: int
                    計算holdings報告中的SYM，無法對應到price table的數量，作為後續調整參考使用。
                -. base_13F_date_list: list
                    列舉13F報告所有based date(截止日5/15, 8/14, 11/14, 2/14)，供後續抓取想比較的個股資料比對用。
            1. Read DB data
                -. hedge fund data: fund_data
                -. date list which have the prices: sorted_dates, max_date(最後一天，用來計算最後統計量)
            2. 各hedge fund計算迴圈
                2.1. 定義迴圈內參數:
                    -. summary_data: list of dict
                        {'date': holdings_time, '市值': market_value, '加碼': scaling_in_sum, '減碼': scaling_out_sum, 'XIRR':xirr}
                        主要輸出的物件，list每個元素是某hedge fund 在13F報告時間點的市值、加碼量、減碼量、XIRR計算值。
                    -. previous_holdings: pd.DataFrame
                        用來儲存上一個13F報告的holdings，主要用來計算shares差異。
                    -. xirr_calculate_dict: {'date':[], 'amounts':[]}
                        用來記錄holdings time和amounts，最後用來輸入至pyxirr計算XIRR值。
                2.2. 調整fund_data:
                    依據條件篩選fund_data中的records。(詳細如each_fund_data_adjust())
                2.3. 各Quarter計算迴圈:
                    2.3.1 定義迴圈內參數:
                        -. hedge_fund_data = {'date': None, '市值': None, '加碼': None, '減碼': None, 'XIRR': None}
                            即要放在主要輸出summary_data中的元素，每一季的統計數據。
                    2.3.2 調整holdings_time:
                        由於13F報告日期不一定有開市，故調整至下一個開市日期。(詳細如adjust_holdings_time())
                    2.3.3 讀取holdings data，並進行特殊處理(詳細如holdings_data_adjust())
                    2.3.4 IF-ELSE語句分別處理第一季/其他季
                        
                    2.3.5 以xirr_calculate_dict計算XIRR值
                    2.3.6 將本季度holdings data暫存，作為下一季度計算使用。(previous_holdings)
                    2.3.7 將統計數值回存至summary_data
                2.4 計算當前持股市值。
            3. 加入其他想比較的個股。(S&P500、0050.TW)
                由於TWS與US的開市時間不同，包裝一個函數能夠修改base_13F_date_list內元素，調整為有開市的時間，才找得到price。(詳細如individual_stock_summary())
            4. 加入自定義基金比較
            5. 輸出資料表格
        '''

        print('Execute Hedge Fund Backtest Flow...')
        '''0. 定義Output obj、統計用obj'''
        ### Final Output Form
        
        summary_table = None
        null_sym_counter = 0
        base_13F_date_list = []

        '''1. Read DB data'''
        query = self.create_query_data_table(self.hedge_fund_portfolio_table)
        fund_data = self.sql_execute(query)
        fund_data = pd.DataFrame(fund_data)
        hedge_fund_list = fund_data['HEDGE_FUND'].unique()
        # hedge_fund_list = ['Appaloosa', ]
        hedge_fund_list = list(hedge_fund_list)
        # hedge_fund_list = ['Scion Asset Management']
        hedge_fund_list.remove('Citadel Advisors')
        hedge_fund_list.remove('Renaissance Technologies')
        hedge_fund_list.remove('Millennium Management')

        print("總共包含{}個對沖基金資料".format(len(hedge_fund_list)))
        print('Hedge Funds:', )
        print(hedge_fund_list)

        '''2. 各hedge fund計算迴圈'''
        for idx, hedge_fund in enumerate(hedge_fund_list):
            '''2.1. 定義迴圈內參數'''
            summary_data = [] # 包含項目: {'date': holdings_time, '市值': market_value, '加碼': scaling_in_sum, '減碼': scaling_out_sum, 'XIRR':xirr}
            corr_analysis_table = None #TBD
            previous_holdings = None
            xirr_calculate_dict = {'date':[], 'amounts':[]}
            previous_holdings_time = None
            previous_sym_str = tuple() #TBD
            '''2.2. 調整fund_data'''
            each_fund_data = self.each_fund_data_adjust(fund_data, hedge_fund)
            quarters_list = each_fund_data['QUARTER'].values
            date_list = each_fund_data['DATE_FILED'].values # 進場時間點使用該基金13F公布時間
            filing_list = each_fund_data['FILING_ID'].values
            if idx == 0:
                base_13F_dates = each_fund_data['BASE_DATE'].values# 進場時間點使用13F公布截止時間
                base_13F_date_list = pd.to_datetime(base_13F_dates, unit='ns')
                base_13F_date_list = [str(date) for date in base_13F_date_list.tolist()]

            print(" === === === 第{}個對沖基金：{}，包含{}個季度資料。 ".format(idx+1, hedge_fund, len(quarters_list)))
            # print(each_fund_data)
            '''2.3. 各Quarter計算迴圈'''
            for idx_q, (quarter, holdings_time, filing_number) in enumerate(zip(quarters_list, date_list, filing_list)):
                '''2.3.1 定義回圈內參數'''
                hedge_fund_data = {'date': None, '市值': None, '加碼': None, '減碼': None, 'XIRR': None}
                
                '''2.3.2 調整holdings_time'''
                holdings_time = self.adjust_holdings_time(holdings_time, self.us_sorted_dates) # 以13F報告公布期限為基準(5/15, 8/14, 11/14, 2/14)
                # print("     第{}個季度：{}，時間為{}".format(idx_q+1, quarter, holdings_time))

                '''2.3.3 讀取holdings data'''
                query = self.create_query_holdings(hedge_fund, quarter, filing_number)
                holdings_data = self.sql_execute(query)
                holdings_data = pd.DataFrame(holdings_data)
                holdings_data = self.holdings_data_adjust(holdings_data)
                '''
                        SYM                   ISSUER_NAME         CL      CUSIP  ...      HEDGE_FUND  QUARTER  FORM_TYPE           FILING_ID
                0     GME             GAMESTOP CORP NEW       CL A  36467W109  ...  Robotti Robert  Q4 2015     13F-HR  000114036116052554
                1    PSMT                PRICESMART INC        COM  741511109  ...  Robotti Robert  Q4 2015     13F-HR  000114036116052554
                2    AEIS          ADVANCED ENERGY INDS        COM  007973100  ...  Robotti Robert  Q4 2015     13F-HR  000114036116052554
                '''
                '''2.3.4 IF-ELSE語句分別處理第一季/其他季
                        2.3.4.1 若為第一季
                            2.3.4.1.1 確認有哪些股票代碼，存為sym_str，空值則計數後存入null_sym_counter。
                            2.3.4.1.2 以holdings time和sym_str查詢當時股價。(create_query_get_open_price_by_date_n_sym())
                            2.3.4.1.3 計算market value: 以holdings_data:SHARE、price_data:Open相乘得出股票市值。(market_value_by_join_holdings_and_price())
                            2.3.4.1.4 第一季加減碼為0。
                        2.3.4.2 若為其他季度
                            2.3.4.2.1 計算本季度與上一季度的SHARE差值，主要用以計算加減碼金額。(shares_difference_between_quarters())
                            2.3.4.2.2 確認有哪些股票代碼，存為sym_str，空值則計數後存入null_sym_counter。
                            2.3.4.2.3 以holdings time和sym_str查詢當時股價。(create_query_get_open_price_by_date_n_sym())
                            2.3.4.2.4 計算市值/加碼/減碼(calculate_scaling_in_and_out())
                '''
                if idx_q > 0: #扣除第一季，每季要計算的內容
                    shares_data = self.shares_difference_between_quarters(previous_holdings, holdings_data)
                    '''
                          SYM  SHARES_current  SHARES_previous  shares_change
                    0     AGN         2937121          4261406       -1324285
                    1    ETP1        11961842         11961842              0
                    2     WPZ         9966502          9487301         479201
                    '''
                    null_sym_counter = null_sym_counter + shares_data['SYM'].isna().sum()
                    sym_str = shares_data['SYM'].dropna().values
                    
                    # print("SYMs:", sym_str)
                    if len(sym_str) == 1:
                        sym_str = str(sym_str)
                        sym_str = sym_str.replace('[', '(')
                        sym_str = sym_str.replace(']', ')')
                    else:
                        sym_str = tuple(sym_str)
                    
                    query = self.create_query_get_open_price_by_date_n_sym(sym_str, holdings_time)
                    price_data = self.sql_execute(query)
                    price_data = pd.DataFrame(price_data)
                    # print('price_data:')
                    # print(price_data)
                    '''
                            date   SYM    Open
                        0   2016-11-14  AAPL   26.93
                        1   2016-11-14   ALL   69.76
                        2   2016-11-14    AY   16.98
                    '''
                    market_value, scaling_in, scaling_out, scaling_even = self.calculate_scaling_in_and_out(shares_data, price_data)
                    # print("Shares Increased:", scaling_in)
                    # print("Shares Decreased:", scaling_out)
                    # print("Shares Unchanged:", scaling_even)
                    scaling_in_sum = sum([i for i in  scaling_in.values()])
                    scaling_out_sum = sum([i for i in  scaling_out.values()])
                    
                    xirr_calculate_dict['date'].append(holdings_time)
                    xirr_calculate_dict['amounts'].append(-(scaling_in_sum - scaling_out_sum))

                    ### correlation analysis TBD
                    current_sym_str = holdings_data['SYM'].dropna().values 
                    current_sym_str = tuple(current_sym_str)
                    # print("SYMs:", current_sym_str)            
                    # print("previous_sym_str:", previous_sym_str)
                    sym_str_combined = set(previous_sym_str) | set(current_sym_str)
                    sym_str_combined = tuple(sym_str_combined)
                    price_ratio_data = self.get_price_change_ratio(previous_holdings_time, holdings_time, sym_str_combined)
                    corr_analysis_data = self.arrange_corr_analysis_data(price_ratio_data, shares_data)
                    if corr_analysis_table is None:
                        corr_analysis_table = corr_analysis_data
                    else:
                        corr_analysis_table = pd.concat([corr_analysis_table, corr_analysis_data], ignore_index=True)
                    
                else: #第一季要計算的內容
                    null_sym_counter = null_sym_counter + holdings_data['SYM'].isna().sum()
                    current_sym_str = holdings_data['SYM'].dropna().values # TBD: 確認Drop數量
                    if len(current_sym_str) == 1:
                        current_sym_str = str(current_sym_str)
                        current_sym_str = current_sym_str.replace('[', '(')
                        current_sym_str = current_sym_str.replace(']', ')')
                    else:
                        current_sym_str = tuple(current_sym_str)
                    query = self.create_query_get_open_price_by_date_n_sym(current_sym_str, holdings_time)
                    price_data = self.sql_execute(query)
                    price_data = pd.DataFrame(price_data)
                    market_value = self.market_value_by_join_holdings_and_price(holdings_data, price_data)

                    scaling_in_sum = 0
                    scaling_out_sum = 0
                    xirr_calculate_dict['date'].append(holdings_time)
                    xirr_calculate_dict['amounts'].append(-market_value)
                '''2.3.5 以xirr_calculate_dict計算XIRR值'''
                temp_xirr_calculate_dict = copy.deepcopy(xirr_calculate_dict)
                if idx_q == 0: # 第一季直接帶pyxirr公式計算結果為10%，沒有研究計算公式，故直接assign 0。
                    xirr = 0
                else:
                    xirr = self.calculate_XIRR(temp_xirr_calculate_dict, holdings_time, market_value)
                '''2.3.6 將本季度holdings data/price_data暫存，作為下一季度計算使用。'''
                previous_holdings = holdings_data.copy()
                previous_holdings_time = holdings_time
                previous_sym_str = current_sym_str # correlation analysis使用
                '''2.3.7 將統計數值回存至summary_data'''
                hedge_fund_data = {'date': holdings_time, '市值': market_value, '加碼': scaling_in_sum, '減碼': scaling_out_sum, 'XIRR':xirr}
                summary_data.append({'hedge_fund': hedge_fund, **hedge_fund_data})
            '''2.4 計算當前持股市值。'''
            holdings_time = self.max_date#2024-03-06'#self.max_date # 可以自訂，此處以DB中最大有交易日期為主(2024-01-09)
            base_13F_date_list.append(str(holdings_time))
            query = self.create_query_get_open_price_by_join_holdings_n_price(sym_str, holdings_time, hedge_fund, quarter, filing_number)
            price_data = self.sql_execute(query)
            price_data = pd.DataFrame(price_data)
            market_value = sum(price_data['Open'] * price_data['SHARES'])
            xirr = self.calculate_XIRR(xirr_calculate_dict, holdings_time, market_value)
            hedge_fund_data = {'date': holdings_time, '市值': market_value, '加碼': 0, '減碼': 0, 'XIRR':xirr}
            summary_data.append({'hedge_fund': hedge_fund, **hedge_fund_data})
            hedge_summary = pd.DataFrame(summary_data)
            hedge_summary = self.summary_statistical_calculates(hedge_summary)
            if summary_table is None:
                summary_table = hedge_summary
            else:
                summary_table = pd.concat([summary_table, hedge_summary], ignore_index=True)
            
            '''4. 相關性分析 TBD'''
            # print(" === === === 第{}個對沖基金：{}，包含{}個季度資料。".format(idx+1, hedge_fund, len(quarters_list)))
            correlation = corr_analysis_table['price_change_ratio'].corr(corr_analysis_table['scaling'])
            # print("Correlation between price_change_ratio and scaling:", correlation)
            # print("{} ---> {}".format(round(correlation, 2), hedge_fund))
        '''3. 加入其他想比較的個股'''
        wish_list = {'^GSPC':'us', '0050':'tw', '0056':'tw'}
        for k_, v_ in wish_list.items():
            print(" === === === 加入個股比較 {}.{}  ".format(k_, v_))
            # individual_summary = self.individual_stock_summary(base_13F_date_list, 'us', '^GSPC')
            # summary_table = pd.concat([summary_table, individual_summary], ignore_index=True)
            individual_summary = self.individual_stock_summary(base_13F_date_list, v_, k_)
            summary_table = pd.concat([summary_table, individual_summary], ignore_index=True)

        '''4. 加入自定義基金比較'''

        customized_fund_list = {
            '自組基金_產業前3公司前3': (self.customize_fund_components, {'industry_top_selection': 3, 'company_top_selection': 3}),
            '自組基金_產業前3公司前2': (self.customize_fund_components, {'industry_top_selection': 3, 'company_top_selection': 2}),
            '自組基金_產業前3公司前1': (self.customize_fund_components, {'industry_top_selection': 3, 'company_top_selection': 1}),
            '自組基金_產業前2公司前3': (self.customize_fund_components, {'industry_top_selection': 2, 'company_top_selection': 3}),
            '自組基金_產業前2公司前2': (self.customize_fund_components, {'industry_top_selection': 2, 'company_top_selection': 2}),
            '自組基金_產業前2公司前1': (self.customize_fund_components, {'industry_top_selection': 2, 'company_top_selection': 1}),
            '自組基金_產業前1公司前3': (self.customize_fund_components, {'industry_top_selection': 1, 'company_top_selection': 3}),
            '自組基金_產業前1公司前2': (self.customize_fund_components, {'industry_top_selection': 1, 'company_top_selection': 2}),
            '自組基金_產業前1公司前1': (self.customize_fund_components, {'industry_top_selection': 1, 'company_top_selection': 1}),
        }

        fund_components_dict = {}
        fund_components_dict_by_hedge = {}
        for k_, v_ in customized_fund_list.items():
            print(' === === === 加入自定義基金進行比較 {}'.format(k_))
            func_to_call, params = v_
            customized_fund_data, customized_table = func_to_call(**params)
            fund_components_dict[k_] = customized_fund_data
            fund_components_dict_by_hedge[k_] = customized_table
            customized_fund_summary = self.customized_fund_stock_summary(k_, customized_fund_data)
            summary_table = pd.concat([summary_table, customized_fund_summary], ignore_index=True)

        '''5. 輸出資料表格'''
        XIRR_table = self.arragne_output_XIRR_excel_format(summary_table)
        if not os.path.exists(self.config_obj.backtest_summary):
            os.makedirs(self.config_obj.backtest_summary)
        path = os.path.join(self.config_obj.backtest_summary, str(datetime.datetime.now()).split()[0] + '_summary_table.xlsx')
        with pd.ExcelWriter(path) as writer:  
            summary_table.to_excel(writer, index=False, sheet_name='raw_data')
            XIRR_table.to_excel(writer, index=False, sheet_name='XIRR排序')
            for fund_name, components_table in fund_components_dict.items():
                components_table.to_excel(writer, index=False, sheet_name=fund_name)
        print("NULL SYM COUNTER:", null_sym_counter)

        #Fund components by each hedge analysis
        path = os.path.join(self.config_obj.backtest_summary, str(datetime.datetime.now()).split()[0] + '_customized_table_by_hedge.xlsx')
        with pd.ExcelWriter(path) as writer:  
            for fund_name, components_table in fund_components_dict_by_hedge.items():
                components_table.to_excel(writer, index=False, sheet_name=fund_name)

    def customize_fund_components(self, industry_top_selection, company_top_selection):
        '''
        function:
            製作自定義基金。
        Input:
        Output:
            仿製基金13F holdings table，包含欄位SYM、SHARES、date、price
        1.2019 2/15 開始進場
        2.XIRR表現在波克夏以上的基金
        3.各基金的前(三)市值產業
        4.前(三)市值產業 的前(三)市值股票
        5.以100萬當入場金
        6.用price回推要購買的SHARES，去進場
            SYM、SHARES、date、price
        7.回測計算MDD、XIRR
        '''
        enter_date = self.config_obj.customize_enter_date #2019 2/15 開始進場
        hedge_funds = self.config_obj.target_hedge_funds #XIRR表現在波克夏以上的基金
        
        # industry_top_selection = self.config_obj.industry_top_selection #各基金的前(三)市值產業 --> 改成以function params控制
        # company_top_selection = self.config_obj.company_top_selection #前(三)市值產業 的前(三)市值股票 --> 改成以function params控制
        enter_cost = self.config_obj.enter_cost
        '''定義Output obj、統計用obj'''
        ### Final Output Form
        customized_table = None

        '''Read DB data'''
        query = self.create_query_data_table(self.hedge_fund_portfolio_table)
        fund_data = self.sql_execute(query)
        fund_data = pd.DataFrame(fund_data)
        fund_data = fund_data[fund_data['HEDGE_FUND'].isin(hedge_funds)]
        count_hedge_funds = fund_data.groupby('QUARTER')['HEDGE_FUND'].nunique().reset_index()
        '''
            QUARTER  HEDGE_FUND
        0   Q1 2014          12
        1   Q1 2015          15
        2   Q1 2016          17
        '''
        
        # hedge_funds = ['Dalal Street Holdings']
        hedge_fund_list = hedge_funds

        # print("總共包含{}個對沖基金資料".format(len(hedge_fund_list)))
        # print('Hedge Funds:', )
        # print(hedge_fund_list)

        '''各hedge fund計算迴圈'''
        for idx, hedge_fund in enumerate(hedge_fund_list):
            '''定義迴圈內參數'''
            # summary_data = [] # 包含項目: {'SYM': , 'SHARES': 依照資金/比例分配後的SHARES, }
            # corr_analysis_table = None 
            # previous_holdings = None
            # xirr_calculate_dict = {'date':[], 'amounts':[]}
            # previous_holdings_time = None
            # previous_sym_str = tuple() #TBD
            
            '''調整fund_data'''
            each_fund_data = self.each_fund_data_adjust(fund_data, hedge_fund)
            quarters_list = each_fund_data['QUARTER'].values
            date_list = each_fund_data['BASE_DATE'].values #date_list = each_fund_data['DATE_FILED'].values # 進場時間點使用該基金13F公布時間
            filing_list = each_fund_data['FILING_ID'].values
            # if idx == 0:
            #     base_13F_dates = each_fund_data['BASE_DATE'].values# 進場時間點使用13F公布截止時間
            #     base_13F_date_list = pd.to_datetime(base_13F_dates, unit='ns')
            #     base_13F_date_list = [str(date) for date in base_13F_date_list.tolist()]

            # print(" === === === 第{}個對沖基金：{}，包含{}個季度資料。 ".format(idx+1, hedge_fund, len(quarters_list)))
            # print(each_fund_data)
            '''各Quarter計算迴圈'''
            for idx_q, (quarter, holdings_time, filing_number) in enumerate(zip(quarters_list, date_list, filing_list)):
                '''定義回圈內參數'''
                # hedge_fund_data = {'date': None, '市值': None, '加碼': None, '減碼': None, 'XIRR': None}
                customized_holdings = None
                '''調整holdings_time'''
                holdings_time = self.adjust_holdings_time(holdings_time, self.us_sorted_dates) # 以13F報告公布期限為基準(5/15, 8/14, 11/14, 2/14)
                # print("     第{}個季度：{}，時間為{}".format(idx_q+1, quarter, holdings_time))

                query = self.create_query_holdings_with_gics_n_price(hedge_fund, quarter, filing_number, holdings_time)
                holdings_data = self.sql_execute(query)
                holdings_data = pd.DataFrame(holdings_data)
                # holdings_data = self.holdings_data_adjust(holdings_data)

                if len(holdings_data) == 0:
                    pass
                else:
                    top_gics = self.get_top_gics_from_holdings(holdings_data, industry_top_selection)
                    customized_holdings = self.select_company_from_holdings(holdings_data, top_gics, company_top_selection)

                    hedge_num = count_hedge_funds[count_hedge_funds['QUARTER'] == quarter]['HEDGE_FUND'].values[0]
                    # print('該季Hedge Fund數:', hedge_num)
                    customized_holdings = self.calculate_customized_shares(customized_holdings, enter_cost, hedge_num)

                if customized_table is None:
                    customized_table = customized_holdings
                else:
                    customized_table = pd.concat([customized_table, customized_holdings], ignore_index=True)
        
        # path = os.path.join(self.config_obj.backtest_summary, str(datetime.datetime.now()).split()[0] + '_{}ind_{}com_customized_table_by_hedge.csv'.format(industry_top_selection, company_top_selection))
        # customized_table.to_csv(path, index=False)

        # 整理該hedge的customized table
        customized_table_by_stock = self.arrange_customized_table(customized_table)
        # path = os.path.join(self.config_obj.backtest_summary, str(datetime.datetime.now()).split()[0] + '_customized_table_by_stock.csv')
        # customized_table_by_stock.to_csv(path, index=False)

        return customized_table_by_stock, customized_table


    def sql_execute(self, query):

        if self.config_obj.LOCAL_FLAG:
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

    def create_query_data_table(self, data_table):
        '''
        其中依照[DATE_FILED]做升續排列。
        '''
        query = '''SELECT * FROM {} WITH(NOLOCK) ORDER BY [DATE_FILED] ASC'''.format(data_table)
        return query

    def get_all_price_date(self, price_table):

        query = '''
        SELECT DISTINCT [date]
        FROM {}
        '''.format(price_table)
        return query

    def each_fund_data_adjust(self, fund_data, hedge_fund):
        '''
        function:
            依據條件篩選fund_data中的records，因為每個季度可能包含多個FILE。
                QUARTER    FORM_TYPE  DATE_FILED           FILING_ID                 HEDGE_FUND
            0   Q4 2013       13F-HR  2014-01-31  000090556714000001  Yacktman Asset Management
            1   Q4 2013  RESTATEMENT  2014-02-13  000090556714000002  Yacktman Asset Management
            2   Q1 2014       13F-HR  2014-05-06  000090556714000004  Yacktman Asset Management
        Input:
            -. 原始fund_data(13F報告)
            -. 目前處理的hedge_fund string
        Output:
            調整後之fund_data
        '''
        # 依據hedge_fund string篩選基金
        # 刪除'HOLDINGS'(持股個數), 'VALUE'(總市值), 'TOP_HOLDINGS'(最高持股量的個股)
        # 重製dataframe index
        df = fund_data[fund_data['HEDGE_FUND']==hedge_fund].drop(['HOLDINGS', 'VALUE', 'TOP_HOLDINGS', ], axis=1).reset_index(drop=True)

        def calculate_base_date(row):
            year = int(row['QUARTER'].split()[1])
            if 'Q1' in row['QUARTER']:
                return datetime.datetime(year, 5, 15)
            elif 'Q2' in row['QUARTER']:
                return datetime.datetime(year, 8, 14)
            elif 'Q3' in row['QUARTER']:
                return datetime.datetime(year, 11, 14)
            elif 'Q4' in row['QUARTER']:
                return datetime.datetime(year + 1, 2, 14)
        # 新增欄位BASE_DATE，依據QUARTER欄位的資料，回傳Q1->5/15, Q2->8/14, Q3->11/14, Q4->2/14等日期
        df['BASE_DATE'] = df.apply(calculate_base_date, axis=1)# 新增 'BASE DATE' 基準日期欄位 (即5/15, 8/14, 11/14, 2/14)
        df['DATE_FILED'] = pd.to_datetime(df['DATE_FILED'])# 將 'DATE_FILED' 轉換為日期時間格式

        df = df[~(df['FORM_TYPE'] == 'NEW HOLDINGS')] # 刪除NEW HOLDINGS的Records

        # 刪除 RESTATEMENT 大於 基準日期的Records，但刪除後該季應該至少剩下一筆資料。
        # base_date = df['BASE_DATE'].unique()
        # adjusted_df = pd.DataFrame(columns=['QUARTER','FORM_TYPE','DATE_FILED','FILING_ID','HEDGE_FUND','BASE_DATE',])
        # for each_base_date in base_date:
        #     each_df = df[df['BASE_DATE']==each_base_date]
        #     if len(each_df) == 1:
        #         pass
        #     else:
        #         each_df = each_df[~((each_df['FORM_TYPE'] == 'RESTATEMENT') & (df['DATE_FILED'] > df['BASE_DATE']))]
        df = df[~((df['FORM_TYPE'] == 'RESTATEMENT') & (df['DATE_FILED'] > df['BASE_DATE']))] # 刪除 RESTATEMENT 大於 基準日期的Records。


        sorting_key = df.groupby('BASE_DATE')['FILING_ID'].idxmax() # 取group中最大FILING_ID，因為可能有多筆records，直接依據FILING_ID取最新的。
        df = df.loc[sorting_key].reset_index(drop=True)

        return df
    def adjust_holdings_time(self, holdings_time, sorted_dates):
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
        if holdings_time != adjust_date:
            result_date = adjust_date
        else:
            result_date = sorted_dates[index+1]
        # print('使用日期:', result_date)
        return result_date    
    def create_query_holdings(self, fund, quarter, filing_number):
        '''
        依據fund和quarter篩選holdings資料表的query語句
        '''
        data_table = self.holdings_data_table
        query = '''SELECT * FROM {} WITH(NOLOCK) 
        WHERE [HEDGE_FUND] = '{}' 
        AND [QUARTER] = '{}' 
        AND [FILING_ID] = '{}'
        AND [OPTION_TYPE] IS NULL
        AND SUBSTRING([CUSIP], 7, 2) = '10'
        AND [SHARES] IS NOT NULL 
        '''.format(data_table, fund, quarter, filing_number)
        return query
    def holdings_data_adjust(self, df):
        '''
        function:
            依據SYM進行groupby，因為13F報告中，同一SYM可能會列舉多筆。故依據SYM加總VALUE、Percentile、SHARES等三個欄位數值(預先轉換為Numeric確保可加性。
        input:
            holdings_data
        output:
            adjusted holdings_data
        '''
        df['VALUE'] = pd.to_numeric(df['VALUE'], errors='coerce')  # 将VALUE列转换为数值，将无法转换的值设为NaN
        df['Percentile'] = pd.to_numeric(df['Percentile'], errors='coerce')  # 将Percentile列转换为数值，将无法转换的值设为NaN
        df['SHARES'] = pd.to_numeric(df['SHARES'], errors='coerce')  # 将SHARES列转换为数值，将无法转换的值设为NaN

        df = df.groupby('SYM', as_index=False).agg({'VALUE':'sum', 'Percentile':'sum', 'SHARES':'sum'})
        return df
    
    def holdings_data_adjust_for_customized_fund(self, df):
        '''
        function:
            同上，但來源為customized fund holdings。
        '''
        df = df.rename(columns={"shares_to_buy": "SHARES"})
        df = df.drop(['QUARTER', 'date', 'price', 'suggested_invest_amount'], axis=1)
        df['SHARES'] = pd.to_numeric(df['SHARES'], errors='coerce')  # 将SHARES列转换为数值，将无法转换的值设为NaN
        return df
    
    def shares_difference_between_quarters(self, previous_holdings, holdings_data):

        previous_holdings = previous_holdings[['SYM', 'SHARES']]
        holdings_data = holdings_data[['SYM', 'SHARES']]

        # 將兩個資料表合併
        merged_data = holdings_data.merge(previous_holdings, on=['SYM'], how='outer', suffixes=('_current', '_previous'))
        merged_data['SHARES_current'].fillna(0, inplace=True)
        merged_data['SHARES_previous'].fillna(0, inplace=True)
        merged_data = merged_data.astype({'SHARES_current': int, 'SHARES_previous': int})
        # 計算持股數量變化
        merged_data['shares_change'] = merged_data['SHARES_current'] - merged_data['SHARES_previous']
        return merged_data
    def create_query_get_open_price_by_join_holdings_n_price(self, SYMs_tuple, date, fund, quarter, filing_number):
        '''
        依據holdings去查表price，透過stock_id(即holdings表中的SYM) join兩張表格，並加入SHARES資訊至price表。
        '''
        price_table = self.us_stock_price_table
        holdings_table = self.holdings_data_table
        query = ''' SELECT DISTINCT tb_price.[date], tb_price.[stock_id], tb_price.[Open], tb_holdings.[SHARES]
            FROM {} tb_price WITH(NOLOCK)
			INNER JOIN {} tb_holdings WITH(NOLOCK) on tb_price.stock_id = tb_holdings.SYM
			WHERE tb_price.[stock_id] IN {}
            AND tb_price.[date] = '{}'
            AND tb_holdings.[HEDGE_FUND] = '{}' 
            AND tb_holdings.[QUARTER] = '{}' 
            AND tb_holdings.[FILING_ID] = '{}'
            AND tb_holdings.[OPTION_TYPE] IS NULL
            AND tb_holdings.[SHARES] IS NOT NULL
            AND SUBSTRING(tb_holdings.[CUSIP], 7, 2) = '10'
            '''.format(price_table, holdings_table, SYMs_tuple, date, fund, quarter, filing_number)
        return query
    
    def create_query_get_open_price_for_customized_fund(self, sym_str, holdings_time):

        price_table = self.us_stock_price_table
        query = '''
        SELECT [date], [stock_id] SYM, [Open] FROM {} WITH(NOLOCK)
        WHERE [stock_id] IN {}
        AND [date] = '{}'
        '''.format(price_table, sym_str, holdings_time)
        return query

    def create_query_get_open_price_by_date_n_sym(self, SYMs_tuple, date):
        '''
        function:
            建立query string: 以SYM字串以及date查詢price table裡面的開盤價。
        input:
            -. SYM tuple
            -. date
        output:
            query(string)
        '''
        price_table = self.us_stock_price_table
        query = ''' SELECT [date], [stock_id] SYM, [Open]
            FROM {} WITH(NOLOCK) WHERE [date] = '{}' AND [stock_id] IN {}
            '''.format(price_table, date, SYMs_tuple)
        return query

    def calculate_scaling_in_and_out(self, merged_data, price_data):
        ''''''
        scaling_in = {}
        scaling_out = {}
        scaling_even = {}
        market_value = 0
        scaling_data = merged_data.merge(price_data, on=['SYM'])

        # 根據持股數量變化分類
        for index, row in scaling_data.iterrows():
            stock_id = row['SYM']
            shares_change = row['shares_change']
            Open_current = row['Open']
            SHARES_current = row['SHARES_current']

            if shares_change > 0:
                scaling_in[stock_id] = shares_change * Open_current
            elif shares_change < 0:
                scaling_out[stock_id] = abs(shares_change) * Open_current
            else:
                scaling_even[stock_id] = 0
            market_value = market_value + SHARES_current * Open_current
        # print("scaling_in")
        # print(scaling_in)
        # print("scaling_out")
        # print(scaling_out)
        # print("scaling_even")
        # print(scaling_even)
        return market_value, scaling_in, scaling_out, scaling_even
    def get_price_change_ratio(self, previous_holdings_time, holdings_time, sym_str_combined):
        '''
        計算本季度與前一季度持股漲跌幅度
        '''
        if len(sym_str_combined) == 1:
            sym_str_combined = str(sym_str_combined)
            sym_str_combined = sym_str_combined.replace(',', '')
        query = self.create_query_get_open_price_by_date_n_sym(sym_str_combined, previous_holdings_time)
        previous_price_data = self.sql_execute(query)
        previous_price_data = pd.DataFrame(previous_price_data)
        query = self.create_query_get_open_price_by_date_n_sym(sym_str_combined, holdings_time)
        price_data = self.sql_execute(query)
        price_data = pd.DataFrame(price_data)

        # 將兩個資料表合併
        merged_data = price_data.merge(previous_price_data, on=['SYM'], how='outer', suffixes=('_current', '_previous'))

        # merged_data['SHARES_current'].fillna(0, inplace=True)
        # merged_data['SHARES_previous'].fillna(0, inplace=True)
        # merged_data = merged_data.astype({'SHARES_current': int, 'SHARES_previous': int})
        # 計算price change ratio
        merged_data['price_change_ratio'] = (merged_data['Open_current'] - merged_data['Open_previous']) / merged_data['Open_previous']
        
        return merged_data
    def arrange_corr_analysis_data(self, price_ratio_data, shares_data):
        '''
        function:

        input:
            -. price_ratio_data
                date_current    SYM  Open_current date_previous  Open_previous  price_change_ratio
            -. shares_data
                SYM  SHARES_current  SHARES_previous  shares_change
        '''
        merged_data = price_ratio_data.merge(shares_data, on=['SYM'], how='outer')
        merged_data['scaling'] = merged_data['shares_change'] * merged_data['Open_current']
        merged_data = merged_data.dropna() #TBD: NA部分為SYM在price表中查不到資料者。
        # print(merged_data)
        '''
        merged data columns:
            date_current    SYM  Open_current date_previous  Open_previous  price_change_ratio  SHARES_current  SHARES_previous  shares_change       scaling
        '''
        # merged_data = merged_data[['SYM', 'price_change_ratio', 'scaling']]
        merged_data = merged_data[['price_change_ratio', 'scaling']]
        return merged_data
    def market_value_by_join_holdings_and_price(self, holdings_data, price_data):
        '''
        function:
            計算股票市值，藉由holdings_data:SHARES、price_data:Open相乘來計算市值，並加總。
        input:
            -. holdings_data
            -. price_data
        output:
            -. market_value
        '''
        market_value = 0
        merged_data = holdings_data.merge(price_data, on=['SYM'])
        # print('Holdings & Price:')
        # print(merged_data)

        for index, row in merged_data.iterrows():
            shares = row['SHARES']
            Open_current = row['Open']
            market_value = market_value + shares * Open_current
        return market_value
    
    def calculate_XIRR(self, data, holdings_time, market_value):
        '''
        將data資料加上最新時間/市值兌現，計算XIRR值。
        '''
        data['date'].append(holdings_time)
        data['amounts'].append(market_value)
        f = lambda d: d.date() if isinstance(d, pd._libs.tslibs.timestamps.Timestamp) else d
        python_date = [f(d) for d in data['date']]
        amounts = [int(a) for a in data['amounts']]
        result = xirr(python_date, amounts)
        # x = {'date':python_date, 'amounts':amounts}
        # print(pd.DataFrame.from_dict(x))
        # print("XIRR:", result)
        return result

    def summary_statistical_calculates(self, hedge_summary):
        # 新增欄位 A 為 [加碼-減碼]
        hedge_summary['淨投入額'] = hedge_summary['加碼'] - hedge_summary['減碼']

        # 新增欄位 為 A/上一個row的市值
        hedge_summary['淨投入額占比'] = hedge_summary['淨投入額'] / hedge_summary['市值'].shift(1)
        return hedge_summary
   
    def individual_stock_summary(self, original_date_list, market, sym_str):
        '''
        function:
            依據13F報告時間點，計算投資某個股的XIRR加入進行比較。
            由於TWS與US的開市時間不同，包裝一個函數能夠修改original_date_list內元素，調整為有開市的時間，才找得到price。
        input:
            -. original_date_list: 包含13F各報告節點的截止時間，設定為買進個股的時間點。
            -. market(string): 時間要調整到的目標市場 ex. 'tw', 'us'
            -. sym_str: 該個股股票代碼
        output:
            individual_summary(pd.DataFrame): {'date': holdings_time, '市值': price, '加碼': 0, '減碼': 0, 'XIRR': xirr, '淨投入額': 0, '淨投入額占比': 0, }
        '''
        summary_data = []
        individual_data = {'date': None, '市值': None, '加碼': None, '減碼': None, 'XIRR': None, '淨投入額': None, '淨投入額占比': None, }

        if market == 'tw':
            price_table = self.tw_stock_price_table
        elif market == 'us':
            price_table = self.us_stock_price_table
        else:
            price_table = self.us_stock_price_table

        adjusted_date_list = self.adjust_date_str_for_market(original_date_list, market=market) # 台股與美股開市時間差異
        data_date_str = tuple(adjusted_date_list)
        query = self.create_query_stock_price_data(price_table, sym_str, data_date_str)
        price_data = self.sql_execute(query)
        price_data = pd.DataFrame(price_data)
        xirr_calculate_dict = {'date':[], 'amounts':[]}
        for index, row in price_data.iterrows():
            holdings_time = row['date']
            price = row['Open']
            if index == 0:
                xirr_calculate_dict['date'].append(holdings_time)
                xirr_calculate_dict['amounts'].append(-price)
            else:
                xirr_calculate_dict['date'].append(holdings_time)
                xirr_calculate_dict['amounts'].append(0)
            temp_xirr_calculate_dict = copy.deepcopy(xirr_calculate_dict)
            if index == 0: # 第一季直接帶pyxirr公式計算結果為10%，沒有研究計算公式，故直接assign 0。
                xirr = 0
            else:
                xirr = self.calculate_XIRR(temp_xirr_calculate_dict, holdings_time, price)
            individual_data = {'date': holdings_time, '市值': price, '加碼': 0, '減碼': 0, 'XIRR': xirr, '淨投入額': 0, '淨投入額占比': 0, }
            summary_data.append({'hedge_fund': str(sym_str), **individual_data})
        individual_summary = pd.DataFrame(summary_data)
        return individual_summary
    
    def customized_fund_stock_summary(self, plan_name, customized_fund_data):
        summary_data = []
        previous_holdings = None
        xirr_calculate_dict = {'date':[], 'amounts':[]}
        previous_holdings_time = None
        previous_sym_str = tuple() #TBD
        fund_data = {'date': None, '市值': None, '加碼': None, '減碼': None, 'XIRR': None, '淨投入額': None, '淨投入額占比': None, }
        quarters_list = customized_fund_data['QUARTER'].drop_duplicates().values
        date_list = customized_fund_data['date'].drop_duplicates().values

        '''2.3. 各Quarter計算迴圈'''
        for idx_q, (quarter, holdings_time) in enumerate(zip(quarters_list, date_list)):
            '''2.3.1 定義回圈內參數'''
            fund_data = {'date': None, '市值': None, '加碼': None, '減碼': None, 'XIRR': None, '淨投入額': None, '淨投入額占比': None, }

            holdings_data = customized_fund_data[customized_fund_data['QUARTER']==quarter]
            holdings_data = self.holdings_data_adjust_for_customized_fund(holdings_data)
            
            '''
                SYM  SHARES
            0   AMT    1609
            1   CCI    1860
            2   FLR    1922

            '''
            if idx_q > 0: #扣除第一季，每季要計算的內容
                shares_data = self.shares_difference_between_quarters(previous_holdings, holdings_data)
                sym_str = shares_data['SYM'].dropna().values
                
                # print("SYMs:", sym_str)
                if len(sym_str) == 1:
                    sym_str = str(sym_str)
                    sym_str = sym_str.replace('[', '(')
                    sym_str = sym_str.replace(']', ')')
                else:
                    sym_str = tuple(sym_str)
                
                query = self.create_query_get_open_price_by_date_n_sym(sym_str, holdings_time)
                price_data = self.sql_execute(query)
                price_data = pd.DataFrame(price_data)
                # print('price_data:')
                # print(price_data)
                '''
                        date   SYM    Open
                    0   2016-11-14  AAPL   26.93
                    1   2016-11-14   ALL   69.76
                    2   2016-11-14    AY   16.98
                    3   2016-11-14   BAC   19.41
                '''
                market_value, scaling_in, scaling_out, scaling_even = self.calculate_scaling_in_and_out(shares_data, price_data)
                # print("Shares Increased:", scaling_in)
                # print("Shares Decreased:", scaling_out)
                # print("Shares Unchanged:", scaling_even)
                scaling_in_sum = sum([i for i in  scaling_in.values()])
                scaling_out_sum = sum([i for i in  scaling_out.values()])
                
                xirr_calculate_dict['date'].append(holdings_time)
                xirr_calculate_dict['amounts'].append(-(scaling_in_sum - scaling_out_sum))

                ### correlation analysis TBD
                current_sym_str = holdings_data['SYM'].dropna().values 
                current_sym_str = tuple(current_sym_str)
                # print("SYMs:", current_sym_str)            
                # print("previous_sym_str:", previous_sym_str)
                sym_str_combined = set(previous_sym_str) | set(current_sym_str)
                sym_str_combined = tuple(sym_str_combined)
                
            else: #第一季要計算的內容

                current_sym_str = holdings_data['SYM'].dropna().values # TBD: 確認Drop數量
                if len(current_sym_str) == 1:
                    current_sym_str = str(current_sym_str)
                    current_sym_str = current_sym_str.replace('[', '(')
                    current_sym_str = current_sym_str.replace(']', ')')
                else:
                    current_sym_str = tuple(current_sym_str)
                query = self.create_query_get_open_price_by_date_n_sym(current_sym_str, holdings_time)
                price_data = self.sql_execute(query)
                price_data = pd.DataFrame(price_data)
                market_value = self.market_value_by_join_holdings_and_price(holdings_data, price_data)

                scaling_in_sum = 0
                scaling_out_sum = 0
                xirr_calculate_dict['date'].append(holdings_time)
                xirr_calculate_dict['amounts'].append(-market_value)
            '''2.3.5 以xirr_calculate_dict計算XIRR值'''
            temp_xirr_calculate_dict = copy.deepcopy(xirr_calculate_dict)
            if idx_q == 0: # 第一季直接帶pyxirr公式計算結果為10%，沒有研究計算公式，故直接assign 0。
                xirr = 0
            else:
                xirr = self.calculate_XIRR(temp_xirr_calculate_dict, holdings_time, market_value)
            '''2.3.6 將本季度holdings data/price_data暫存，作為下一季度計算使用。'''
            previous_holdings = holdings_data.copy()
            previous_holdings_time = holdings_time
            previous_sym_str = current_sym_str # correlation analysis使用
            '''2.3.7 將統計數值回存至summary_data'''
            fund_data = {'date': holdings_time, '市值': market_value, '加碼': scaling_in_sum, '減碼': scaling_out_sum, 'XIRR':xirr}
            summary_data.append({'hedge_fund': plan_name, **fund_data})
        
        
        holdings_time = self.max_date#'2024-03-06'#self.max_date # 可以自訂，此處以DB中最大有交易日期為主(2024-01-09)

        # query = self.create_query_get_open_price_by_join_holdings_n_price(sym_str, holdings_time, hedge_fund, quarter, filing_number)
        # price_data = self.sql_execute(query)
        # price_data = pd.DataFrame(price_data)
        query = self.create_query_get_open_price_for_customized_fund(current_sym_str, holdings_time)
        price_data = self.sql_execute(query)
        price_data = pd.DataFrame(price_data)
        price_data = price_data.merge(holdings_data, on=['SYM'], how='left')

        market_value = sum(price_data['Open'] * price_data['SHARES'])
        xirr = self.calculate_XIRR(xirr_calculate_dict, holdings_time, market_value)
        hedge_fund_data = {'date': holdings_time, '市值': market_value, '加碼': 0, '減碼': 0, 'XIRR':xirr}
        summary_data.append({'hedge_fund': plan_name, **hedge_fund_data})
        customized_fund_summary = pd.DataFrame(summary_data)
        customized_fund_summary = self.summary_statistical_calculates(customized_fund_summary)

        return customized_fund_summary
    def adjust_date_str_for_market(self, base_13F_date_list, market):
        adjusted_data_list = []
        if market == 'tw':
            soruce_date = self.tws_sorted_dates
        elif market == 'us':
            soruce_date = self.us_sorted_dates
        else:
            soruce_date = self.us_sorted_dates
        
        for date in base_13F_date_list:
            index = soruce_date.searchsorted(date)
            adjust_date  = soruce_date[index] if index < len(soruce_date) else soruce_date[-1] # 如果日期正好在列表中，返回該日期；否則返回下一個最接近的日期
            adjusted_data_list.append(str(adjust_date))
        return adjusted_data_list

    def create_query_stock_price_data(self, price_table, sym_str, date_str):
        '''
        依據price table查詢股價
        '''
        query = '''
        SELECT [date],[stock_id],[Open]
        FROM {} WHERE stock_id = '{}' AND [date] IN {} 
        '''.format(price_table, sym_str, date_str)
        return query

    def create_query_holdings_with_gics_n_price(self, hedge_fund, quarter, filing_number, holdings_time):

        holdings_table = self.holdings_data_table
        gics_table = self.us_stock_gics_table
        price_table = self.us_stock_price_table
        query = '''
        SELECT tb_hedge.[SYM], tb_hedge.[SHARES], tb_hedge.[HEDGE_FUND], tb_hedge.[QUARTER], tb_hedge.[GICS], tb_price.[date] date, tb_price.[Open] price FROM
        (SELECT tb_holdings.[SYM], tb_holdings.[SHARES], tb_holdings.[HEDGE_FUND], tb_holdings.[QUARTER], tb_gics.[GICS_2_digit] GICS
        FROM {} tb_holdings WITH(NOLOCK)
        INNER JOIN {} tb_gics WITH(NOLOCK)
        ON tb_holdings.SYM = tb_gics.Ticker 
        WHERE tb_holdings.[HEDGE_FUND] = '{}' AND tb_holdings.[QUARTER] = '{}' AND tb_holdings.[FILING_ID] = '{}') tb_hedge
        INNER JOIN {} tb_price WITH(NOLOCK) 
        ON tb_hedge.SYM = tb_price.stock_id 
        WHERE tb_price.[date] = '{}'
        '''.format(holdings_table, gics_table, hedge_fund, quarter, filing_number, price_table, holdings_time)
        return query

    def get_top_gics_from_holdings(self, holdings_data, industry_top_selection):
        '''
        function:
            依據GICs去計算Hedge持股的產業分布，列舉出前(industry_top_selection)市值高的產業。
        input:
            -. holdings_data:
            -. industry_top_selection: int
        output:
            -. top_gics: list
                前(industry_top_selection)產業list，如['20' '25' '60']。
        '''
        if len(holdings_data) == 0:
            return None
        # top_gics = []
        holdings_data['market_price'] = holdings_data['price'] * holdings_data['SHARES']
        df = holdings_data.groupby('GICS', as_index=False).agg({'market_price':'sum'})
        df = df.sort_values('market_price', ascending=False)
        top_gics = df['GICS'][:industry_top_selection].values
        return top_gics
    
    def select_company_from_holdings(self, holdings_data, top_gics, company_top_selection):
        '''
        function:
            依據top GICs，去篩選holdings_data。列篩選前(company_top_selection)市值高的公司，輸出table。
        input:
            -. holdings_data: df
            -. top_gics: list
            -. company_top_selection: int
        output:
            customized_holdings: df
        '''
        customized_holdings = holdings_data[holdings_data['GICS'].isin(top_gics)]
        customized_holdings = customized_holdings.sort_values(by=['GICS', 'market_price'], ascending=False)
        customized_holdings = customized_holdings.groupby('GICS').apply(lambda x: x.nlargest(company_top_selection, 'market_price')).reset_index(drop=True)
        return customized_holdings
    
    def calculate_customized_shares(self, customized_holdings, enter_cost, hedge_num):
        
        # print(customized_holdings)
        # 計算每個持股的投資金額
        customized_holdings['investment_amount'] = enter_cost / hedge_num / len(customized_holdings)

        # 計算可以投入的股票數量（向下取整）
        customized_holdings['shares_to_buy'] = customized_holdings['investment_amount'] / customized_holdings['price']
        customized_holdings['shares_to_buy'] = customized_holdings['shares_to_buy'].astype(int)

        # print(customized_holdings)
        return customized_holdings

    def arrange_customized_table(self, customized_table):

        total_shares_to_buy = customized_table.groupby(['QUARTER', 'SYM'], as_index=False)['shares_to_buy'].sum()
        
        # 將 total_shares_to_buy 與原始 DataFrame 合併，以保留 SYM、QUARTER、date、price 欄位
        merged_df = pd.merge(total_shares_to_buy, customized_table[['SYM', 'QUARTER', 'date', 'price']], on=['SYM', 'QUARTER'], how='left')
        merged_df = merged_df.drop_duplicates()
        merged_df['suggested_invest_amount'] = merged_df['shares_to_buy'] * merged_df['price']
        merged_df = merged_df.sort_values(by=['date'], ascending=True)
        return merged_df

    def arragne_output_XIRR_excel_format(self, summary_table):
        summary_table = summary_table[summary_table['date'] == self.max_date]
        summary_table = summary_table.sort_values(by=['XIRR'], ascending=False)
        return summary_table