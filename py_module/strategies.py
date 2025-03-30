from py_module.config import Configuration

from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import pymssql
import calendar
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import sys
# np.set_printoptions(threshold=sys.maxsize)
import copy
import os
import datetime
from datetime import timedelta
import re
from collections import defaultdict

# from pyxirr import xirr

class Strategy13F(object):

    def __init__(self):
        self.config_obj = Configuration()

        # 找到price data中的date欄位，對日期進行排序，找到最大的日期
        query = self.get_all_price_date(self.config_obj.us_stock_price_table) # 為了取得時間欄位
        all_date_list = self.sql_execute(query)
        all_date_list = pd.DataFrame(all_date_list)['date'].values
        us_sorted_dates = sorted(all_date_list)
        self.us_sorted_dates = pd.to_datetime(us_sorted_dates)
        self.min_date = min(self.us_sorted_dates)
        self.max_date = max(self.us_sorted_dates)
        print('美股歷史價格從{}到{}'.format(self.min_date, self.max_date))

        query = self.get_all_price_date(self.config_obj.tw_stock_price_table) # 為了取得時間欄位
        all_date_list = self.sql_execute(query)
        all_date_list = pd.DataFrame(all_date_list)['date'].values
        tws_sorted_dates = sorted(all_date_list)
        self.tws_sorted_dates = pd.to_datetime(tws_sorted_dates)
        self.tws_min_date = min(self.tws_sorted_dates)
        self.tws_max_date = max(self.tws_sorted_dates)
        print('TWS歷史價格從{}到{}'.format(self.tws_min_date, self.tws_max_date))

    def customized_hedge_build_and_store_multi_threading(self):
        '''
        FUNCTION:
            針對大量基金，使用多線呈完成個股挑選(I1C2等篩選)，並將結果轉換為13F portfolio以及holdings，存入DB中。            
        INPUT:
            None
        OUTPUT:
            None
        '''
        
        customized_name_suffix = '_I1C2_2024Q3'
        # funds_list = self.get_all_hedge_funds_name(self.config_obj.hedge_fund_portfolio_table)
        # funds_list = self.get_all_hedge_funds_larger_than_10_years_name(self.config_obj.hedge_fund_portfolio_table)
        funds_list = self.config_obj.target_hedge_funds_dict['I1C2_sharpe']

        # exist_funds_list = self.get_all_hedge_funds_name(self.config_obj.customized_individual_fund_portfolio_table)
        # exist_funds_list = [fund.replace(customized_name_suffix, '') for fund in exist_funds_list]
        # funds_list = list(set(funds_list) - set(exist_funds_list))

        total_amount = len(funds_list) #大約為10578檔基金
        self.config_obj.logger.warning('Multi-threading for {} hedge funds data customized build and store'.format(total_amount))
        split_amount = 1
        splited_fund_list = self.split_list(funds_list, split_amount) #將全部基金分成1000份，若1萬家，則每份約10家基金。

        for split_count, each_fund_list in enumerate(splited_fund_list):
            
            # # 先將hedge_fund_portfolio_table_filtered、holdings_data_table_filtered兩個TABLE資料清空
            # self.funds_data_delete_from_table(self.config_obj.hedge_fund_portfolio_table_filtered)
            # self.funds_data_delete_from_table(self.config_obj.holdings_data_table_filtered)
            
            # # 將本輪資料從hedge_fund_portfolio_table、holdings_data_table複製進filtered TABLES。
            # self.funds_data_copy_and_insert_into_table(self.config_obj.hedge_fund_portfolio_table, self.config_obj.hedge_fund_portfolio_table_filtered, each_fund_list)
            # self.funds_data_copy_and_insert_into_table(self.config_obj.holdings_data_table, self.config_obj.holdings_data_table_filtered, each_fund_list)
            
            sub_amount = len(each_fund_list)
            completed_amount = 0
            lock = Lock()
            
            with ThreadPoolExecutor(max_workers=10) as executor:

                #參數範例: {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Barton Investment Management'], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}
                
                futures = {executor.submit(
                    self.customized_hedge_build_and_store, 
                    name[:20] + customized_name_suffix,
                    True, #reinvest_flag
                    False, #share_profit_flag
                    [name], #hedge_funds
                    1, #industry_top_selection
                    2, #company_top_selection
                    True, #mcap_weighted_flag
                    ): name for name in each_fund_list}

                for future in as_completed(futures):
                    hedge_name = futures[future]
                    try:
                        future.result()
                        with lock:
                            completed_amount += 1
                            self.config_obj.logger.warning('Completed calculating data for fund {}. Progress: {}/{} {}/{}'.format(hedge_name, completed_amount, sub_amount, split_count+1, split_amount))
                    except Exception as exc:
                        self.config_obj.logger.error('Fund {} generated an exception: {}'.format(hedge_name, exc))

    def calculate_preferred_index_from_hedge_and_output(self):
        '''
        FUNCTION:
            在自組基金TABLE中抓取基金各季度Portfolio，並且計算季/年賺賠比, 季/年夏普值, 季/年標準差, 季/年獲益率等指標。
            季賺賠比：各季度賺錢/賠錢的比值，例如10季賺錢、8季賠錢，季賺賠比10/8=1.25
            年賺賠比：各年度賺錢/賠錢的比值，例如5年賺錢、0年賠錢，年賺賠比5/0=5
            季/年標準差：以標準差公式計算各季/年度獲益波動。
            季/年獲益率：計算季/年平均獲益率。(ROI)
            季夏普值：[(每季報酬率平均值- 無風險利率0.45) / (每季報酬的標準差)]x (4平方根)
            portfolio table example:            
                QUARTER	HOLDINGS	VALUE	TOP_HOLDINGS	FORM_TYPE	DATE_FILED	FILING_ID	HEDGE_FUND
                Q4 2013	5	999976	CZWI, FCCO, HTBI	-	2014-02-14	-	FJ Capital Management LLC_share_0812
                Q1 2014	5	970620	FCCO, HTBI, ISBC	-	2014-05-15	-	FJ Capital Management LLC_share_0812
                Q2 2014	1	968670	ISBC	-	2014-08-14	-	FJ Capital Management LLC_share_0812
                Q3 2014	1	999994	ISBC	-	2014-11-14	-	FJ Capital Management LLC_share_0812
                Q4 2014	1	1085416	ISBC	-	2015-02-14	-	FJ Capital Management LLC_share_0812
                Q1 2015	1	1126270	ISBC	-	2015-05-15	-	FJ Capital Management LLC_share_0812
                Q2 2015	1	1123485	ISBC	-	2015-08-14	-	FJ Capital Management LLC_share_0812
                Q3 2015	5	999954	ABCB, DCOM, HBCP	-	2015-11-14	-	FJ Capital Management LLC_share_0812
                Q4 2015	5	916801	ABCB, DCOM, HBCP	-	2016-02-14	-	FJ Capital Management LLC_share_0812
                Q1 2016	5	966085	DCOM, HBCP, ISBC	-	2016-05-15	-	FJ Capital Management LLC_share_0812
                Q2 2016	5	987094	DCOM, HBCP, ISBC	-	2016-08-14	-	FJ Capital Management LLC_share_0812
                Q3 2016	5	999943	HBCP, ISBC, ONB	-	2016-11-14	-	FJ Capital Management LLC_share_0812
                Q4 2016	2	1133481	ISBC, VBTX	-	2017-02-14	-	FJ Capital Management LLC_share_0812
                Q1 2017	2	1073448	ISBC, VBTX	-	2017-05-15	-	FJ Capital Management LLC_share_0812
                Q2 2017	5	1032752	HBCP, ISBC, PNFP	-	2017-08-14	-	FJ Capital Management LLC_share_0812
        INPUT:
            None
        OUTPUT:
            None
        '''

        index_table = []
        customized_name_suffix = '_reinvest_0818'
        # funds_list = self.get_all_hedge_funds_name(self.config_obj.customized_individual_fund_portfolio_table)
        funds_list = self.get_all_hedge_funds_name(self.config_obj.customized_fund_portfolio_table)
        
        # funds_list = [fund.replace(customized_name_suffix, '') for fund in funds_list]
        # funds_list = funds_list[:10]
        # funds_list = ['EDMUNDS WHITE PARTNERS LLC' + customized_name_suffix]
        funds_list = [item for item in funds_list if 'share_I1C2_0828' not in item]
        funds_list = [item for item in funds_list if 'share_I1C3_0828' not in item]


        for idx, each_funds in enumerate(funds_list):
            # 計算各指標
            

            # query = self.get_each_fund_portfolio_data(table_name=self.config_obj.customized_individual_fund_portfolio_table, fund_name=each_funds)
            query = self.get_each_fund_portfolio_data(table_name=self.config_obj.customized_fund_portfolio_table, fund_name=each_funds)
            each_portfolio_data = self.sql_execute(query)
            each_portfolio_data = pd.DataFrame(each_portfolio_data)

            each_portfolio_data['Q_ROI'] = each_portfolio_data['VALUE'].pct_change()
            each_portfolio_data['YEAR'] = each_portfolio_data['QUARTER'].str[-4:]
            yearly_value = each_portfolio_data.groupby('YEAR')['VALUE'].last()
            each_portfolio_data['Y_ROI'] = each_portfolio_data['YEAR'].map(yearly_value.pct_change())

            # 計算季賺賠比
            positive_quarters = (each_portfolio_data['Q_ROI'] > 0).sum()
            negative_quarters = (each_portfolio_data['Q_ROI'] < 0).sum()
            quarter_win_loss_ratio = positive_quarters / negative_quarters

            # 計算年賺賠比
            positive_years = (each_portfolio_data.groupby('YEAR')['Y_ROI'].last() > 0).sum()
            negative_years = (each_portfolio_data.groupby('YEAR')['Y_ROI'].last() < 0).sum()
            year_win_loss_ratio = positive_years / negative_years

            # 主要持倉內容
            mainly_holdings = each_portfolio_data.tail(1)['TOP_HOLDINGS'].values
            top_holdings = mainly_holdings[0].split(',')[0]
            query = self.get_gics_by_sym(self.config_obj.us_stock_gics_table, top_holdings)
            gic_num = self.sql_execute(query)[0]['GICS_2_digit']
            industry = self.config_obj.gics_dict[gic_num]

            # 計算季標準差
            quarterly_std_dev = each_portfolio_data['Q_ROI'].std()

            # 計算年標準差
            yearly_std_dev = each_portfolio_data.groupby('YEAR')['Y_ROI'].last().std()

            # 計算季平均獲益率
            quarterly_avg_roi = each_portfolio_data['Q_ROI'].mean()

            # 計算年平均獲益率
            yearly_avg_roi = each_portfolio_data.groupby('YEAR')['Y_ROI'].last().mean()

            # 計算季夏普值
            risk_free_rate = 0.0045 # 無風險利率
            sharpe_ratio = ((quarterly_avg_roi - risk_free_rate) / quarterly_std_dev) * (4 ** 0.5)

            # 結果輸出
            # print(f'季賺賠比: {quarter_win_loss_ratio}')
            # print(f'年賺賠比: {year_win_loss_ratio}')
            # print(f'季標準差: {quarterly_std_dev}')
            # print(f'年標準差: {yearly_std_dev}')
            # print(f'季平均獲益率: {quarterly_avg_roi}')
            # print(f'年平均獲益率: {yearly_avg_roi}')
            # print(f'季夏普值: {sharpe_ratio}')
            # print(each_portfolio_data)

            index_table.append({
                'HEDGE_FUND': each_funds,#.replace(customized_name_suffix, ''),
                '總季度': len(each_portfolio_data),
                '主要持倉': mainly_holdings,
                'I1產業': industry,
                '季賺賠比': quarter_win_loss_ratio,
                '年賺賠比': year_win_loss_ratio,
                '季標準差': quarterly_std_dev,
                '年標準差':yearly_std_dev,
                '季平均獲益率': quarterly_avg_roi,
                '年平均獲益率': yearly_avg_roi,
                '季夏普值': sharpe_ratio,
            })

            self.config_obj.logger.warning('Completed calculating index for fund {}. Progress: {}/{}'.format(each_funds, idx+1, len(funds_list)))

        df = pd.DataFrame(index_table)
        output_path = os.path.join(self.config_obj.backtest_summary, str(datetime.datetime.now()).split()[0] + '_all_hedge_funds_backtest.xlsx')
        df.to_excel(output_path, index=False)
            
    def customized_hedge_build_and_store(self, customized_name_string=None, reinvest_flag=None, share_profit_flag=None, hedge_funds_range_list=None, industry_top_selection=None, company_top_selection=None, mcap_weighted_flag=None):
        
        customized_fund_list = {
            # customized_name_string: (self.customize_fund_components_revised, 
            #                          {'reinvest_flag':reinvest_flag, 
            #                           'share_profit_flag':share_profit_flag, 
            #                           'hedge_funds_range': hedge_funds_range_list, 
            #                           'industry_top_selection': industry_top_selection, 
            #                           'company_top_selection': company_top_selection, 
            #                           'mcap_weighted_flag': mcap_weighted_flag
            #                           }),

            'I1C2_sharpe_2024Q4' : (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': self.config_obj.target_hedge_funds_dict['I1C2_sharpe'], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),

            # 'I1C2_reinvest_0905': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': self.config_obj.target_hedge_funds_dict['I1C2_sharpe'], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
            # 'I1C2_share_0905': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': self.config_obj.target_hedge_funds_dict['I1C2_sharpe'], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
            # 'I1C3_reinvest_0905': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': self.config_obj.target_hedge_funds_dict['I1C2_sharpe'], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            # 'I1C3_share_0905': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': self.config_obj.target_hedge_funds_dict['I1C2_sharpe'], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),

        #    'cincin_test_0901': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Cincinnati Specialty Underwriters Insurance CO'], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}), 
        }
        holdings_dict = {}
        portfolio_dict = {}
        # self.config_obj.logger.warning('準備建置 {} 組自定義基金'.format(len(customized_fund_list)))
        for k_, v_ in customized_fund_list.items():
            # self.config_obj.logger.warning('建置自定義基金數據 {}'.format(k_))
            func_to_call, params = v_
            customized_fund_data, customized_table = func_to_call(**params)

            holdings_data = self.modify_customized_fund_data_to_holdings_data_structures(k_, customized_fund_data)
            portfolio_data = self.arrage_customized_fund_portfolio_data(k_, holdings_data)
            holdings_dict[k_] = holdings_data
            portfolio_dict[k_] = portfolio_data
            # self.config_obj.logger.debug(holdings_data)
            # self.config_obj.logger.debug(portfolio_data)
        # path = os.path.join(self.config_obj.backtest_summary, str(datetime.datetime.now()).split()[0] + '_customized_data.xlsx')
        # with pd.ExcelWriter(path) as writer:
        #     for k_, v_ in holdings_dict.items():
        #         holdings_dict[k_].to_excel(writer, index=False, sheet_name=k_)
        #         portfolio_dict[k_].to_excel(writer, index=False, sheet_name=k_ + '_portfolio')



        for k_, v_ in holdings_dict.items():
            self.config_obj.logger.warning('Costomized_hedge:{}'.format(k_))
            table_name, inserted_rows = self.insert_records_to_DB(table_name=self.config_obj.customized_holdings_data_table, data=v_)                             #自組基金
            # table_name, inserted_rows = self.insert_records_to_DB(table_name=self.config_obj.customized_individual_holdings_data_table, data=v_)                    #獨立基金
            self.config_obj.logger.warning('資料庫數據Insert:TABLE{} 筆數{}'.format(table_name, inserted_rows))
            table_name, inserted_rows = self.insert_records_to_DB(table_name=self.config_obj.customized_fund_portfolio_table, data=portfolio_dict[k_])            #自組基金
            # table_name, inserted_rows = self.insert_records_to_DB(table_name=self.config_obj.customized_individual_fund_portfolio_table, data=portfolio_dict[k_])   #獨立基金
            self.config_obj.logger.warning('資料庫數據Insert:TABLE{} 筆數{}'.format(table_name, inserted_rows))

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
        query = self.create_query_data_table(self.config_obj.hedge_fund_portfolio_table)
        fund_data = self.sql_execute(query)
        fund_data = pd.DataFrame(fund_data)
        hedge_fund_list = fund_data['HEDGE_FUND'].unique()
        # hedge_fund_list = ['Appaloosa', ]
        hedge_fund_list = list(hedge_fund_list)
        # hedge_fund_list = ['Scion Asset Management']
        hedge_fund_list.remove('Citadel Advisors')
        hedge_fund_list.remove('Renaissance Technologies')
        hedge_fund_list.remove('Millennium Management')
        self.config_obj.logger.warning("Back_Test_flow，總共包含{}個對沖基金資料".format(len(hedge_fund_list)))
        # print('Hedge Funds:', )
        # print(hedge_fund_list)

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
            self.config_obj.logger.warning("Back_Test_flow，對沖基金{}，最後一個季度：{}".format(hedge_fund, quarters_list[-1]))
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
        # self.config_obj.logger.warning("Web_crawler_13F，預計爬取共{}支基金資料".format(len(urls)))

        #Fund components by each hedge analysis
        path = os.path.join(self.config_obj.backtest_summary, str(datetime.datetime.now()).split()[0] + '_customized_table_by_hedge.xlsx')
        with pd.ExcelWriter(path) as writer:  
            for fund_name, components_table in fund_components_dict_by_hedge.items():
                components_table.to_excel(writer, index=False, sheet_name=fund_name)

    def customize_fund_components_revised(self, reinvest_flag, share_profit_flag, hedge_funds_range, industry_top_selection, company_top_selection, mcap_weighted_flag): 
        
        '''
        相較於customize_fund_components()，迴圈先Quarter再Hedge，使得每季可以計算總資金變化。

        function:
            製作自定義基金。
        Input:
            -.industry_top_selection: int
                各基金取產業市值前幾
            -.company_top_selection: int
                各基金產業，取個股市值前幾
            -.mcap_weighted_flag
                產業、個股是否以市值加權 (mv: market)
        '''
        save_excel_details_flag = False #控制是否產製Excel文件查看計算細節
        enter_date = self.config_obj.customize_enter_date #2019 2/15 開始進場
        # hedge_funds = self.config_obj.target_hedge_funds_dict['XIRR_output_filter'] #XIRR表現在波克夏以上的基金
        # hedge_funds = self.config_obj.target_hedge_funds_dict['sharpe_output_filter'] #計算已平倉獲利後，依照sharpe ratio、勝算比排序
        hedge_funds = hedge_funds_range
        
        enter_cost = self.config_obj.enter_cost
        '''定義Output obj、統計用obj'''
        ### Final Output Form
        adjusted_fund_data = None
        customized_table = None

        '''Read DB data'''
        # query = self.create_query_data_table(self.config_obj.hedge_fund_portfolio_table)
        query = self.create_query_data_table(self.config_obj.hedge_fund_portfolio_table_filtered)
        fund_data = self.sql_execute(query)
        fund_data = pd.DataFrame(fund_data)
        fund_data = fund_data[fund_data['HEDGE_FUND'].isin(hedge_funds)]
        hedge_fund_list = hedge_funds


        '''各hedge fund計算迴圈'''
        for idx, hedge_fund in enumerate(hedge_fund_list):
            '''調整fund_data'''
            each_fund_data = self.each_fund_data_adjust(fund_data, hedge_fund, enter_date)

            if adjusted_fund_data is None:
                    adjusted_fund_data = each_fund_data
            else:
                adjusted_fund_data = pd.concat([adjusted_fund_data, each_fund_data], ignore_index=True)

        adjusted_fund_data = adjusted_fund_data.sort_values(by=['BASE_DATE'], ascending=True).reset_index(drop=True)
        # print(adjusted_fund_data)
        
        date_list = adjusted_fund_data['BASE_DATE'].unique()
        quarters_list = adjusted_fund_data['QUARTER'].unique()
        date_13F_list = adjusted_fund_data['13F_DATE'].unique()
        

        holdings_dict = {}
        GICs_dict = {}
        processed_dict = {}
        industries_select_dict = {}
        company_select_dict = {}

        for idx_q, (quarter, holdings_time, date_13F) in enumerate(zip(quarters_list, date_list, date_13F_list)):

            q_customized_table = None
            holdings_time = self.adjust_holdings_time(holdings_time, self.us_sorted_dates, next_day=False) # 季末當日，即3/31、6/30、9/30、12/31
            date_13F = self.adjust_holdings_time(date_13F, self.us_sorted_dates, next_day=False)
            sub_fund_list = adjusted_fund_data[adjusted_fund_data['QUARTER']==quarter]['HEDGE_FUND'].values

            
            for idx, hedge_fund in enumerate(sub_fund_list):

                filing_number = adjusted_fund_data[(adjusted_fund_data['QUARTER']==quarter) & (adjusted_fund_data['HEDGE_FUND']==hedge_fund)]['FILING_ID'].values[0]
                query = self.create_query_holdings_with_gics_n_price(hedge_fund, quarter, filing_number, holdings_time)
                holdings_data_Q_date = self.sql_execute(query)
                holdings_data_Q_date = pd.DataFrame(holdings_data_Q_date)

                query = self.create_query_holdings_with_gics_n_price(hedge_fund, quarter, filing_number, date_13F)
                holdings_data_13F_date = self.sql_execute(query)
                holdings_data_13F_date = pd.DataFrame(holdings_data_13F_date)

                if holdings_data_Q_date.empty or holdings_data_13F_date.empty:
                    sub_fund_list = sub_fund_list[sub_fund_list != hedge_fund]
                    print(f"Season {quarter} remove hedge {hedge_fund} because NO DATA.")
                    continue

                holdings_data = pd.merge(holdings_data_Q_date, holdings_data_13F_date[['SYM', 'QUARTER', 'date', 'price']], on=['SYM', 'QUARTER'], how='inner')
                holdings_data = holdings_data.rename(columns={'date_y': 'date_13F', 'price_y': 'price_13F'})    # 13F報告日
                holdings_data = holdings_data.rename(columns={'date_x': 'date', 'price_x': 'price'})            # 基金實際資產時間(季末)
                                
                holdings_data, holdings_GICs_data = self.group_by_GICs_from_holdings_data(holdings_data) # holdings_data新增欄位；建置holdings_GICs_data
                '''儲存holdings_dict'''
                if save_excel_details_flag:
                    if hedge_fund not in holdings_dict.keys():
                        holdings_dict[hedge_fund] = holdings_data
                    else:
                        holdings_dict[hedge_fund] = pd.concat([holdings_dict[hedge_fund], holdings_data], ignore_index=True)
                '''儲存GICs_dict'''
                if save_excel_details_flag:
                    if hedge_fund not in GICs_dict.keys():
                        GICs_dict[hedge_fund] = holdings_GICs_data
                    else:
                        GICs_dict[hedge_fund] = pd.concat([GICs_dict[hedge_fund], holdings_GICs_data], ignore_index=True)
                processed_data, industry_result_data, company_result_data = self.select_company_from_holdings_adjusted(holdings_data, holdings_GICs_data, industry_top_selection, company_top_selection)

                '''儲存industries_select_dict'''
                if save_excel_details_flag:
                    if hedge_fund not in industries_select_dict.keys():
                        industries_select_dict[hedge_fund] = industry_result_data
                    else:
                        industries_select_dict[hedge_fund] = pd.concat([industries_select_dict[hedge_fund], industry_result_data], ignore_index=True)
                ''''''
                if save_excel_details_flag:
                    if hedge_fund not in processed_dict.keys(): #re-weight for company
                        processed_dict[hedge_fund] = processed_data
                    else:
                        processed_dict[hedge_fund] = pd.concat([processed_dict[hedge_fund], processed_data], ignore_index=True)
                '''儲存company_select_dict'''
                if save_excel_details_flag:
                    if hedge_fund not in company_select_dict.keys():
                        company_select_dict[hedge_fund] = company_result_data
                    else:
                        company_select_dict[hedge_fund] = pd.concat([company_select_dict[hedge_fund], company_result_data], ignore_index=True)
                hedge_num = len(sub_fund_list) #本季有幾間hedge
                company_result_data_copy = company_result_data.copy()

                # 依據投資時間(即13F報告時間)，進行篩選額外操作(若需要，例如高價股刪除)，並將資金分配至個股，計算建議購買股數(shares_to_buy)。
                customized_holdings = self.calculate_customized_shares(company_result_data_copy, enter_cost, hedge_num, mcap_weighted_flag)
                if q_customized_table is None:
                    q_customized_table = customized_holdings
                else:
                    q_customized_table = pd.concat([q_customized_table, customized_holdings], ignore_index=True)

            q_customized_table = self.individual_stock_filtering(q_customized_table, upper_price_limit=self.config_obj.upper_price_limit)

            q_customized_table_by_stock = self.arrange_customized_table(q_customized_table) #合併share_to_buy from different hedge fund
            if customized_table is None:
                customized_table = q_customized_table_by_stock
            else:
                customized_table = pd.concat([customized_table, q_customized_table_by_stock], ignore_index=True)

            if reinvest_flag: #利潤再投入

                if share_profit_flag:
                    if 'Q2' in quarter: #13F Q3報告出來，即11/15開始分潤計算、重置資金。
                        new_cmap = self.calculate_market_price_growth(q_customized_table_by_stock) #下一季的投資金額
                        enter_cost = self.config_obj.enter_cost if new_cmap > self.config_obj.enter_cost else new_cmap #若賠錢則無分潤(沿用new_cmap)，若賺錢則分潤後，重置資金
                    else:
                        enter_cost = self.calculate_market_price_growth(q_customized_table_by_stock) #下一季的投資金額
                # print('{}-MCAP:{}'.format(quarter, enter_cost))
                else:
                    enter_cost = self.calculate_market_price_growth(q_customized_table_by_stock) #下一季的投資金額

        '''
        輸出Excel
        '''
        # if save_excel_details_flag:
        #     path = os.path.join(self.config_obj.backtest_summary, str(datetime.datetime.now()).split()[0] + '_原持股_I{}C{}.xlsx'.format(industry_top_selection, company_top_selection))
        #     with pd.ExcelWriter(path) as writer:
        #         for k_, v_ in holdings_dict.items():
        #             holdings_dict[k_].to_excel(writer, index=False, sheet_name=k_)
            
        #     path = os.path.join(self.config_obj.backtest_summary, str(datetime.datetime.now()).split()[0] + '_原產業_I{}C{}.xlsx'.format(industry_top_selection, company_top_selection))
        #     with pd.ExcelWriter(path) as writer:
        #         for k_, v_ in GICs_dict.items():
        #             GICs_dict[k_].to_excel(writer, index=False, sheet_name=k_)

        #     path = os.path.join(self.config_obj.backtest_summary, str(datetime.datetime.now()).split()[0] + '_篩選產業_I{}C{}.xlsx'.format(industry_top_selection, company_top_selection))
        #     with pd.ExcelWriter(path) as writer:
        #         for k_, v_ in industries_select_dict.items():
        #             industries_select_dict[k_].to_excel(writer, index=False, sheet_name=k_)

        #     path = os.path.join(self.config_obj.backtest_summary, str(datetime.datetime.now()).split()[0] + '_持股重新配重_I{}C{}.xlsx'.format(industry_top_selection, company_top_selection))
        #     with pd.ExcelWriter(path) as writer:
        #         for k_, v_ in processed_dict.items():
        #             processed_dict[k_].to_excel(writer, index=False, sheet_name=k_)


        #     path = os.path.join(self.config_obj.backtest_summary, str(datetime.datetime.now()).split()[0] + '_篩選持股_I{}C{}.xlsx'.format(industry_top_selection, company_top_selection))
        #     with pd.ExcelWriter(path) as writer:
        #         for k_, v_ in company_select_dict.items():
        #             company_select_dict[k_].to_excel(writer, index=False, sheet_name=k_)
            
        # self.config_obj.logger.debug('Costomized holdings table shape:{}'.format(customized_table.shape))
        # self.config_obj.logger.debug(customized_table)
        # print(customized_table)
        return customized_table, None

    def customize_fund_components(self, industry_top_selection, company_top_selection, mcap_weighted_flag):
        '''
        function:
            製作自定義基金。
        Input:
            -.industry_top_selection: int
                各基金取產業市值前幾
            -.company_top_selection: int
                各基金產業，取個股市值前幾
            -.mcap_weighted_flag
                產業、個股是否以市值加權 (mv: market)
        Output:
            仿製基金13F holdings table，包含欄位SYM、SHARES、date、price
        1.2019 2/15 開始進場
        2.XIRR表現在波克夏以上的基金 or 其他選擇方式(-. 單筆獲利計算夏普值排序)
        3.各基金的前(三)市值產業
        4.前(三)市值產業 的前(三)市值股票
        5.以100萬當入場金
        6.用price回推要購買的SHARES，去進場
            SYM、SHARES、date、price
        7.回測計算MDD、XIRR
        '''
        enter_date = self.config_obj.customize_enter_date #2019 2/15 開始進場
        # hedge_funds = self.config_obj.target_hedge_funds_dict['XIRR_output_filter'] #XIRR表現在波克夏以上的基金
        hedge_funds = self.config_obj.target_hedge_funds_dict['sharpe_output_filter'] #計算已平倉獲利後，依照sharpe ratio、勝算比排序
        
        enter_cost = self.config_obj.enter_cost
        '''定義Output obj、統計用obj'''
        ### Final Output Form
        customized_table = None

        '''Read DB data'''
        query = self.create_query_data_table(self.config_obj.hedge_fund_portfolio_table)
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
            date_list = each_fund_data['13F_DATE'].values #date_list = each_fund_data['DATE_FILED'].values # 進場時間點使用該基金13F公布時間
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

                holdings_time = self.adjust_holdings_time(holdings_time, self.us_sorted_dates) # 修正為有開市的日期(美股or台股)
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
                    customized_holdings = self.calculate_customized_shares(customized_holdings, enter_cost, hedge_num, mcap_weighted_flag)

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
        
        # 判斷是否是DML操作
        if query.strip().upper().startswith("DELETE") or query.strip().upper().startswith("INSERT") or query.strip().upper().startswith("UPDATE"):
            rows_affected = cursor.rowcount
            conn.commit()  # 確保DML操作被提交
            cursor.close()
            conn.close()
            return rows_affected
        else:
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

    def each_fund_data_adjust(self, fund_data, hedge_fund, enter_date):
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
            -. enter_date string

        Output:
            調整後之fund_data
                1.新增BASE_DATE欄位，多用於後面查price。
                2.刪除多餘的報告，每季基本上只留下一組資料。
        '''
        # 依據hedge_fund string篩選基金
        # 刪除以下欄位：'HOLDINGS'(持股個數), 'VALUE'(總市值), 'TOP_HOLDINGS'(最高持股量的個股)
        # 重製dataframe index
        df = fund_data[fund_data['HEDGE_FUND']==hedge_fund].drop(['HOLDINGS', 'VALUE', 'TOP_HOLDINGS', ], axis=1)

        #某些狀況要使用13F報告公布日期
        def calculate_13F_date(row):
            year = int(row['QUARTER'].split()[1])
            if 'Q1' in row['QUARTER']:
                return datetime.datetime(year, 5, 15)
            elif 'Q2' in row['QUARTER']:
                return datetime.datetime(year, 8, 14)
            elif 'Q3' in row['QUARTER']:
                return datetime.datetime(year, 11, 14)
            elif 'Q4' in row['QUARTER']:
                return datetime.datetime(year + 1, 2, 14)
        #某些狀況要使用每季最後一天
        def calculate_base_date(row):
            year = int(row['QUARTER'].split()[1])
            if 'Q1' in row['QUARTER']:
                return datetime.datetime(year, 3, 31)
            elif 'Q2' in row['QUARTER']:
                return datetime.datetime(year, 6, 30)
            elif 'Q3' in row['QUARTER']:
                return datetime.datetime(year, 9, 30)
            elif 'Q4' in row['QUARTER']:
                return datetime.datetime(year, 12, 31)
        # 新增欄位BASE_DATE，依據QUARTER欄位的資料，回傳Q1->5/15, Q2->8/14, Q3->11/14, Q4->2/14等日期

        df['13F_DATE'] = df.apply(calculate_13F_date, axis=1)# 新增 '13F DATE' 基準日期欄位 (即5/15, 8/14, 11/14, 2/14)
        df['BASE_DATE'] = df.apply(calculate_base_date, axis=1)# 新增 'BASE DATE' 基準日期欄位 (即3/31, 6/30, 9/30, 12/31)
        df['DATE_FILED'] = pd.to_datetime(df['DATE_FILED'])# 將 'DATE_FILED' 轉換為日期時間格式 (此為確切公告日期)
        
        # 時間篩選
        enter_date = pd.to_datetime(enter_date)
        df['DATE_FILED'] = pd.to_datetime(df['DATE_FILED'])
        df = df[df['DATE_FILED'] >= enter_date].reset_index(drop=True) # 刪除DATE_FILED欄位資料在enter_date以前的records
        df = df[df['BASE_DATE'] >= enter_date].reset_index(drop=True) # 刪除DATE_FILED欄位資料在enter_date以前的records
        

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

        '''有時只有RESTATEMENT資料，故下式先移除'''
        # df = df[~((df['FORM_TYPE'] == 'RESTATEMENT') & (df['DATE_FILED'] > df['BASE_DATE']))] # 刪除 RESTATEMENT 大於 基準日期的Records。


        sorting_key = df.groupby('BASE_DATE')['FILING_ID'].idxmax() # 取group中最大FILING_ID，因為可能有多筆records，直接依據FILING_ID取最新的。
        df = df.loc[sorting_key].reset_index(drop=True)

        return df
    def adjust_holdings_time(self, holdings_time, sorted_dates, next_day=True):
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
    def create_query_holdings(self, fund, quarter, filing_number):
        '''
        依據fund和quarter篩選holdings資料表的query語句
        '''
        holdings_table = self.config_obj.holdings_data_table
        holdings_table = self.config_obj.holdings_data_table_filtered
        query = '''SELECT * FROM {} WITH(NOLOCK) 
        WHERE [HEDGE_FUND] = '{}' 
        AND [QUARTER] = '{}' 
        AND [FILING_ID] = '{}'
        AND [OPTION_TYPE] IS NULL
        AND SUBSTRING([CUSIP], 7, 2) = '10'
        AND [SHARES] IS NOT NULL 
        '''.format(holdings_table, fund, quarter, filing_number)
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
        # merged_data['SHARES_current'].fillna(0, inplace=True)
        # merged_data['SHARES_previous'].fillna(0, inplace=True)
        merged_data.fillna({'SHARES_current': 0, 'SHARES_previous': 0}, inplace=True)
        merged_data = merged_data.astype({'SHARES_current': int, 'SHARES_previous': int})
        # 計算持股數量變化
        merged_data['SHARES_CHANGE'] = merged_data['SHARES_current'] - merged_data['SHARES_previous']
        return merged_data
    def create_query_get_open_price_by_join_holdings_n_price(self, SYMs_tuple, date, fund, quarter, filing_number):
        '''
        依據holdings去查表price，透過stock_id(即holdings表中的SYM) join兩張表格，並加入SHARES資訊至price表。
        '''
        price_table = self.config_obj.us_stock_price_table
        # holdings_table = self.config_obj.holdings_data_table
        holdings_table = self.config_obj.holdings_data_table_filtered
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

        price_table = self.config_obj.us_stock_price_table
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
        price_table = self.config_obj.us_stock_price_table
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
            shares_change = row['SHARES_CHANGE']
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
        merged_data['scaling'] = merged_data['SHARES_CHANGE'] * merged_data['Open_current']
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
            price_table = self.config_obj.tw_stock_price_table
        elif market == 'us':
            price_table = self.config_obj.us_stock_price_table
        else:
            price_table = self.config_obj.us_stock_price_table

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

        # holdings_table = self.config_obj.holdings_data_table
        holdings_table = self.config_obj.holdings_data_table_filtered
        gics_table = self.config_obj.us_stock_gics_table
        price_table = self.config_obj.us_stock_price_table
        query = '''
        SELECT tb_hedge.[SYM], tb_hedge.[SHARES], tb_hedge.[HEDGE_FUND], tb_hedge.[QUARTER], tb_hedge.[GICS], tb_price.[date] date, tb_price.[Open] price FROM
        (SELECT tb_holdings.[SYM], tb_holdings.[SHARES], tb_holdings.[HEDGE_FUND], tb_holdings.[QUARTER], tb_gics.[GICS_2_digit] GICS
        FROM {} tb_holdings WITH(NOLOCK)
        INNER JOIN {} tb_gics WITH(NOLOCK)
        ON tb_holdings.SYM = tb_gics.Ticker 
        WHERE 
            tb_holdings.[HEDGE_FUND] = '{}' 
            AND tb_holdings.[QUARTER] = '{}' 
            AND tb_holdings.[FILING_ID] = '{}'
            AND tb_holdings.[OPTION_TYPE] IS NULL
            AND tb_holdings.[SHARES] IS NOT NULL
            AND SUBSTRING(tb_holdings.[CUSIP], 7, 2) = '10'
            ) tb_hedge
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
        # print(top_gics)
        return top_gics
    def group_by_GICs_from_holdings_data(self, holdings_data):
        '''
        FUNCTION:
            1. holdings_data新增欄位
                holdings_data新增market_price、WEIGHTS、WEIGHTS_diff
            2. 建置gics_df
                gics_df是group by holdings_data on GICS，欄位包含['GICS','INDUSTRY', 'WEIGHTS', 'WEIGHTS_diff','market_price','HEDGE_FUND','QUARTER','date',]
        INPUT:
            -. holdings_data: pd.DataFrame
        OUTPUT:
            -. holdings_data: pd.DataFrame
            -. gics_df: pd.DataFrame
        '''

        # holdings_data新增欄位：計算個股市值(market_price) & 對應的權重(WEIGHTS)
        holdings_data['market_price'] = holdings_data['price'] * holdings_data['SHARES']
        company_market_values = holdings_data['market_price'].sum()
        holdings_data['WEIGHTS'] = holdings_data['market_price'] / company_market_values
        
        # holdings_data新增欄位：加入各GICs英文名稱欄位(INDUSTRY)
        def get_GICs_industry_name(row):
            return self.config_obj.gics_dict[row['GICS']]
        holdings_data['INDUSTRY'] = holdings_data.apply(get_GICs_industry_name, axis=1)

        # holdings_data新增欄位：以市值排序並生成WEIGHTS_diff欄位
        holdings_data = holdings_data.sort_values('market_price', ascending=False)
        holdings_data['WEIGHTS_diff'] = holdings_data['WEIGHTS'].diff()

        #新增DataFrame：Group by產業別，產製新的DataFrame(gics_df)，並計算產業市值(SUM UP market_price)
        gics_df = holdings_data.groupby('GICS', as_index=False).agg({'INDUSTRY':'first', 'market_price':'sum', 'HEDGE_FUND':'first', 'QUARTER':'first', 'date':'first'})
        gics_df = gics_df.sort_values('market_price', ascending=False)

        # gics_df新增欄位：計算產業市值(market_price) & 對應的權重(WEIGHTS)
        # gics_df新增欄位：生成WEIGHTS_diff欄位
        industry_market_values = gics_df['market_price'].sum()
        gics_df['WEIGHTS'] = gics_df['market_price'] / industry_market_values
        gics_df['WEIGHTS_diff'] = gics_df['WEIGHTS'].diff()
        gics_df = gics_df[['GICS','INDUSTRY', 'WEIGHTS', 'WEIGHTS_diff','market_price','HEDGE_FUND','QUARTER','date',]]

        return holdings_data, gics_df   
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
        # print(customized_holdings)
        customized_holdings = customized_holdings.groupby('GICS').apply(lambda x: x.nlargest(company_top_selection, 'market_price')).reset_index(drop=True)
        return customized_holdings    
    def select_company_from_holdings_adjusted(self, holdings_data, holdings_GICs_data, industry_top_selection, company_top_selection):
        '''
        FUNCTION:
            依據3個邏輯進行產業篩選：
            1. 剔除WEIGHTS_diff小於-30%的產業，即市值不能差前面的太多。(weights_diff_filter())
            2. 加入WEIGHTS_diff大於-2%的產業，即市值如果只差前面一個產業2%以內，就納入。(weights_diff_addition())
            3. 取前industry_top_selection數量的產業。
                先執行(1)，再執行(3)，最後執行(2)，獲得selected_industries

            依據2個邏輯進行產業篩選：
            1. 依據selected_industries，篩選holdings_data裡面的產業(GICs)。
            2. (FOR迴圈)每個產業，依市值計算個股的WEIGHTS、WEIGHTS_diff
                依照上面產業篩選的3個邏輯進行 [產業內的個股] 篩選。
                先執行(1)，再執行(3)，最後執行(2)，獲得company_result_data
            
            
            #篩選出高價股，以利後續刪除(現在刪除的話，若刪除後剩下Empty dataframe會使得enter_cost在calculate_customized_shares執行時分配不均)

        INPUT:
            -. holdings_data, 
            -. holdings_GICs_data, 
            -. industry_top_selection, 
            -. company_top_selection
        OUTPUT:
            -. processed_data, 
            -. industry_result_data, 
            -. company_result_data
        '''
        holdings_data = holdings_data.reset_index(drop=True)
        holdings_GICs_data = holdings_GICs_data.reset_index(drop=True)
        # print('holdings_GICs_data', holdings_GICs_data)
        '''1. 產業篩選'''
        def weights_diff_filter(data): #Top1若 > Top2 30%以上，則不繼續取Top2，依此類推
            indexes = []
            for index, row in data.iterrows(): 
                if index == 0: #情況一: 第一筆資料，納入
                    indexes.append(index)
                    pass
                elif row['WEIGHTS_diff'] > -0.3: #情況二: 後續資料都要 > -30% 才納入
                    indexes.append(index)
                else:   #情況三: 有發生 < -30% 就break
                    break
            return indexes
        industry_weights_diff_filter_idx = weights_diff_filter(holdings_GICs_data)
        # print('industry_weights_diff_filter_idx', industry_weights_diff_filter_idx)
        def weights_diff_addition(data): #Top1~3之外，若第4名差距<2% --> 納入，依此類推
            marked_indexes = []
            for index, row in data.iterrows():
                if row['WEIGHTS_diff'] > -0.02:
                    marked_indexes.append(index)
            return marked_indexes
        industry_weights_diff_addition_idx = weights_diff_addition(holdings_GICs_data)
        # print('industry_weights_diff_addition_idx', industry_weights_diff_addition_idx)
        #捨棄與前面差距30%以上的產業
        filtered_data = holdings_GICs_data.loc[industry_weights_diff_filter_idx]
        #套用自訂參數，選擇前industry_top_selection
        top_filtered_data = filtered_data.iloc[0:industry_top_selection,:]

        # print('top_filtered_data', top_filtered_data)
        #加入與前項差距小於2%的產業
        from_idx = len(top_filtered_data)
        addition_indexes = []
        for index, row in holdings_GICs_data.iterrows():
            if (index >= from_idx) & (index in industry_weights_diff_addition_idx):
                addition_indexes.append(index)
            elif (index in top_filtered_data.index):
                pass
            else:
                break
        addtion_rows = holdings_GICs_data.loc[addition_indexes]
        industry_result_data = pd.concat([top_filtered_data, addtion_rows], ignore_index=True)
        # print('industry_result_data', industry_result_data)
        selected_industries = industry_result_data['GICS'].values

        '''2. 公司篩選'''
        company_result_data = None
        processed_data = None

        for ind in selected_industries:
            # print('產業', ind)
            sub_data = holdings_data[holdings_data['GICS'] == ind].copy() # 前一步驟所篩選的產業
            sub_data = sub_data.reset_index(drop=True)

            #重新計算占比
            company_market_values = sub_data['market_price'].sum()
            sub_data['WEIGHTS'] = sub_data['market_price'] / company_market_values
            sub_data['WEIGHTS_diff'] = sub_data['WEIGHTS'].diff()
            if processed_data is None:
                processed_data = sub_data
            else:
                processed_data = pd.concat([processed_data, sub_data], ignore_index=True)

            company_weights_diff_filter_idx = weights_diff_filter(sub_data)
            company_weights_diff_addition_idx = weights_diff_addition(sub_data)
            # print('company_weights_diff_filter_idx')
            # print(company_weights_diff_filter_idx)
            # print('company_weights_diff_addition_idx')
            # print(company_weights_diff_addition_idx)
            #捨棄與前面差距30%以上的公司
            filtered_data = sub_data.loc[company_weights_diff_filter_idx]
            #套用自訂參數，選擇前company_top_selection
            top_filtered_data = filtered_data.iloc[0:company_top_selection,:]
            #加入與前項差距小於2%的公司
            from_idx = len(top_filtered_data)
            addition_indexes = []
            for index, row in sub_data.iterrows():
                if (index >= from_idx) & (index in company_weights_diff_addition_idx):
                    addition_indexes.append(index)
                elif (index in top_filtered_data.index):
                    pass
                else:
                    break
            # print('addition_indexes')
            # print(addition_indexes)
            addtion_rows = sub_data.loc[addition_indexes]
            sub_result_data = pd.concat([top_filtered_data, addtion_rows], ignore_index=True)
            # print('sub_result_data')
            # print(sub_result_data)

            if company_result_data is None:
                company_result_data = sub_result_data
            else:
                company_result_data = pd.concat([company_result_data, sub_result_data], ignore_index=True)
        
        company_result_data = company_result_data.head(5)
        # 篩選出高價股
        # filtered_result_data = company_result_data[company_result_data['price_13F'] >= 1000].copy()
        # if filtered_result_data.empty:
        #     pass
        # else:
        #     print('company_result_data', company_result_data)
        #     print('filtered_result_data', filtered_result_data)
        return processed_data, industry_result_data, company_result_data   
    def calculate_customized_shares(self, data, enter_cost, hedge_num, mcap_weighted_flag=True):
        '''
        FUNCTION:
            依據投資時間(即13F報告時間)，進行篩選額外操作(若需要，例如高價股刪除)，將資金分配至個股，並計算建議購買股數(shares_to_buy)。
            0. 依據價格篩選個股(2024-06-15:由於有些股價過高，造成資金分配後無法購買) --> 改寫在 individual_stock_filtering()
            1. 計算13F報告時間市值。
            2. (FOR迴圈)依據產業分配資金，(FOR迴圈)再依據產業內個股分配資金
            3. customized_holdings新增欄位: investment_amount投資金額
            4. customized_holdings新增欄位: shares_to_buy建議購買股數
            5.  customized_holdings新增欄位: shares_origin建議購買股數保留值(因shares_to_buy後續會修正)
        INPUT:
            -. customized_holdings: pd.DataFrame
                ['SYM', 'SHARES', 'HEDGE_FUND', 'QUARTER', 'GICS', 'date', 'price', 'date_13F', 'price_13F', 'market_price', 'WEIGHTS', 'INDUSTRY', 'WEIGHTS_diff']
            -. enter_cost: int
                入場資金
            -. hedge_num: int
                參考的基金數量，用來分配資金
            -. mcap_weighted_flag
        OUTPUT:
            -. customized_holdings: pd.DataFrame
                更新後的customized_holdings
        '''
        customized_holdings = data[['SYM', 'SHARES', 'HEDGE_FUND', 'QUARTER', 'GICS', 'date_13F', 'price_13F', ]].copy()
        # market price是季末(date)的市值，用來篩選產業、公司；而date_13F是相隔1.5個月的時間，應該計算新的市值market price 13F date
        customized_holdings.loc[:, 'market_price_13F_date'] = customized_holdings['SHARES'] * customized_holdings['price_13F']
        if mcap_weighted_flag:# 計算每個持股的投資金額(方法1. 加權分配)
            industry_market_values = customized_holdings.groupby('GICS')['market_price_13F_date'].sum()
            weights = industry_market_values / industry_market_values.sum()
            industry_allocation = enter_cost / hedge_num * weights #各產業依照比重分配資金
            # 根据每个GICS的市值分配資金到每支股票
            for gics, allocation in industry_allocation.items():
                subset = customized_holdings[customized_holdings['GICS'] == gics].copy()
                total_market_value = subset['market_price_13F_date'].sum()
                subset['investment_amount'] = allocation * (subset['market_price_13F_date'] / total_market_value)
                customized_holdings.loc[customized_holdings['GICS'] == gics, 'investment_amount'] = subset['investment_amount']
        else:# 計算每個持股的投資金額(方法2. 平均分配)
            customized_holdings['investment_amount'] = enter_cost / hedge_num / len(customized_holdings)
        customized_holdings['shares_to_buy'] = customized_holdings['investment_amount'] / customized_holdings['price_13F']

        # 取整數
        # na_inf_records = customized_holdings[customized_holdings['shares_to_buy'].isna() | np.isinf(customized_holdings['shares_to_buy'])]
        # print(na_inf_records)
        customized_holdings['shares_to_buy'] = customized_holdings['shares_to_buy'].replace([np.inf, -np.inf], np.nan)
        customized_holdings['shares_to_buy'] = customized_holdings['shares_to_buy'].fillna(0).astype(int)
        customized_holdings['shares_to_buy'] = customized_holdings['shares_to_buy'].astype(int) #取整數

        customized_holdings['shares_origin'] = customized_holdings['shares_to_buy'].copy()

        return customized_holdings
    def individual_stock_filtering(self, q_customized_table, upper_price_limit):
        '''
        FUNCTION:
            針對每季篩選出的數據(個股)進行額外篩選，並且重新分配多出的資金給其他個股
            (internal表示分給該基金內其他個股；external表示分給該基金以外的其他基金個股)
        INPUT:
            q_customized_table: pd.DataFrame
                篩選前的df
        OUTPUT:
            data: pd.DataFrame
                篩選後的df
        '''
        
        data = q_customized_table[['SYM', 'QUARTER', 'HEDGE_FUND','date_13F', 'price_13F', 'investment_amount', 'shares_to_buy', 'shares_origin']].copy()
        # print(f'Before individual filtering...')
        # print(data[['SYM', 'QUARTER', 'date_13F', 'price_13F', 'investment_amount', 'shares_to_buy', 'shares_origin']])

        data['invest_adj'] = data['investment_amount']
        data['shares_adj'] = 0
        hedge_list = data['HEDGE_FUND'].unique()
        # 多的資金紀錄，internal表示分給該基金內其他個股；external表示分給該基金以外的其他基金個股
        extra_cost_dict = {
            'internal': [], # 因為要區隔每個基金，list裡面預計放{hedge名字: 多的分配金額}
            'external': 0
        }

        # 針對本季每個hedge，去分析幾個保留、幾個濾除，並記錄多的資金分配額度。
        # 針對本季每個hedge，去分析幾個保留、幾個濾除，並記錄多的資金分配額度。
        final_kept_data = pd.DataFrame()  # 儲存保留的records
        final_delete_data = pd.DataFrame()  # 儲存濾除的records

        for idx, each_hedge in enumerate(hedge_list):
            # print('The {} hedge: {} {}'.format(idx, each_hedge, data['QUARTER'].values[0]))
            sub_data = data[data['HEDGE_FUND'] == each_hedge]

            kept_data = sub_data[sub_data['price_13F'] < upper_price_limit]
            delete_data = sub_data[sub_data['price_13F'] >= upper_price_limit]

            if not delete_data.empty:  # 有個股被濾除
                extra_cost = delete_data['investment_amount'].sum()
                if kept_data.empty:
                    extra_cost_dict['external'] += extra_cost
                else:
                    extra_cost_dict['internal'].append({each_hedge: extra_cost})

            final_kept_data = pd.concat([final_kept_data, kept_data], ignore_index=True)
            final_delete_data = pd.concat([final_delete_data, delete_data], ignore_index=True)

        # 2. 開始資金分配 internal
        for item in extra_cost_dict['internal']:
            hedge_ = list(item.keys())[0]
            value_ = list(item.values())[0]
            # print('額外資金分配:{} {}'.format(hedge_, value_))
            
            # 將value平均分配給final_kept_data[final_kept_data['HEDGE_FUND'] == hedge_]的資料中
            mask = final_kept_data['HEDGE_FUND'] == hedge_
            count = mask.sum()
            if count > 0:
                final_kept_data.loc[mask, 'invest_adj'] += value_ / count

        # 3. 分配資金 external
        external_value = extra_cost_dict['external']
        if not final_kept_data.empty:
            count = len(final_kept_data)
            final_kept_data['invest_adj'] += external_value / count

        # 4. 用投資額'invest_adj'和價格'price_13F'計算應該投資的股數'shares_adj'
        final_kept_data['shares_adj'] = (final_kept_data['invest_adj'] / final_kept_data['price_13F'])

        final_kept_data['shares_adj'] = final_kept_data['shares_adj'].replace([np.inf, -np.inf], np.nan)
        final_kept_data['shares_adj'] = final_kept_data['shares_adj'].fillna(0).astype(int)
        final_kept_data['shares_adj'] = final_kept_data['shares_adj'].astype(int)

        # 查看結果
        # print('final_kept_data\n', final_kept_data)
        result_data = pd.concat([final_kept_data, final_delete_data], ignore_index=True)
        result_data = result_data[['SYM', 'QUARTER', 'HEDGE_FUND','date_13F', 'price_13F', 'invest_adj', 'shares_adj', 'shares_origin']]
        result_data = result_data.rename(columns={'invest_adj':'investment_amount', 'shares_adj':'shares_to_buy'})

        # print(f'After individual filtering...')
        # print(result_data)

        return result_data
    def arrange_customized_table(self, customized_table):
        '''
        FUNCTION:
            各基金可能有投資相同的公司股票，故此步驟是要合併這些records。
            以['QUARTER', 'SYM'] 進行GROUP BY，SUM UP ['shares_to_buy', 'shares_origin']  (區分兩種是因為shares_to_buy有可能是被調整過的)
        INPUT:
        OUTPUT:

        '''
        merge_shares_to_buy = customized_table.groupby(['QUARTER', 'SYM'], as_index=False)[['shares_to_buy', 'shares_origin']].sum()
        # 將 merge_shares_to_buy 與原始 DataFrame 合併，以保留 SYM、QUARTER、date、price 欄位
        merged_df = pd.merge(merge_shares_to_buy, customized_table[['SYM', 'QUARTER', 'date_13F', 'price_13F']], on=['SYM', 'QUARTER'], how='left')
        merged_df = merged_df.drop_duplicates()
        merged_df['suggested_invest_amount'] = merged_df['shares_to_buy'] * merged_df['price_13F']
        merged_df = merged_df.sort_values(by=['date_13F'], ascending=True)
        merged_df = merged_df.rename(columns={'date_13F': 'date', 'price_13F': 'price'})
        return merged_df
    def calculate_market_price_growth(self, table): #下一季的投資金額
        
        q_customized_table = table.copy()
        def calculate_next_q_date(row):
            year = int(row['QUARTER'].split()[1])
            if 'Q1' in row['QUARTER']:
                return datetime.datetime(year, 8, 14)
            elif 'Q2' in row['QUARTER']:
                return datetime.datetime(year, 11, 14) 
            elif 'Q3' in row['QUARTER']:
                return datetime.datetime(year+1, 2, 14) 
            elif 'Q4' in row['QUARTER']:
                return datetime.datetime(year+1, 5, 15) 
        # 新增欄位NEXT_QUARTER_DATE
        q_customized_table['NEXT_QUARTER_DATE'] = q_customized_table.apply(calculate_next_q_date, axis=1)
        current_sym_str = q_customized_table['SYM'].dropna().values
        if len(current_sym_str) == 1:
            current_sym_str = str(current_sym_str)
            current_sym_str = current_sym_str.replace('[', '(')
            current_sym_str = current_sym_str.replace(']', ')')
        else:
            current_sym_str = tuple(current_sym_str)

        holdings_time = q_customized_table['NEXT_QUARTER_DATE'][0]
        # print(f'季末結算市值日:{holdings_time}')

        holdings_time = self.adjust_holdings_time(holdings_time, self.us_sorted_dates, next_day=False)
        
        query = self.create_query_get_open_price_for_customized_fund(current_sym_str, holdings_time)
        price_data = self.sql_execute(query)
        price_data = pd.DataFrame(price_data)
        price_data = price_data.merge(q_customized_table, on=['SYM'], how='left')
        # print(price_data)
        market_value = sum(price_data['Open'] * price_data['shares_to_buy'])
        # print(f'季末結算市值日(調整):{holdings_time} {market_value}')
        return market_value
    def arragne_output_XIRR_excel_format(self, summary_table):
        summary_table = summary_table[summary_table['date'] == self.max_date]
        summary_table = summary_table.sort_values(by=['XIRR'], ascending=False)
        return summary_table   
    def modify_customized_fund_data_to_holdings_data_structures(self, hedge_fund, customized_fund_data):

        df = customized_fund_data
        df['ISSUER_NAME'] = '-'
        df['CL'] = '-'
        df['CUSIP'] = '-'
        df['PRINCIPAL'] = '-'
        df['OPTION_TYPE'] = '-'
        df['FORM_TYPE'] = '-'
        df['HEDGE_FUND'] = hedge_fund
        df['FILING_ID'] = '-'

        df.rename(columns={'suggested_invest_amount': 'VALUE'}, inplace=True)
        df['VALUE'] = df['VALUE'].astype(int)

        total_value_by_quarter = df.groupby('QUARTER')['VALUE'].transform('sum')
        df['Percentile'] = np.where(total_value_by_quarter == 0, 0, df['VALUE'] / total_value_by_quarter)

        df.rename(columns={'shares_to_buy': 'SHARES'}, inplace=True)

        df = df[['SYM', 'ISSUER_NAME', 'CL', 'CUSIP', 'VALUE', 'Percentile', 'SHARES', 'PRINCIPAL', 'OPTION_TYPE', 'HEDGE_FUND', 'QUARTER', 'FORM_TYPE', 'FILING_ID']]
        df = df.reset_index(drop=True)
        return df   
    def arrage_customized_fund_portfolio_data(self, hedge_fund, holdings_data):

        current_date = datetime.datetime.now().date()

        df = holdings_data.groupby('QUARTER').agg(
            HOLDINGS=('SYM', 'count'),
            VALUE=('VALUE', 'sum'),
            TOP_HOLDINGS=('SYM', lambda x: ', '.join(x[:3])),
        ).reset_index()

        df['FORM_TYPE'] = '-'

        def get_date(row):
            year = int(row['QUARTER'].split()[1])
            if 'Q1' in row['QUARTER']:
                return datetime.datetime(year, 5, 15)
            elif 'Q2' in row['QUARTER']:
                return datetime.datetime(year, 8, 14)
            elif 'Q3' in row['QUARTER']:
                return datetime.datetime(year, 11, 14)
            elif 'Q4' in row['QUARTER']:
                return datetime.datetime(year+1, 2, 14)
        df['DATE_FILED'] = df.apply(get_date, axis=1)
        # df['DATE_FILED'] = current_date
        df['FILING_ID'] = '-'
        df['HEDGE_FUND'] = hedge_fund
        df = df[['QUARTER', 'HOLDINGS', 'VALUE', 'TOP_HOLDINGS', 'FORM_TYPE', 'DATE_FILED', 'FILING_ID', 'HEDGE_FUND']]
        df = df.sort_values(by=['DATE_FILED'], ascending=True).reset_index(drop=True)
        return df   
    def insert_records_to_DB(self, table_name, data):
        
        if self.config_obj.LOCAL_FLAG:
            conn = pymssql.connect(host='localhost', user = 'myfirstjump', password='myfirstjump', database='US_DB')
        else:
            conn = pymssql.connect(host='localhost', user = 'stock_search', password='1qazZAQ!', database='STOCK_SKILL_DB')
        
        cursor = conn.cursor(as_dict=True)

        data_tuple = [tuple(row) for row in data.values]
        # print(hedge_tuple)
        if table_name == '[US_DB].[dbo].[HEDGE_FUND_PORTFOLIO]':

            cursor.executemany(
                """INSERT INTO [US_DB].[dbo].[HEDGE_FUND_PORTFOLIO]
                (
                [QUARTER]
                ,[HOLDINGS]
                ,[VALUE]
                ,[TOP_HOLDINGS]
                ,[FORM_TYPE]
                ,[DATE_FILED]
                ,[FILING_ID]
                ,[HEDGE_FUND]
                ) 
                VALUES (%s,%d,%d,%s,%s,%s,%s,%s)"""
                , data_tuple
            )
            conn.commit()
        
        elif table_name == '[US_DB].[dbo].[HOLDINGS_DATA]':
            
            cursor.executemany(
                    """INSERT INTO [US_DB].[dbo].[HOLDINGS_DATA]
                    (
                    [SYM]
                    ,[ISSUER_NAME]
                    ,[CL]
                    ,[CUSIP]
                    ,[VALUE]
                    ,[Percentile]
                    ,[SHARES]
                    ,[PRINCIPAL]
                    ,[OPTION_TYPE]
                    ,[HEDGE_FUND]
                    ,[QUARTER]
                    ,[FORM_TYPE]
                    ,[FILING_ID]
                    ) 
                    VALUES(%s,%s,%s,%s,%d,%d,%d,%s,%s,%s,%s,%s,%s)"""
                    , data_tuple
            )
            conn.commit()
        
        elif table_name == self.config_obj.customized_fund_portfolio_table:
            cursor.executemany(
                """INSERT INTO [US_DB].[dbo].[CUSTOMIZED_HEDGE_FUND_PORTFOLIO]
                (
                [QUARTER]
                ,[HOLDINGS]
                ,[VALUE]
                ,[TOP_HOLDINGS]
                ,[FORM_TYPE]
                ,[DATE_FILED]
                ,[FILING_ID]
                ,[HEDGE_FUND]
                ) 
                VALUES (%s,%d,%d,%s,%s,%s,%s,%s)"""
                , data_tuple
            )
            conn.commit()
        elif table_name == self.config_obj.customized_holdings_data_table:
            cursor.executemany(
                    """INSERT INTO [US_DB].[dbo].[CUSTOMIZED_HOLDINGS_DATA]
                    (
                    [SYM]
                    ,[ISSUER_NAME]
                    ,[CL]
                    ,[CUSIP]
                    ,[VALUE]
                    ,[Percentile]
                    ,[SHARES]
                    ,[PRINCIPAL]
                    ,[OPTION_TYPE]
                    ,[HEDGE_FUND]
                    ,[QUARTER]
                    ,[FORM_TYPE]
                    ,[FILING_ID]
                    ) 
                    VALUES(%s,%s,%s,%s,%d,%d,%d,%s,%s,%s,%s,%s,%s)"""
                    , data_tuple
            )
            conn.commit()
        elif table_name == self.config_obj.customized_individual_fund_portfolio_table:
            cursor.executemany(
                """INSERT INTO [US_DB].[dbo].[CUSTOMIZED_INDIVIDUAL_HEDGE_FUND_PORTFOLIO]
                (
                [QUARTER]
                ,[HOLDINGS]
                ,[VALUE]
                ,[TOP_HOLDINGS]
                ,[FORM_TYPE]
                ,[DATE_FILED]
                ,[FILING_ID]
                ,[HEDGE_FUND]
                ) 
                VALUES (%s,%d,%d,%s,%s,%s,%s,%s)"""
                , data_tuple
            )
            conn.commit()
        elif table_name == self.config_obj.customized_individual_holdings_data_table:
            cursor.executemany(
                    """INSERT INTO [US_DB].[dbo].[CUSTOMIZED_INDIVIDUAL_HOLDINGS_DATA]
                    (
                    [SYM]
                    ,[ISSUER_NAME]
                    ,[CL]
                    ,[CUSIP]
                    ,[VALUE]
                    ,[Percentile]
                    ,[SHARES]
                    ,[PRINCIPAL]
                    ,[OPTION_TYPE]
                    ,[HEDGE_FUND]
                    ,[QUARTER]
                    ,[FORM_TYPE]
                    ,[FILING_ID]
                    ) 
                    VALUES(%s,%s,%s,%s,%d,%d,%d,%s,%s,%s,%s,%s,%s)"""
                    , data_tuple
            )
            conn.commit()

        inserted_rows = cursor.rowcount
        cursor.close()
        conn.close()

        return table_name, inserted_rows   
    def get_all_hedge_funds_name(self, table_name):
        # 構建查詢語句
        query = '''
        SELECT DISTINCT [HEDGE_FUND] FROM {}
        '''.format(table_name)

        # 執行查詢並返回結果
        result  = self.sql_execute(query)
        hedge_funds = [row['HEDGE_FUND'] for row in result]
        return hedge_funds    
    def get_all_hedge_funds_larger_than_10_years_name(self, table_name):
        # 構建查詢語句
        query = '''
        SELECT HEDGE_FUND, COUNT(*) AS row_count
        FROM {}
        GROUP BY HEDGE_FUND
        HAVING COUNT(*) >= 40;
        '''.format(table_name)

        # 執行查詢並返回結果
        result  = self.sql_execute(query)
        hedge_funds = [row['HEDGE_FUND'] for row in result]
        return hedge_funds
    def split_list(self, lst, n):
        # 確定每個子列表的大小
        k, m = divmod(len(lst), n)
        # 使用列表生成器將原列表分割成 n 個子列表
        return [lst[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n)]    
    def funds_data_delete_from_table(self, table_name):
        query = '''
        DELETE FROM {}
        '''.format(table_name)

        rows_affected = self.sql_execute(query)
        self.config_obj.logger.warning(f"Deleted {rows_affected} rows from table {table_name}")
    def funds_data_copy_and_insert_into_table(self, original_table, filtered_table, target_list):
        # 將 target_list 轉換為 SQL 語句可接受的格式
        formatted_target_list = ', '.join("N'{}'".format(fund.replace("'", "''")) for fund in target_list)

        # 將formatted_target_list放入查詢語句中
        query = '''
        INSERT INTO {}
        SELECT *
        FROM {}
        WHERE [HEDGE_FUND] IN ({})
        '''.format(filtered_table, original_table, formatted_target_list)

        rows_affected = self.sql_execute(query)
        self.config_obj.logger.warning(f"Inserted {rows_affected} rows to table {filtered_table}")
    def get_each_fund_portfolio_data(self, table_name, fund_name):
        query = '''
        SELECT * FROM {}
        WHERE [HEDGE_FUND] = '{}'
        '''.format(table_name, fund_name)
        return query   
    def get_gics_by_sym(self, table_name, SYM):
        query = '''
        SELECT 
        [GICS_2_digit]
        FROM {}
        WHERE Ticker = '{}'
        '''.format(table_name, SYM)
        return query

class StrategySeasonal(object):

    def __init__(self):
        self.config_obj = Configuration()
    
    def monthly_seasonality_stats(self, source_table) -> pd.DataFrame:
        """
        針對 [SEASONAL_STAT_DB].[dbo].[MONTHLY_INFO] 結構的 DataFrame
        (包含 month, stock_id, monthly_return, max_drawdown 等欄位)，
        計算每檔股票在 1~12 月份的季節性統計量。
        
        回傳值:
            DataFrame，欄位包括:
                stock_id        : 股票代號
                MonthVal        : 月份(1~12)
                TotalYears      : 資料期間內，該股票在此月份出現的不同年份總數
                #Count           : 該月份在歷史資料中總筆數
                WinRate         : 勝率(報酬率>0 之次數 / 總次數)
                AvgReturn       : 平均報酬率
                StdDevReturn    : 報酬率標準差
                AvgMaxDrawdown  : 平均最大回撤
                MaxDrawdown     : 最大回撤
                AvgVolume       : 平均交易量
                AvgPct2Low      : 平均跌幅
                StdDevPct2Low   : 跌幅標準差
                AvgPct2High     : 平均漲幅
                StdDevPct2High  : 漲幅標準差
        """
        stats_df = None

        query_stock_ids = f"""
        SELECT DISTINCT stock_id, market
        FROM {source_table}
        """
        stock_info = self.sql_execute(query_stock_ids)
        
        for idx, row in enumerate(stock_info):
            s_id = row['stock_id']
            s_market = row['market']

            if idx % 250 == 0:
                self.config_obj.logger.warning(f"計算{s_id}-{s_market}月統計量({idx+1}/{len(stock_info)})。")

            query_stock_monthly_data = f"""
            SELECT *
            FROM {source_table}
            WHERE stock_id = '{s_id}'
            AND market = '{s_market}'
            """
            data = self.sql_execute(query_stock_monthly_data)
            if not data: ### 處理無資料的狀況
                continue
            df = pd.DataFrame(data)

            # STEP 1: 將 'month' 從 'YYYY-MM' 格式解析出年、月
            # 若 'month' 已是 datetime 型態，可改用 df['month'].dt.year, df['month'].dt.month
            df['Year'] = df['month'].astype(str).str[:4].astype(int)
            df['MonthVal'] = df['month'].astype(str).str[5:7].astype(int)

            # STEP 2: 以 stock_id + MonthVal 為群組，進行聚合計算
            each_stats_df  = df.groupby(['stock_id', 'market', 'MonthVal']).agg(
                TotalYears=('Year', 'nunique'),
                # Count=('monthly_return', 'count'),
                WinRate=('monthly_return', lambda x: (x > 0).sum() / len(x)),
                AvgReturn=('monthly_return', 'mean'),
                StdDevReturn=('monthly_return', 'std'),
                AvgMaxDrawdown=('max_drawdown', 'mean'),
                MaxDrawdown=('max_drawdown', 'min'),
                AvgVolume=('avg_volume', 'mean'),
                AvgPct2Low=('pct_to_low', 'mean'),
                StdDevPct2Low=('pct_to_low', 'std'),
                AvgPct2High=('pct_to_high', 'mean'),
                StdDevPct2High=('pct_to_high', 'std'),
            ).reset_index()
            
            if stats_df is None:
                stats_df = each_stats_df 
            else:
                stats_df = pd.concat([stats_df, each_stats_df ], ignore_index=True)
        stats_df = stats_df.rename({'stock_id': '股票代號', 'market':'市場', 'MonthVal': '月份', 'TotalYears':'歷時年數',
                                    'WinRate': '勝率', 'AvgReturn': '平均報酬率', 'StdDevReturn':'報酬率標準差',
                                      'AvgMaxDrawdown': '平均最大回撤', 'MaxDrawdown': '最大回撤', 
                                      'AvgVolume': '平均交易量', 'AvgPct2Low':'平均跌幅', 'AvgPct2High':'平均漲幅',
                                      'StdDevPct2Low':'跌幅標準差', 'StdDevPct2High':'漲幅標準差',}, axis='columns')
        
        path = os.path.join(self.config_obj.seasonal_summary, str(datetime.datetime.now()).split()[0] + '_seasonal_summary.xlsx')
        stats_df.to_excel(path, index=False)
        return stats_df
    
    def monthly_seasonaly_strategy_backtest(self, seasonal_filtered_df):


        """
        針對 seasonal_filtered_result 中勝率100%的標的進行策略回測。
        策略：在指定月份(月初)以 open 價買進，月末以 close 價賣出。
        本金默認 100000 (可自行設定)，忽略交易成本與匯率問題。
        """
        seasonal_filtered_df = seasonal_filtered_df.rename({'股票代號':'stock_id', '市場': 'market', '月份':'month'}, axis='columns')

        # ---------------------------------------------------------
        #  Step 2: 針對每支股票 + 月份 執行回測
        # ---------------------------------------------------------
        result_list = []  # 用來存放每次交易的結果

        for idx, row in seasonal_filtered_df.iterrows():
            stock_id = row['stock_id']
            market   = row['market']
            target_m = int(row['month'])  # 目標月份 (1~12)
            
            # -----------------------------------------------------
            #  Step 2-1: 取出此股票在全部年度的日K資料
            # -----------------------------------------------------
            if market == 'TW':
                sql_price = f"""
                    SELECT [date], [stock_id],
                        [open], [close]
                    FROM {self.config_obj.tw_stock_price_table}
                    WHERE stock_id = '{stock_id}'
                    ORDER BY [date] ASC
                """
            else:  # market == 'US'
                sql_price = f"""
                    SELECT [date], [stock_id],
                        [Open]  AS [open],
                        [Close] AS [close]
                    FROM {self.config_obj.us_stock_price_table}
                    WHERE stock_id = '{stock_id}'
                    ORDER BY [date] ASC
                """

            price_data = self.sql_execute(sql_price)
            price_df = pd.DataFrame(price_data)
            price_df['date'] = pd.to_datetime(price_df['date'])

            # -----------------------------------------------------
            #  Step 2-2: 根據日期做出「年」與「月」的欄位，方便過濾
            # -----------------------------------------------------
            price_df['year'] = price_df['date'].dt.year
            price_df['month'] = price_df['date'].dt.month

            # -----------------------------------------------------
            #  Step 2-3: 針對每個年度的 target_m(目標月份)做交易
            #    策略：該月份第一個交易日 用open買進
            #           該月份最後一個交易日 用close賣出
            # -----------------------------------------------------
            # 先篩出目標月份
            target_month_data = price_df[price_df['month'] == target_m].copy()
            
            # 取所有有交易的「年」
            all_years = sorted(target_month_data['year'].unique())
            
            # 初始本金(僅作範例)，可根據需要設定
            capital = 100000.0

            for y in all_years:
                yearly_data = target_month_data[target_month_data['year'] == y].copy()
                
                if len(yearly_data) == 0:
                    continue

                # 取得當月第一個交易日、最後一個交易日
                first_trade_day = yearly_data.iloc[0]
                last_trade_day  = yearly_data.iloc[-1]

                buy_date = first_trade_day['date']
                sell_date = last_trade_day['date']
                buy_price = first_trade_day['open']
                sell_price = last_trade_day['close']
                
                # 若價格有遺漏或0等，需要額外處理(此處略)
                if pd.isnull(buy_price) or pd.isnull(sell_price):
                    self.config_obj.logger.warning(f"stock_id {stock_id} 需檢視{y}年度{target_m}月份價格資料。")

                # 假設全倉(全部本金買進)
                # 計算買進股數 (忽略手續費、稅金、股數限制等)
                shares = capital // buy_price
                pnl = (sell_price - buy_price) * shares
                roi = pnl / capital  # 報酬率
                capital = capital + pnl
                
                result_list.append({
                    'stock_id': stock_id,
                    'market': market,
                    'year': y,
                    'month': target_m,
                    'buy_date': buy_date,
                    'sell_date': sell_date,
                    'buy_price': buy_price,
                    'sell_price': sell_price,
                    'shares': shares,
                    'pnl': pnl,
                    'capital': capital,
                    'roi': roi,
                })

        # ---------------------------------------------------------
        # Step 3: 將所有交易的結果整理成 DataFrame 
        # ---------------------------------------------------------
        result_df = pd.DataFrame(result_list)
        result_df = result_df.rename({'stock_id':'股票代號', 'market': '市場', 'year': '年份',
                    'month': '月份',
                    'buy_date': '買進日',
                    'sell_date': '賣出日',
                    'buy_price': '買進價格',
                    'sell_price': '賣出價格',
                    'shares': '股數',
                    'pnl': '獲利',
                    'capital': '累積淨值',
                    'roi': '年報酬率',}, axis='columns')
        
        # 進行加總或績效指標計算
        # 例如計算整體報酬率 / 平均年化報酬 / 等等
        # ---------------------------------------------------------
        path = os.path.join(self.config_obj.seasonal_summary, str(datetime.datetime.now()).split()[0] + '_seasonal_strategy1(一次買賣)_backtest.xlsx')
        result_df.to_excel(path, index=False)        
        self.config_obj.logger.warning(f"回測完成，輸出至Excel。")
        # ---------------------------------------------------------
        return result_df
  
    def monthly_seasonaly_strategy_adjusted_backtest(self, strategy_name, seasonal_filtered_df, 
        INITIAL_CAP_PERCENT,
        BUY_IN_RATE,     # 
        OUT_LV1_STD_RATE,# 
        OUT_LV2_STD_RATE, # 
        principal=100000,
        ):
        """

        """

        # 僅針對勝率=100%的標的進行回測
        filtered_df = seasonal_filtered_df[seasonal_filtered_df['勝率'] >= 0.9].copy()
        if filtered_df.empty:
            self.config_obj.logger.warning(f"沒有勝率=100%的標的，無法進行調整後策略回測。")
            return pd.DataFrame()

        all_records = []  # 用於儲存所有標的的回測紀錄
        cumulative_capital_map = {} # 用來記錄各股票的「累積資金」，不同股票互不影響

        for idx, row in filtered_df.iterrows():
            stock_id = row['股票代號']
            market = row['市場']
            trade_month = int(row['月份'])
            avg_drop = float(row['平均跌幅'])
            std_drop = float(row['跌幅標準差'])
            avg_up = float(row['平均漲幅'])
            std_up = float(row['漲幅標準差'])

            # 取得該股票日線資料
            if market == 'TW':
                sql_price = f"""
                    SELECT [date], [stock_id],
                        [open], 
                        [max] AS [high],
                        [min] AS [low],
                        [close]
                    FROM {self.config_obj.tw_stock_price_table}
                    WHERE stock_id = '{stock_id}'
                    ORDER BY [date] ASC
                """
            else:  # market == 'US'
                sql_price = f"""
                    SELECT [date], [stock_id],
                        [Open]  AS [open],
                        [High] as [high],
                        [Low] AS [low],
                        [Close] AS [close]
                    FROM {self.config_obj.us_stock_price_table}
                    WHERE stock_id = '{stock_id}'
                    ORDER BY [date] ASC
                """

            price_data = self.sql_execute(sql_price)
            df_daily = pd.DataFrame(price_data)
            df_daily = df_daily[
                (df_daily['open']  > 0) &
                (df_daily['high']  > 0) &
                (df_daily['low']   > 0) &
                (df_daily['close'] > 0)
            ].copy()

            if df_daily.empty:
                continue
            df_daily['date'] = pd.to_datetime(df_daily['date'])
            df_daily['year'] = df_daily['date'].dt.year
            df_daily['month'] = df_daily['date'].dt.month

            # 僅篩出目標月份資料
            df_month = df_daily[df_daily['month'] == trade_month].copy()
            if df_month.empty:
                continue

            # 以年份分組，每一年度該月份做一筆回測
            yearly_groups = df_month.groupby('year')

            stock_trade_records = []
            # 年份排序，確保按「由小到大」順序進行累積
            years_sorted = sorted(yearly_groups.groups.keys())

            for yr in years_sorted:
                grp = yearly_groups.get_group(yr).sort_values(by='date')
                if len(grp) < 2:
                    continue

                if stock_id not in cumulative_capital_map:
                    cumulative_capital_map[stock_id] = principal

                # 1) 月初 / 月末
                first_row = grp.iloc[0]
                last_row = grp.iloc[-1]
                open_price = first_row['open']    # 月初買進價
                close_price = last_row['close']   # 月末收盤價

                ### 進場指標 open_price × (1+(avg_drop+k×std_drop))   k --> BUY_IN_RATE 可以設定為0.3、0.5、0.7
                enter_index = 1 + avg_drop + BUY_IN_RATE * std_drop


                # 設定部位與資金
                capital = cumulative_capital_map[stock_id]
                remain_capital = capital  # 當前可用資金

                share_holding = 0.0         # 當前持有股數
                total_shares_bought = 0.0   # 用來紀錄"股數"欄位 (總買入股數)
                is_half_sold = False
                has_added_position = False
                sell_all = False
                early_sold = False

                # 分批進場：當 進場指標 enter_index < 1 即代表會採用兩段進場策略
                multiple_entry = (enter_index < 1)

                # 決定第一筆買入股數
                if multiple_entry:
                    invest_capital = capital * INITIAL_CAP_PERCENT ### 分批入場的比例，例如INITIAL_CAP_PERCENT=50%
                    share_holding = invest_capital / open_price
                    total_shares_bought = share_holding
                    remain_capital -= invest_capital
                else:
                    # 一次買 100%
                    share_holding = capital / open_price
                    total_shares_bought = share_holding
                    remain_capital = 0.0

                # 計算停利門檻 (以月初買價為基準)
                stop_profit_lv1 = open_price * (1 + avg_up + OUT_LV1_STD_RATE * std_up)
                stop_profit_lv2 = open_price * (1 + avg_up + OUT_LV2_STD_RATE * std_up)

                # 加碼門檻 (若 avg_drop < 0，即 multiple_entry = True)
                add_position_price = open_price * enter_index if multiple_entry else None

                # 紀錄最終賣出日與賣出價格
                final_sell_date = None
                final_sell_price = None
                
                # 低價加碼：是否真的執行加碼
                did_add = False

                # 逐日檢查停利 / 加碼
                for i in range(len(grp)):
                    current_date = grp.iloc[i]['date']
                    high_price = grp.iloc[i]['high']
                    low_price = grp.iloc[i]['low']
                    close_price = grp.iloc[i]['close']

                    # 若未全數賣出，先檢查加碼 (只加一次)
                    if multiple_entry and (not has_added_position) and (not sell_all):
                        if low_price <= add_position_price: # 看最低價是否 < 加碼價
                            # 加碼 50% 的資金
                            add_shares = remain_capital / add_position_price # 用加碼價去加碼
                            share_holding += add_shares
                            total_shares_bought += add_shares
                            remain_capital = 0.0
                            has_added_position = True
                            did_add = True

                    # 停利條件
                    if not sell_all:
                        # 2.1 如果價格 >= stop_profit_lv1，且尚未賣一半
                        if (high_price >= stop_profit_lv1) and (not is_half_sold):
                            half_shares = share_holding * 0.5 # 先獲利了結一半
                            gain = half_shares * stop_profit_lv1
                            remain_capital += gain
                            share_holding -= half_shares
                            is_half_sold = True

                        # 2.2 如果價格 >= stop_profit_lv2，直接全數賣出
                        if close_price >= stop_profit_lv2:
                            gain = share_holding * stop_profit_lv2
                            remain_capital += gain
                            share_holding = 0.0
                            early_sold = True
                            sell_all = True
                            final_sell_date = current_date
                            final_sell_price = close_price
                            break

                # 2.3 若到月底仍有持股，則以月底收盤價全部賣出
                if share_holding > 0.0 and (not sell_all):
                    gain = share_holding * close_price
                    remain_capital += gain
                    share_holding = 0.0
                    final_sell_date = last_row['date']
                    final_sell_price = close_price
                elif sell_all:
                    # 若中途已經賣光，final_sell_date/final_sell_price 可能已在上方設定
                    pass

                # 若本月完全沒觸發停利(也沒月底賣出)，理論上不會發生，除非沒有交易
                if final_sell_date is None:
                    final_sell_date = last_row['date']
                    final_sell_price = close_price

                # 最終資金、獲利、報酬率
                final_capital = remain_capital
                profit = final_capital - capital
                # 用「(最終 / 初始) - 1」當作"年報酬率"
                annual_return = profit / capital

                # 將本年度的 final_capital 作為「累積淨值」更新回字典
                cumulative_capital_map[stock_id] = final_capital

                stock_trade_records.append({
                    '股票代號': stock_id,
                    '市場': market,
                    '年份': yr,
                    '月份': trade_month,
                    '買進日': first_row['date'],
                    '賣出日': final_sell_date,
                    '買進價格': open_price,
                    '賣出價格': final_sell_price,
                    # 以「總買入股數」呈現，方便對照最初買入部位 / 加碼部位總和
                    '股數': round(total_shares_bought, 0),
                    '獲利': round(profit, 2),
                    '累積淨值': round(final_capital, 2),
                    '年報酬率': annual_return,
                    '分批進場': '是' if multiple_entry else '否',  
                    '低價加碼': '是' if did_add else '否',
                    '提早出場': '是' if is_half_sold else '否',
                    '提早完全出場': '是' if early_sold else '否',
                })

            # 結合該股票各年份的回測紀錄
            if stock_trade_records:
                results_df = pd.DataFrame(stock_trade_records)
                all_records.append(results_df)

        # 最終合併
        if all_records:
            final_result = pd.concat(all_records, ignore_index=True)
        else:
            final_result = pd.DataFrame()
        # ---------------------------------------------------------
        path = os.path.join(self.config_obj.seasonal_summary, str(datetime.datetime.now()).split()[0] + f'_seasonal_strategy_{strategy_name}({INITIAL_CAP_PERCENT}-{BUY_IN_RATE}-{OUT_LV1_STD_RATE}-{OUT_LV2_STD_RATE})_backtest.xlsx')
        final_result.to_excel(path, index=False)        
        self.config_obj.logger.warning(f"{strategy_name}回測完成，輸出至Excel。")
        # ---------------------------------------------------------
        return final_result     

    def seasonal_strategies_comparison_function(self, paths):
        """
        paths: List[str]
            傳入多個回測檔案(Excel)的路徑

        回傳一個比較各策略最終累積淨值以及年報酬率勝率的 DataFrame，
        並在 stock_id 後額外輸出對應的月份欄位。
        """

        # 用來存放所有 (stock_id, month) 與其對應的各策略資料
        # 結構: all_data[(stock_id, month)][strategy_name] = {
        #           'yearly_returns': {year: 年報酬率, ...},
        #           'final_net': 最後一年(最大年份)累積淨值
        #       }
        all_data = defaultdict(lambda: defaultdict(lambda: {
            'yearly_returns': {},
            'final_net': None
        }))

        # 1. 逐檔讀取並解析
        for path in paths:
            # 從檔名擷取策略名稱
            filename = os.path.basename(path)
            pattern = r'seasonal_strategy_(.*?)_backtest\.xlsx'
            match = re.search(pattern, filename)
            if match:
                strategy_name = match.group(1)
            else:
                strategy_name = filename  # 若檔名格式不符，直接用整個檔名

            # 讀取該回測檔案
            df = pd.read_excel(path)

            # 2. 以 groupby(['股票代號','月份']) 聚合
            #    代表同一檔股票、同一個月份視為一組
            for (stock_id, month_value), group in df.groupby(['股票代號', '月份']):

                # 確保結構存在
                if strategy_name not in all_data[(stock_id, month_value)]:
                    all_data[(stock_id, month_value)][strategy_name] = {
                        'yearly_returns': {},
                        'final_net': None
                    }

                # 找出「最後一年」(年份最大)那筆資料
                max_year_idx = group['年份'].idxmax()
                last_year_row = group.loc[max_year_idx]
                all_data[(stock_id, month_value)][strategy_name]['final_net'] = last_year_row['累積淨值']

                # 紀錄每一年的年報酬率
                for _, row in group.iterrows():
                    year = row['年份']
                    all_data[(stock_id, month_value)][strategy_name]['yearly_returns'][year] = row['年報酬率']

        # 3. 開始統計所有 (stock_id, month) 之下，各策略的「年報酬率勝率」與「最終累積淨值」
        #   - 先整理所有 (stock_id, month) 
        #   - 以及所有可能出現過的策略
        all_keys = sorted(all_data.keys())  # (stock_id, month) 組合
        all_strategies = set()
        for key in all_keys:
            for s in all_data[key].keys():
                all_strategies.add(s)
        all_strategies = sorted(all_strategies)

        result_rows = []

        for (stock_id, month_value) in all_keys:
            # 該組合對應的所有策略
            strategy_dict = all_data[(stock_id, month_value)]  # { strategy_name: {...}, ... }
            stock_strategies = list(strategy_dict.keys())

            # 先抓各策略的「最終累積淨值」
            final_nets = {
                s: strategy_dict[s]['final_net']
                for s in stock_strategies
            }

            # 找出該 (stock_id, month) 所有出現過的年份(合併各策略)
            all_years = set()
            for s in stock_strategies:
                all_years.update(strategy_dict[s]['yearly_returns'].keys())

            # wins[s] = 該策略在該 (stock_id, month) 的多少年份勝出
            wins = {s: 0 for s in stock_strategies}

            # 逐年找出最高報酬策略
            for y in all_years:
                best_strat = None
                best_ret = None
                for s in stock_strategies:
                    ret = strategy_dict[s]['yearly_returns'].get(y, None)
                    if ret is not None:
                        if (best_ret is None) or (ret > best_ret):
                            best_ret = ret
                            best_strat = s
                if best_strat is not None:
                    wins[best_strat] += 1

            total_years = len(all_years)
            win_rates = {
                s: (wins[s] / total_years) if total_years > 0 else 0
                for s in stock_strategies
            }

            # 建構一列結果
            # 欄位: [stock_id, month, ...各策略累積淨值..., ...各策略勝率...]
            row_data = [stock_id, month_value]

            # 依全域(all_strategies)排序填入 final_net
            for s in all_strategies:
                if s in final_nets:
                    row_data.append(final_nets[s])
                else:
                    row_data.append(None)

            # 依全域(all_strategies)排序填入勝率
            for s in all_strategies:
                if s in win_rates:
                    row_data.append(win_rates[s])
                else:
                    row_data.append(None)

            result_rows.append(row_data)

        # 4. 建構欄位名稱
        # columns = ['stock_id', 'month', {策略1累積淨}, {策略2累積淨}, ..., {策略1年報酬勝率}, {策略2年報酬勝率}, ...]
        columns = ['stock_id', 'month']
        for s in all_strategies:
            columns.append(f'{s}累積淨')
        for s in all_strategies:
            columns.append(f'{s}年報酬勝率')

        # 5. 產生 DataFrame
        result_df = pd.DataFrame(result_rows, columns=columns)

        # ---------------------------------------------------------
        path = os.path.join(self.config_obj.seasonal_summary, str(datetime.datetime.now()).split()[0] + f'strategies_backtest_comparison.xlsx')
        result_df.to_excel(path, index=False)        
        self.config_obj.logger.warning(f"完成比較，輸出至Excel。")
        # ---------------------------------------------------------

        return result_df
    def _get_daily_price_df(self, stock_id, market):
        """
        讀取該股票全部日線資料，若無或價格有問題，回傳空DF。
        """
        if market == 'TW':
            sql_price = f"""
                SELECT [date], [stock_id],
                       [open],
                       [max]  AS [high],
                       [min]  AS [low],
                       [close]
                FROM {self.config_obj.tw_stock_price_table}
                WHERE stock_id = '{stock_id}'
                ORDER BY [date] ASC
            """
        else:  # market == 'US'
            sql_price = f"""
                SELECT [date], [stock_id],
                       [Open]  AS [open],
                       [High]  AS [high],
                       [Low]   AS [low],
                       [Close] AS [close]
                FROM {self.config_obj.us_stock_price_table}
                WHERE stock_id = '{stock_id}'
                ORDER BY [date] ASC
            """
        price_data = self.sql_execute(sql_price)
        if not price_data:
            return pd.DataFrame()

        df = pd.DataFrame(price_data)
        df['date'] = pd.to_datetime(df['date'])
        # 去除價格<=0的不合理數據
        df = df[(df['open']>0) & (df['high']>0) & (df['low']>0) & (df['close']>0)].copy()
        return df

    def _record_trade(self, trades_list, stock_id, date, price, direction, trade_shares):
        """
        僅保留指定的五個欄位。
        """
        trades_list.append({
            'stock_id': stock_id,
            'date': date,
            'price': price,
            'buy/sell': direction,
            'shares': trade_shares
        })

    def monthly_seasonaly_strategy_each_transaction_record(self,
                                                           seasonal_filtered_df,
                                                           strategies_dict,
                                                           principal=100000,
                                                           start_year=2013,
                                                           start_month=1,
                                                           end_year=2025,
                                                           end_month=2):
        """
        單一投組，逐月分配資金給符合月份的標的，再逐日執行買/賣。
        僅輸出 5 欄位：stock_id, date, price, buy/sell, shares

        說明：
          - 已排除台股，僅保留美股 => 不再區分 market，不需匯率計算。
          - DB 查詢只對 us_stock_price_table 做一次性查詢 (stock_id in (...) and date between ...).
        """

        cash_balance = float(principal)
        self.config_obj.logger.warning(f"現金異動 {cash_balance}")
        positions = {}         # stock_id -> shares
        trades_list = []       # 最終輸出結果
        stock_budget = {}      # 子資金池: stock_id -> leftover
        self.positions_extra = {}  # 紀錄停利/加碼等資訊

        def next_month(y, m):
            return (y+1,1) if m==12 else (y,m+1)

        prev_year = None
        y, m = start_year, start_month

        while True:
            if (prev_year is None) or (y != prev_year):
                self.config_obj.logger.warning(f"=== Starting year {y} ===")
                prev_year = y

            # 重置 positions_extra
            self.positions_extra = {}
            # 1) 找出當月標的 (已無TW，僅US)
            df_this_month = seasonal_filtered_df[seasonal_filtered_df['月份'] == m].copy()
            if not df_this_month.empty:
                # 收集本月所有 US 的 stock_id
                stock_list = df_this_month['股票代號'].unique().tolist()
                n_stocks = len(stock_list)
                

                if n_stocks > 0:
                    # 2) 先以月份計算日期範圍
                    days_in_month = calendar.monthrange(y, m)[1]
                    start_date = f"{y}-{m:02d}-01"
                    end_date   = f"{y}-{m:02d}-{days_in_month:02d}"

                    # 3) 一次抓取該月所有 stock_id 的美股日線資料
                    in_clause = ','.join(f"'{sid}'" for sid in stock_list)
                    query = f"""
                        SELECT [date],[stock_id],
                               [Open]  AS [open],
                               [High]  AS [high],
                               [Low]   AS [low],
                               [Close] AS [close]
                        FROM {self.config_obj.us_stock_price_table}
                        WHERE stock_id IN ({in_clause})
                          AND [date] >= '{start_date}'
                          AND [date] <= '{end_date}'
                        ORDER BY [date]
                    """
                    data_us = self.sql_execute(query)
                    if data_us:
                        df_us = pd.DataFrame(data_us)
                        df_us['date'] = pd.to_datetime(df_us['date'])
                        # 去除不合理行情
                        df_us = df_us[
                            (df_us['open']>0)&
                            (df_us['high']>0)&
                            (df_us['low']>0)&
                            (df_us['close']>0)
                        ].copy()
                    else:
                        df_us = pd.DataFrame()

                    # 4) 再次篩選有資料的 valid_stocks
                    valid_stocks = []
                    if not df_us.empty:
                        for stk in stock_list:
                            sub = df_us[df_us['stock_id'] == stk]
                            if not sub.empty:
                                valid_stocks.append(stk)
                    # self.config_obj.logger.warning(f"{y}年 {m}月 共{len(valid_stocks)}個標的有交易 ===")
                    if len(valid_stocks)>0:
                        # (A) 均分資金 => stock_budget
                        alloc_per_stock = cash_balance / len(valid_stocks)
                        total_alloc = alloc_per_stock * len(valid_stocks)
                        cash_balance -= total_alloc
                        # self.config_obj.logger.warning(f"現金異動 {cash_balance}")
                        for sid in valid_stocks:
                            stock_budget[sid] = alloc_per_stock
                        # self.config_obj.logger.warning(f"stock_budget {stock_budget}")
                        # (B) 把 seasonal_filtered_df 的欄位帶進 df_us
                        #     (如 avg_drop, std_drop, avg_up, std_up, 建議策略)
                        #     之後可於每筆資料 row2 讀取
                        df_month_small = df_this_month[['股票代號','平均跌幅','跌幅標準差','平均漲幅','漲幅標準差','建議策略']]
                        df_month_small.columns = ['stock_id','avg_drop','std_drop','avg_up','std_up','strategy_name']
                        df_merge = pd.merge(
                            df_us, 
                            df_month_small, 
                            on='stock_id',
                            how='inner'
                        )
                        df_merge.sort_values(by=['date','stock_id'], inplace=True)

                        unique_days = df_merge['date'].unique()
                        for day in unique_days:
                            df_day = df_merge[df_merge['date']==day]
                            for i2, row2 in df_day.iterrows():
                                stock_id = row2['stock_id']
                                open_p   = row2['open']
                                high_p   = row2['high']
                                low_p    = row2['low']
                                close_p  = row2['close']
                                avg_drop = row2['avg_drop']
                                std_drop = row2['std_drop']
                                avg_up   = row2['avg_up']
                                std_up   = row2['std_up']
                                strategy_name = row2['strategy_name']

                                if strategy_name not in strategies_dict:
                                    continue
                                INIT_CAP_PERCENT, BUY_IN_RATE, OUT_LV1_STD_RATE, OUT_LV2_STD_RATE = strategies_dict[strategy_name]

                                enter_index = 1 + avg_drop + BUY_IN_RATE*std_drop
                                multiple_entry = (enter_index < 1)

                                if stock_id not in self.positions_extra:
                                    self.positions_extra[stock_id] = {
                                        'entry_price': None,
                                        'stop_lv1': None,
                                        'stop_lv2': None,
                                        'add_price': None,
                                        'has_added': False,
                                        'is_half_sold': False,
                                        'exited': False
                                    }
                                pos_info = self.positions_extra[stock_id]
                                shares_holding = positions.get(stock_id, 0)

                                # ============= (A) 首次買進 =============
                                if pos_info['entry_price'] is None and (stock_budget.get(stock_id,0)>0) and (open_p>0):
                                    if multiple_entry:
                                        first_invest = stock_budget[stock_id] * INIT_CAP_PERCENT
                                    else:
                                        first_invest = stock_budget[stock_id]

                                    if first_invest>0 and open_p>0:
                                        buy_shares = first_invest / open_p
                                        positions[stock_id] = shares_holding + buy_shares
                                        stock_budget[stock_id] -= first_invest

                                        # 停利門檻
                                        pos_info['entry_price'] = open_p
                                        pos_info['stop_lv1'] = open_p*(1+avg_up+OUT_LV1_STD_RATE*std_up)
                                        pos_info['stop_lv2'] = open_p*(1+avg_up+OUT_LV2_STD_RATE*std_up)
                                        pos_info['add_price'] = open_p*enter_index if multiple_entry else None
                                        pos_info['has_added'] = False
                                        pos_info['is_half_sold'] = False
                                        pos_info['exited'] = False

                                        # self.config_obj.logger.warning(f"pos_info {pos_info}")
                                        self._record_trade(trades_list, stock_id, day, open_p, 'BUY', buy_shares)

                                # ============= (B) 加碼(只一次) =============
                                if multiple_entry and (not pos_info['has_added']) and (not pos_info['exited']):
                                    if pos_info['add_price'] and pos_info['add_price']>0:
                                        if low_p <= pos_info['add_price'] and stock_budget.get(stock_id,0)>0:
                                            leftover = stock_budget[stock_id]
                                            add_shares = leftover / pos_info['add_price']
                                            positions[stock_id] += add_shares
                                            stock_budget[stock_id] = 0
                                            pos_info['has_added'] = True
                                            

                                            self._record_trade(trades_list, stock_id, day, pos_info['add_price'], 'BUY', add_shares)
                                            # self.config_obj.logger.warning(f"BUY                現金異動 {cash_balance}")

                                # ============= (C) 停利判斷 =============
                                if shares_holding>0 and (not pos_info['exited']):
                                    # 2.1 賣一半
                                    if pos_info['stop_lv1'] and (not pos_info['is_half_sold']):
                                        if high_p >= pos_info['stop_lv1']:
                                            half_shares = positions[stock_id]*0.5
                                            positions[stock_id] -= half_shares
                                            gain = half_shares * pos_info['stop_lv1']
                                            cash_balance += gain
                                            
                                            pos_info['is_half_sold']=True

                                            self._record_trade(trades_list, stock_id, day, pos_info['stop_lv1'], 'SELL', -half_shares)
                                            # self.config_obj.logger.warning(f"SELL停利Lv1        現金異動 {cash_balance}")

                                    # 2.2 全出
                                    if pos_info['stop_lv2'] and positions[stock_id]>0:
                                        if close_p >= pos_info['stop_lv2']:
                                            final_sh = positions[stock_id]
                                            positions[stock_id] = 0
                                            gain = final_sh * pos_info['stop_lv2']
                                            cash_balance += gain
                                            pos_info['exited'] = True

                                            self._record_trade(trades_list, stock_id, day, pos_info['stop_lv2'], 'SELL', -final_sh)
                                            # self.config_obj.logger.warning(f"SELL停利Lv2        現金異動 {cash_balance}")

                        # ============= (D) 月末強制平倉 =============
                        if not df_merge.empty:
                            last_day = df_merge['date'].max()
                            df_last = df_merge[df_merge['date'] == last_day]
                            for i3, row3 in df_last.iterrows():
                                st_id = row3['stock_id']
                                c_p   = row3['close']
                                if positions.get(st_id,0)>0:
                                    sh = positions[st_id]
                                    positions[st_id]=0
                                    gain = sh*c_p
                                    cash_balance+=gain
                                    # self.config_obj.logger.warning(f"SELL ALL           現金異動 {cash_balance}")

                                    self._record_trade(trades_list, st_id, last_day, c_p, 'SELL', -sh)
                                    if st_id in self.positions_extra:
                                        self.positions_extra[st_id]['exited']=True

                        # (E) 月底退回未用到的子資金
                        for stk in valid_stocks:
                            leftover = stock_budget.get(stk,0)
                            if leftover>0:
                                cash_balance+=leftover
                                # self.config_obj.logger.warning(f"現金異動 {cash_balance}")
                            stock_budget[stk]=0
            
            # self.config_obj.logger.warning(f"{y}-{m}")
            # self.config_obj.logger.warning(f"月末CASH:      {cash_balance}")

            if (y==end_year) and (m==end_month):
                break
            y,m=next_month(y,m)

        trades_df = pd.DataFrame(trades_list, columns=['stock_id','date','price','buy/sell','shares'])

        # ---------------------------------------------------------
        path = os.path.join(self.config_obj.seasonal_summary, str(datetime.datetime.now()).split()[0] + '_季節性策略回測_逐筆交易紀錄.xlsx')
        trades_df.to_excel(path, index=False)        
        self.config_obj.logger.warning(f"回測完成，輸逐筆交易紀錄出至Excel。")
        # ---------------------------------------------------------

        return trades_df

    def sql_execute(self, query):

        if self.config_obj.LOCAL_FLAG:
            conn = pymssql.connect(host='localhost', user = 'myfirstjump', password='myfirstjump', database='US_DB')
        else:
            conn = pymssql.connect(host='localhost', user = 'stock_search', password='1qazZAQ!', database='STOCK_SKILL_DB')
        cursor = conn.cursor(as_dict=True)
        cursor.execute(query)
        
        # 判斷是否是DML操作
        if query.strip().upper().startswith("DELETE") or query.strip().upper().startswith("INSERT") or query.strip().upper().startswith("UPDATE"):
            rows_affected = cursor.rowcount
            conn.commit()  # 確保DML操作被提交
            cursor.close()
            conn.close()
            return rows_affected
        else:
            data = []
            for row in cursor:
                data.append(row)
            cursor.close()
            conn.close()
        return data

class StrategyPerformance(object):

    def __init__(self):
        self.config_obj = Configuration()


    def get_all_price_date(self, price_table):

        query = '''
        SELECT DISTINCT [date]
        FROM {}
        '''.format(price_table)
        return query
    def generate_types_of_performance_output(self, strategy_name, df_trades, us_price_table, initial_capital):

        '''
        輸入逐日交易紀錄，並輸出所需的績效圖表。
        '''
        df_trades['date'] = pd.to_datetime(df_trades['date'])
        df_trades.sort_values("date", inplace=True)

        # 找出所有交易日期的範圍
        all_symbols = df_trades['stock_id'].unique().tolist()
        symbol_list_str = ",".join(f"'{s}'" for s in all_symbols)
        start_date = df_trades['date'].min()
        end_date   = df_trades['date'].max()
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str   = end_date.strftime("%Y-%m-%d")

        query = f"""
        SELECT [date], [stock_id], [Close] AS [close]
        FROM {us_price_table}
        WHERE [date] >= '{start_date_str}'
          AND [date] <= '{end_date_str}'
          AND [stock_id] IN ({symbol_list_str})
        ORDER BY [date], [stock_id];
        """
        data_all = self.sql_execute(query)
        df_price = pd.DataFrame(data_all)  # columns: date, stock_id, close
        df_price['date'] = pd.to_datetime(df_price['date'])
        # 為了查詢方便，可以做 pivot 或 MultiIndex
        # pivot: row=日期, col=stock_id, value=收盤價
        df_price_pivot = df_price.pivot(index='date', columns='stock_id', values='close')
        df_price_pivot[df_price_pivot <= 0] = np.nan
        df_price_pivot.ffill(inplace=True) # 使用forward fill補值，每個股票的缺失值都會被自動填成該股票之前「最近一次」的有效收盤價

        # self.config_obj.logger.warning(f"DF PRICE")
        # print(df_price)
        # self.config_obj.logger.warning(f"DF PRICE PIVOT")
        # print(df_price_pivot)

        #-----------------------------------
        # 進行每日 NAV 計算
        #-----------------------------------
        current_cash = initial_capital
        # 股票持倉 dict (key = stock_id, value = 持股股數)
        positions = {}
        # 收集每日資產淨值資料
        records = []  # list of (date, portfolio_value)

        # 產生從 start_date 到 end_date 的所有曆日 (亦可只針對交易所開市日)

        if strategy_name == '動能策略':
        
            query = self.get_all_price_date(self.config_obj.us_stock_price_table_IBAPI) # 為了取得時間欄位
        else:
            query = self.get_all_price_date(self.config_obj.us_stock_price_table) # 為了取得時間欄位
        all_date_list = self.sql_execute(query)
        all_date_list = pd.DataFrame(all_date_list)['date'].values
        us_sorted_dates = sorted(all_date_list)
        self.us_sorted_dates = pd.to_datetime(us_sorted_dates)
        
        date_range = self.us_sorted_dates   # pd.date_range(start_date, end_date, freq='D') ### 應該使用開市日，不宜使用全部日期。 

        if strategy_name == '動能策略':
            date_range = date_range[date_range >= '2016-12-29']
            date_range = date_range[date_range < '2024-11-20']   
        elif strategy_name == '季節性策略':
            date_range = date_range[date_range < '2025-03-01']  

        trade_idx = 0  # df_trades 的 row index
        n_trades = len(df_trades)

        for current_date in date_range:
            # 先處理當天所有交易 (可能同一天多筆)
            while trade_idx < n_trades and df_trades.iloc[trade_idx]['date'].date() == current_date.date():

                if trade_idx % 200 == 0:
                    self.config_obj.logger.warning(f"進度...{current_date}")
                row = df_trades.iloc[trade_idx]
                stock_id = row['stock_id']
                trade_price = row['price']
                shares = row['shares']
                side = row['buy/sell'].upper()

                # BUY -> 扣現金，增加持股
                # SELL -> 增現金，減少持股
                cost = trade_price * shares
                
                if side == "BUY":
                    current_cash -= cost
                    positions[stock_id] = positions.get(stock_id, 0.0) + shares
                else:  # SELL
                    current_cash += abs(cost)  # shares 可能是負號，故取 abs
                    positions[stock_id] = positions.get(stock_id, 0.0) - abs(shares)
                    
                    # 若賣到持股歸零，可刪除該 stock_id
                    if abs(positions[stock_id]) < 1e-8:
                        del positions[stock_id]
                
                trade_idx += 1

            # 計算今日 portfolio value
            # = 剩餘現金 + sum(持股股數 * 當天收盤價)
            portfolio_value = current_cash

            try:
                price_series = df_price_pivot.loc[current_date]
            except KeyError:
                price_series = pd.Series(dtype='float64')  # 如果當天沒資料
            
            for sid, qty in positions.items():
                # 取得 sid 在 current_date 的收盤價
                c_price = price_series.get(sid, np.nan)
                if pd.isna(c_price):
                    # 若是 NaN, 可能要改用前一日收盤價(若要 forward fill)
                    # 這裡簡單視為 0 or 跳過
                    c_price = 0.0
                portfolio_value += qty * c_price
            
            records.append((current_date, portfolio_value))
        #--------------------------------------------------------------
        # 4. 將每日 NAV 整理為 DataFrame，並計算年度、季度表
        #--------------------------------------------------------------
        df_daily_nav = pd.DataFrame(records, columns=["date", "nav"])

        # 幫助後續計算報酬率
        df_daily_nav['year'] = df_daily_nav['date'].dt.year
        df_daily_nav['quarter'] = df_daily_nav['date'].dt.quarter

        #（1）年度初始/結束資金
        #    groupby 年份後，取當年第一天與最後一天 nav
        grouped_year = df_daily_nav.groupby('year')
        yearly_data = []
        for y, grp in grouped_year:
            grp = grp.sort_values('date')
            year_start_nav = grp.iloc[0]['nav']
            year_end_nav   = grp.iloc[-1]['nav']
            
            #（2）計算各季報酬(Q1, Q2, Q3, Q4)
            q_returns = {}

            if strategy_name =='季節性策略':
                for q in [1, 2, 3, 4]:
                    grp_q = grp[grp['quarter']==q]
                    if len(grp_q) > 0:
                        q_start = year_start_nav  if q == 1 else grp[grp['quarter']<q].iloc[-1]['nav']
                        q_end   = grp_q.iloc[-1]['nav']
                        q_return_pct = (q_end - q_start) / q_start
                        q_returns[q] = q_return_pct
                    else:
                        q_returns[q] = np.nan  # 該季沒交易日可留空
            elif strategy_name =='動能策略':

                for q in [1, 2, 3, 4]:
                    grp_q = grp[grp['quarter'] == q]
                    if not grp_q.empty:
                        # 若 q == 1，則直接用 year_start_nav；否則檢查上一季資料是否為空
                        if q == 1:
                            q_start = year_start_nav
                        else:
                            df_sub = grp[grp['quarter'] < q]
                            if df_sub.empty:
                                q_start = year_start_nav
                            else:
                                q_start = df_sub.iloc[-1]['nav']

                        q_end = grp_q.iloc[-1]['nav']
                        q_return_pct = (q_end - q_start) / q_start
                        q_returns[q] = q_return_pct
                    else:
                        q_returns[q] = np.nan  # 該季若完全沒交易日，就以 NaN 表示

            #（3）年度報酬率(不嚴格年化，只是該年度漲幅)
            apr = (year_end_nav - year_start_nav) / year_start_nav

            yearly_data.append({
                "year": y,
                "start_capital": year_start_nav,
                "end_capital": year_end_nav,
                "Q1": q_returns[1],
                "Q2": q_returns[2],
                "Q3": q_returns[3],
                "Q4": q_returns[4],
                "APR": apr
            })

        df_yearly = pd.DataFrame(yearly_data)

        # ---------------------------------------------------------
        path = os.path.join(self.config_obj.seasonal_summary, str(datetime.datetime.now()).split()[0] + f'_{strategy_name}回測_Daily_NAV.xlsx')
        df_daily_nav.to_excel(path, index=False)  

        path = os.path.join(self.config_obj.seasonal_summary, str(datetime.datetime.now()).split()[0] + f'_{strategy_name}回測_資產成長績效表.xlsx')
        df_yearly.to_excel(path, index=False)        
        # ---------------------------------------------------------
        if strategy_name == '季節性策略':
            title_ = 'Assets Growth Trend (SEASONAL)'
        elif strategy_name == '動能策略':
            title_ = 'Assets Growth Trend (MOMENTUM)'
        else:
            title_ = 'Assets Growth Trend (Cash + Position)'
        plt.figure(figsize=(10,6))
        plt.plot(df_daily_nav['date'], df_daily_nav['nav'], label='Portfolio NAV', linewidth=0.5)
        plt.title(title_)
        plt.xlabel("Date")
        plt.ylabel("Portfolio Value ($)")
        plt.legend()
        plt.grid(True)
        plt.show()

        self.config_obj.logger.warning(f"回測完成，輸逐筆交易紀錄出至Excel。")


    def get_close_price(self, stock_id, date_str):
        """
        根據 stock_id, date_str 從資料庫或快取的 df_us 查詢該日收盤價
        若該日無交易(休市)或價格 NaN，可自行決定:
        - 用前一交易日價格填補 (forward fill) 
        - 或直接返回 None 做進一步處理
        """
        query = f"""
        SELECT [date],[stock_id],[Close] AS [close]
        FROM {self.config_obj.us_stock_price_table}
        WHERE stock_id='{stock_id}' AND [date]='{date_str}'
        """
        # 假設這裡使用某函式 sql_execute(query) 回傳 list of dict
        data_us = self.sql_execute(query)
        
        if len(data_us) == 0:
            # 沒查到，視情況做 forward fill 或回傳 None
            return None
        
        return data_us[0]['close']

    def sql_execute(self, query):

        if self.config_obj.LOCAL_FLAG:
            conn = pymssql.connect(host='localhost', user = 'myfirstjump', password='myfirstjump', database='US_DB')
        else:
            conn = pymssql.connect(host='localhost', user = 'stock_search', password='1qazZAQ!', database='STOCK_SKILL_DB')
        cursor = conn.cursor(as_dict=True)
        cursor.execute(query)
        
        # 判斷是否是DML操作
        if query.strip().upper().startswith("DELETE") or query.strip().upper().startswith("INSERT") or query.strip().upper().startswith("UPDATE"):
            rows_affected = cursor.rowcount
            conn.commit()  # 確保DML操作被提交
            cursor.close()
            conn.close()
            return rows_affected
        else:
            data = []
            for row in cursor:
                data.append(row)
            cursor.close()
            conn.close()
        return data
