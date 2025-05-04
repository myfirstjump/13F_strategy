from py_module.config import Configuration
from py_module.crawler import Crawler, IBApp
from py_module.strategies import Strategy13F, StrategySeasonal, StrategyPerformance
# from dashboard import DashBuilder
from py_module.database_CRUD import DatabaseManipulation

import os
import pandas as pd
import logging
import time
import datetime

class StockStrategies(object):

    def __init__(self):
        self.config_obj = Configuration()
        self.crawler_obj = Crawler()
        self.stock_crawler_obj = IBApp()
        self.strategy_obj = Strategy13F()
        self.seasonal_strategy_obj = StrategySeasonal()
        self.performance = StrategyPerformance()
        self.db_obj = DatabaseManipulation()

    def data_crawl(self):
        self.crawler_obj.web_crawler_13F()
        # self.crawler_obj.web_crawler_13F_one_thread()
        # self.stock_crawler_obj.stock_data_crawler()

    def data_update(self):
        # self.db_obj.Update_GICs_to_DB()

        source_table = self.config_obj.us_stock_price_table
        target_table = self.config_obj.monthly_info
        self.db_obj.generate_monthly_stock_info(source_table=source_table, target_table=target_table, before_month='2025-05-01')

        # source_table = self.config_obj.tw_stock_price_table
        # target_table = self.config_obj.monthly_info
        # self.db_obj.generate_monthly_stock_info(source_table=source_table, target_table=target_table)

    def strategy_13F_investing(self):
        # self.strategy_obj.customize_fund_components(industry_top_selection=3, company_top_selection=3)
        # self.strategy_obj.back_test_flow()
        self.strategy_obj.customized_hedge_build_and_store()
        # self.strategy_obj.customized_hedge_build_and_store_multi_threading()
        # self.strategy_obj.calculate_preferred_index_from_hedge_and_output()

        # self.strategy_obj.customize_fund_components_revised(
        #                         reinvest_flag=True,
        #                         share_profit_flag=True,
        #                         hedge_funds_range=[
        #                             'Yacktman Asset Management',
        #                             'Scion Asset Management',
        #                             'Altarock Partners',
        #                             'Brave Warrior Advisors',
        #                             'Pershing Square Capital Management',
        #                             'Stilwell Value',
        #                         ], 
        #                         industry_top_selection=2, 
        #                         company_top_selection=2, 
        #                         mcap_weighted_flag=True)

        # self.strategy_obj.customize_fund_components_revised(
        #     reinvest_flag=True,
        #     share_profit_flag=False,
        #     hedge_funds_range=[
        #         'Cincinnati Specialty Underwriters Insurance CO'
        #     ],
        #     industry_top_selection=1,
        #     company_top_selection=3,
        #     mcap_weighted_flag=True,
        # )
    
    def strategy_seasonal_investing(self, ini_cap, strategy_exit):
        ### 製作seasonal_summary資料
        # target_table = self.config_obj.monthly_info
        # self.seasonal_strategy_obj.monthly_seasonality_stats(target_table)

        ### 根據統計資料，進行標的自動篩選
        # path = os.path.join(self.config_obj.seasonal_summary, '2025-04-06_seasonal_summary(2013-2022).xlsx')
        # seasonal_summary_df = pd.read_excel(path)
        # self.seasonal_strategy_obj.monthly_seasonal_summary_filtering(seasonal_summary_df)

        ### 透過seasonal_summary(filtered)資料，進行回測，觀察哪種進出策略比較優異。
        # path = os.path.join(self.config_obj.seasonal_summary, '2025-04-07_seasonal_summary(2013-2022_filtered).xlsx')
        # seasonal_filtered_df = pd.read_excel(path)
        # final_result = self.seasonal_strategy_obj.monthly_seasonaly_strategy_backtest(seasonal_filtered_df) ### 較不會用到，獨立計算回測用。
        strategies_dict = {
            '01分進': [0.5,0.5,999,999],
            '02分進分出1': [0.5,0.5,0.5,999],
            '03分進分出1': [0.5,0.5,1.0,999],
            '04分進分出2': [0.5,0.5,0.5,1.0],
            '05分進分出2': [0.5,0.5,1.0,2.0],

            '06單進': [1.0,999,999,999],
            '07單進分出1': [1.0,999,0.5,999],
            '08單進分出1': [1.0,999,1.0,999],
            '09單進分出2': [1.0,999,0.5,1.0],
            '10單進分出2': [1.0,999,1.0,2.0],
        }

        ### 完整回測流程
        '''
        1. 製作seasonal_summary資料: 確認範圍初始為 2013-2022 (2023 reference) -> 2013-2023 (2024 reference) -> 2013-2024 (2025 reference)
        2. 進行標的自動篩選
        3. 策略套用: (先固定用 '10單進分出2': [1.0,999,1.0,2.0],)
        4. 輸出逐筆交易紀錄
        以上每年資料(2023, 2024, 2025)合併輸出整體逐筆交易紀錄
        '''
        transaction_df = pd.DataFrame()
        cash_balance = ini_cap
        for year in (2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025):
            self.config_obj.logger.warning(f"   ======  執行 {year} 季節性交易策略  ")
            self.config_obj.logger.warning(f"   ======      Step1. Monthly Stats       ")
            target_table = self.config_obj.monthly_info
            before_month = str(year) + '-01' ### 2023-01 代表計算 最初月份~2022-12
            stats_df = self.seasonal_strategy_obj.monthly_seasonality_stats(target_table, before_month)
            

            self.config_obj.logger.warning(f"   ======      Step2. Filtering       ")
            filter_parameters = {
                'market': 'US',
                'year_long': 8,
                'win_rate': 1.0,
                'volume':100000,
            }
            filtered_stats_df = self.seasonal_strategy_obj.monthly_seasonal_summary_filtering(stats_df, filter_parameters)

            self.config_obj.logger.warning(f"   ======      Step3. Fit Transaction Strategies      ")
            filtered_stats_df['建議策略'] = strategy_exit #'10單進分出2'

            self.config_obj.logger.warning(f"   ======      Step4. Transaction Records      ")
            end_month = 12 if year < 2025 else 3
            trades_df, cash = self.seasonal_strategy_obj.monthly_seasonaly_strategy_each_transaction_record(filtered_stats_df, strategies_dict,
                                                                                                                    principal=cash_balance,
                                                                                                                    start_year=year,
                                                                                                                    start_month=1,
                                                                                                                    end_year=year,
                                                                                                                    end_month=end_month)
            cash_balance = cash
            if transaction_df.empty:
                transaction_df = trades_df
            else:
                transaction_df = pd.concat([transaction_df, trades_df], ignore_index=True)
        path = os.path.join(self.config_obj.seasonal_summary, str(datetime.datetime.now()).split()[0] + f'_季節性策略回測_逐筆交易紀錄(策略_{strategy_exit}).xlsx')
        transaction_df.to_excel(path, index=False)        
        self.config_obj.logger.warning(f"回測完成，輸出至Excel。")
        return transaction_df
            
    def strategy_seasonal_investing_version_n_years(self, n_years, ini_cap, output_str):
        '''
        以累積所有年數觀察勝率，可能會有產業變遷的問題；所以關注近n_year年表現。
        DB資料範圍為2002-01~2025-04，故若n_year=3，可以從2005開始回測。
        1. for loop製作seasonal_summary資料: 2002-2004 (2005 reference), 2003-2005 (2006 reference), ... 2022-2024 (2025 reference)
        2. 進行標的自動篩選
        3. 交易策略套用
        4. 輸出逐筆交易紀錄
        以上每年資料(2005 ~ 2025)合併輸出整體逐筆交易紀錄
        '''
        strategies_dict = {
            '01分進': [0.5,0.5,999,999],
            '02分進分出1': [0.5,0.5,0.5,999],
            '03分進分出1': [0.5,0.5,1.0,999],
            '04分進分出2': [0.5,0.5,0.5,1.0],
            '05分進分出2': [0.5,0.5,1.0,2.0],

            '06單進': [1.0,999,999,999],
            '07單進分出1': [1.0,999,0.5,999],
            '08單進分出1': [1.0,999,1.0,999],
            '09單進分出2': [1.0,999,0.5,1.0],
            '10單進分出2': [1.0,999,1.0,2.0],
        }
        transaction_df = pd.DataFrame()
        cash_balance = ini_cap
        start_year = 2002 + n_years
        end_year = 2005
        for year in range(start_year, end_year + 1):
            self.config_obj.logger.warning(f"   ======  執行 {year} 季節性交易策略  ")
            self.config_obj.logger.warning(f"   ======      Step1. Monthly Stats       ")
            target_table = self.config_obj.monthly_info
            after_year = year-n_years
            before_year = year
            stats_df = self.seasonal_strategy_obj.monthly_seasonality_stats_n_years_interval(target_table, after_year, before_year)

            self.config_obj.logger.warning(f"   ======      Step2. Filtering       ")
            filter_parameters = {
                'market': 'US',
                'year_long': n_years,   # 至少n年
                'win_rate': 1.0,        # n年均賺錢
                'volume':100000,
            }
            filtered_stats_df = self.seasonal_strategy_obj.monthly_seasonal_summary_filtering(stats_df, filter_parameters)

            self.config_obj.logger.warning(f"   ======      Step3. Fit Transaction Strategies      ")
            filtered_stats_df['建議策略'] = '06單進'

            self.config_obj.logger.warning(f"   ======      Step4. Transaction Records      ")
            end_month = 12 if year < 2025 else 4
            trades_df, cash = self.seasonal_strategy_obj.monthly_seasonaly_strategy_each_transaction_record(filtered_stats_df, strategies_dict,
                                                                                                                    principal=cash_balance,
                                                                                                                    start_year=year,
                                                                                                                    start_month=1,
                                                                                                                    end_year=year,
                                                                                                                    end_month=end_month)
            cash_balance = cash
            if transaction_df.empty:
                transaction_df = trades_df
            else:
                transaction_df = pd.concat([transaction_df, trades_df], ignore_index=True)
        path = os.path.join(self.config_obj.seasonal_summary, str(datetime.datetime.now()).split()[0] + f'_季節性策略回測_逐筆交易紀錄(策略_{output_str}).csv')
        transaction_df.to_csv(path, index=False)        
        self.config_obj.logger.warning(f"回測完成，輸出至CSV。")
        return transaction_df

    
    def strategy_performance_output(self, strategy_name, ini_cap, transaction_df, output_str, rf_annual):

        # strategy_name = '季節性策略'
        # # strategy_name = '動能策略'
        if strategy_name == '季節性策略':
            path = os.path.join(self.config_obj.seasonal_summary, '2025-04-07_季節性策略回測_逐筆交易紀錄.xlsx')
            us_price_table = self.config_obj.us_stock_price_table
            ini_cap = ini_cap
        elif strategy_name == '動能策略':
            path = os.path.join(self.config_obj.seasonal_summary, '2025-04-06_動能策略_逐筆交易紀錄.xlsx') ### Produced by hgdfmjg
            us_price_table = self.config_obj.us_stock_price_table_IBAPI ### 
            ini_cap = ini_cap
            
        # trade_records = pd.read_excel(path)
        trade_records = transaction_df
        self.performance.generate_types_of_performance_output(strategy_name, trade_records, us_price_table, initial_capital=ini_cap, output_str=output_str, rf_annual=rf_annual)

    # def dash_server(self, data):
    #     self.dash_app = DashBuilder(data)

def main_flow():
    ''''''
    main_obj = StockStrategies()

    '''13F官網資料爬蟲 & 補充'''
    # main_obj.data_crawl()

    '''資料表計算操作'''
    # main_obj.data_update()
    # time.sleep(10)

    '''13F投資策略回測'''
    # main_obj.strategy_13F_investing()


    '''季節性策略回測'''
    # strategy_name = '季節性策略'
    # strategies_dict = {
    #     '01分進': [0.5,0.5,999,999],
    #     '02分進分出1': [0.5,0.5,0.5,999],
    #     '03分進分出1': [0.5,0.5,1.0,999],
    #     '04分進分出2': [0.5,0.5,0.5,1.0],
    #     '05分進分出2': [0.5,0.5,1.0,2.0],

    #     '06單進': [1.0,999,999,999],
    #     '07單進分出1': [1.0,999,0.5,999],
    #     '08單進分出1': [1.0,999,1.0,999],
    #     '09單進分出2': [1.0,999,0.5,1.0],
    #     '10單進分出2': [1.0,999,1.0,2.0],
    # }
    # for k in strategies_dict.keys():
    #     strategy_exit = k
    #     print(f"執行{strategy_name} 策略:{k}")
    #     ini_cap = 100000
    #     transaction_df = main_obj.strategy_seasonal_investing(ini_cap, strategy_exit)
    #     main_obj.strategy_performance_output(strategy_name, ini_cap, transaction_df, strategy_exit)

    '''季節性策略回測(近N年)'''    
    strategy_name = '季節性策略'
    output_str = '3Y'
    rf_annual = 0.02

    transaction_df = main_obj.strategy_seasonal_investing_version_n_years(n_years=3, ini_cap=100000, output_str=output_str)
    main_obj.strategy_performance_output(strategy_name=strategy_name, ini_cap=100000, transaction_df=transaction_df, output_str=output_str, rf_annual = rf_annual)

    '''動能策略回測'''
    # strategy_name = '動能策略'
    # ini_cap = 100000

    # file_str = 'plot_data_5_10_60_40_2025-04-27.csv'

    # data_path = os.path.join(main_obj.config_obj.backtest_summary, file_str)
    # if data_path.split('.')[-1] == 'xlsx':
    #     transaction_df = pd.read_excel(data_path)
    #     print(transaction_df)
    # else:
    #     transaction_df = pd.read_csv(data_path, header=0)
    #     print(transaction_df)
    
    # strategy_exit = file_str.split('.')[-2]
    # main_obj.strategy_performance_output(strategy_name, ini_cap, transaction_df, strategy_exit)

    '''Dash篩選'''
    # data_path = os.path.join(main_obj.config_obj.backtest_summary, '2024-01-28_summary_table.csv')
    # data = pd.read_csv(data_path)
    # data = main_obj.dash_server(data)


if __name__ == "__main__":
    main_flow()

