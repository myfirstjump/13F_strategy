from py_module.config import Configuration
from py_module.crawler import Crawler, IBApp
from py_module.strategies import Strategy13F, StrategySeasonal
# from dashboard import DashBuilder
from py_module.database_CRUD import DatabaseManipulation

import os
import pandas as pd
import logging
import time

class StockStrategies(object):

    def __init__(self):
        self.config_obj = Configuration()
        self.crawler_obj = Crawler()
        self.stock_crawler_obj = IBApp()
        self.strategy_obj = Strategy13F()
        self.seasonal_strategy_obj = StrategySeasonal()
        self.db_obj = DatabaseManipulation()

    def data_crawl(self):
        self.crawler_obj.web_crawler_13F()
        # self.crawler_obj.web_crawler_13F_one_thread()
        # self.stock_crawler_obj.stock_data_crawler()

    def data_update(self):
        # self.db_obj.Update_GICs_to_DB()

        source_table = self.config_obj.us_stock_price_table
        target_table = self.config_obj.monthly_info
        self.db_obj.generate_monthly_stock_info(source_table=source_table, target_table=target_table)

        source_table = self.config_obj.tw_stock_price_table
        target_table = self.config_obj.monthly_info
        self.db_obj.generate_monthly_stock_info(source_table=source_table, target_table=target_table)

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
    
    def strategy_seasonal_investing(self):
        ### 製作seasonal_summary資料
        # target_table = self.config_obj.monthly_info
        # self.seasonal_strategy_obj.monthly_seasonality_stats(target_table)

        ### 透過seasonal_summary資料，進行回測
        path = os.path.join(self.config_obj.seasonal_summary, '2025-03-01_seasonal_summary(filtered).xlsx')
        seasonal_filtered_df = pd.read_excel(path)
        # final_result = self.seasonal_strategy_obj.monthly_seasonaly_strategy_backtest(seasonal_filtered_df)
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
        # for idx, (name, param) in enumerate(strategies_dict.items()):
        #     self.config_obj.logger.warning(f"策略({name})計算... ({idx+1}/{len(strategies_dict)})")
        #     self.seasonal_strategy_obj.monthly_seasonaly_strategy_adjusted_backtest(strategy_name=name, 
        #                                                                             seasonal_filtered_df=seasonal_filtered_df, 
        #                                                                             INITIAL_CAP_PERCENT=param[0],    # 分批投入時，初始投入比例。
        #                                                                             BUY_IN_RATE=param[1],            # 判斷是否分批時，最大跌幅標準差的係數  (↑越容易加碼 ↓越難加碼)
        #                                                                             OUT_LV1_STD_RATE=param[2],       # 提早停利時(一半出場)，最大漲幅標準差的係數 (↑越難停利)
        #                                                                             OUT_LV2_STD_RATE=param[3],       # 提早停利時(全數出場)，最大漲幅標準差的係數 (↑越難停利) 
        #                                                                             principal=100000,)
        # print(final_result)

        # strategy_paths = []
        # for idx, (name, param) in enumerate(strategies_dict.items()):
        #     path = os.path.join(self.config_obj.seasonal_summary, '2025-02-21' + f'_seasonal_strategy_{name}({param[0]}-{param[1]}-{param[2]}-{param[3]})_backtest.xlsx')
        #     strategy_paths.append(path)
        # self.seasonal_strategy_obj.seasonal_strategies_comparison_function(strategy_paths)

        self.seasonal_strategy_obj.monthly_seasonaly_strategy_2025v1(seasonal_filtered_df, strategies_dict)
    
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
    main_obj.strategy_seasonal_investing()

    '''Dash篩選'''
    # data_path = os.path.join(main_obj.config_obj.backtest_summary, '2024-01-28_summary_table.csv')
    # data = pd.read_csv(data_path)
    # data = main_obj.dash_server(data)


if __name__ == "__main__":
    main_flow()

