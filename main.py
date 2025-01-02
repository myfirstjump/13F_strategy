from py_module.config import Configuration
from py_module.crawler import Crawler, IBApp
from py_module.strategies import Strategy13F, StrategySeasonal
# from dashboard import DashBuilder
from py_module.database_CRUD import DatabaseManipulation

import os
import pandas as pd
import logging

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
        target_table = self.config_obj.monthly_info
        self.seasonal_strategy_obj.monthly_seasonality_stats(target_table)
    
    # def dash_server(self, data):
    #     self.dash_app = DashBuilder(data)

def main_flow():
    ''''''
    main_obj = StockStrategies()

    '''13F官網資料爬蟲 & 補充'''
    # main_obj.data_crawl()

    '''資料表計算操作'''
    # main_obj.data_update()

    '''13F投資策略回測'''
    # main_obj.strategy_13F_investing()
    main_obj.strategy_seasonal_investing()

    '''Dash篩選'''
    # data_path = os.path.join(main_obj.config_obj.backtest_summary, '2024-01-28_summary_table.csv')
    # data = pd.read_csv(data_path)
    # data = main_obj.dash_server(data)


if __name__ == "__main__":
    main_flow()

