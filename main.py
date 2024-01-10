from py_module.config import Configuration
from py_module.crawler import Crawler
from py_module.strategies import Strategy13F

import os

class StockStrategies(object):

    def __init__(self):
        self.config_obj = Configuration()
        self.crawler_obj = Crawler()
        self.strategy_obj = Strategy13F()

    def data_crawl(self):
        self.crawler_obj.web_crawler_13F()

    def strategy_13F_investing(self):
        self.strategy_obj.main_strategy_flow()

def main_flow():
    ''''''
    main_obj = StockStrategies()

    '''13F官網資料爬蟲'''
    # main_obj.data_crawl()

    '''13F投資策略回測'''
    main_obj.strategy_13F_investing()



if __name__ == "__main__":
    main_flow()

