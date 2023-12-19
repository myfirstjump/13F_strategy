from py_module.config import Configuration
from py_module.crawler import Crawler

import os

class StockStrategies(object):

    def __init__(self):
        self.config_obj = Configuration()
        self.crawler_obj = Crawler()

    def data_crawl(self):
        self.crawler_obj.web_crawler_13F()

def main_flow():
    ''''''
    main_obj = StockStrategies()
    main_obj.data_crawl()



if __name__ == "__main__":
    main_flow()

