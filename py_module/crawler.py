from py_module.config import Configuration

import requests
from bs4 import BeautifulSoup
import os
import time
import pymssql
import pandas as pd
import numpy as np
import logging

class Crawler(object):

    def __init__(self):
        self.config_obj = Configuration()
        self.hedge_fund_portfolio_table = '[US_DB].[dbo].[HEDGE_FUND_PORTFOLIO]'
        self.holdings_data_table = '[US_DB].[dbo].[HOLDINGS_DATA]'
        self.us_stock_info_table = '[US_DB].[dbo].[USStockInfo]'
        self.us_stock_price_table = '[US_DB].[dbo].[USStockPrice]'
        self.tw_stock_price_table = '[STOCK_SKILL_DB].[dbo].[TW_STOCK_PRICE_Daily]'
    
    def web_crawler_13F(self):
        '''
        '''

        query = self.create_query_get_13F_filing_id_in_DB(self.hedge_fund_portfolio_table)
        existed_filing_id = self.sql_execute(query)
        existed_filing_id = pd.DataFrame(existed_filing_id)['FILING_ID'].values

        query = self.create_query_get_13F_filing_id_in_DB(self.holdings_data_table)
        holdings_existed_filing_id = self.sql_execute(query)
        holdings_existed_filing_id = pd.DataFrame(holdings_existed_filing_id)['FILING_ID'].values

        # urls = self.config_obj.hedge_fund_urls
        urls = self.config_obj.all_13F_manager_urls
        headers = {
            'user-agent': 'Mozilla/5.0'
        }
        parameters = {}
        self.config_obj.logger.warning("Web_crawler_13F，預計爬取共{}支基金資料".format(len(urls)))

        for idx, name in enumerate(urls):
            

            response = requests.get(urls[name], headers = headers)
            soup = BeautifulSoup(response.text, "html.parser")
            self.config_obj.logger.warning("第{}支基金：{}".format(idx+1, name))
            self.config_obj.logger.warning("    連線網址：{}".format(response.url))
            self.config_obj.logger.warning("    連線狀況：{}".format(response.status_code))
            self.config_obj.logger.warning("    網頁Title：{}".format(soup.title.string))
            table = soup.find('table')

            # 初始化空的資料列表
            data = []
            holdings_urls = {}

            # 遍歷表格的每一列（排除表頭）
            for row in table.find_all('tr')[1:]:
                '''
                get manager表格資料
                '''
                # 找到列中的每個資料單元格
                cells = row.find_all('td')
                quarterly_link = "https://13f.info" + row.find('a')['href']
                                
                # 提取所需欄位的資料
                quarter = cells[0].text.strip()
                holdings = cells[1].text.strip()
                value = cells[2].text.strip()
                top_holdings = cells[3].text.strip()
                form_type = cells[4].text.strip()
                date_filed = cells[5].text.strip()
                filing_id = cells[6].text.strip()
                
                
                holdings_urls[(quarter, form_type, filing_id)] = quarterly_link
                # 將資料組合成字典並添加到資料列表中
                data.append({
                    'QUARTER': quarter,
                    'HOLDINGS': int("".join(holdings.split(','))),
                    'VALUE ($000)': int("".join(value.split(','))),
                    'TOP HOLDINGS': top_holdings,
                    'FORM TYPE': form_type,
                    'DATE FILED': date_filed,
                    'FILING_ID': filing_id,
                    'HEDGE FUND': name,
                })
            hedge_fund_data = pd.DataFrame(data)
            hedge_fund_data = hedge_fund_data.replace(np.nan, None) # 部分資料為pandas nan，需轉為python None
            # output_folder = self.config_obj.assets_hedge_fund_data
            # file_name = "hedge_fund_portfolio_filings_" + str(idx+1) + "_" + "-".join(soup.title.string.split()) + ".csv"
            # hedge_fund_data.to_csv(os.path.join(output_folder, file_name), index=False)

            '''
            確認DB中的資料，並比對差異。
            '''
            hedge_fund_data = self.remove_existed_records_by_filing_id(hedge_fund_data, existed_filing_id)
            if len(hedge_fund_data) != 0:
                self.insert_records_to_DB(table_name=self.hedge_fund_portfolio_table, data=hedge_fund_data)
                self.config_obj.logger.info('{} hedge data have been stored from hedge {}.(DB:{})'.format(len(hedge_fund_data), name, self.hedge_fund_portfolio_table))
            else:
                self.config_obj.logger.info('No hedge data have been stored from hedge {}.'.format(name))
                pass

                
            count = 0
            for (quarter, form_type, filing_id), quarterly_link in holdings_urls.items():
                
                count += 1  
                network_source = quarterly_link.split('-')[0].split('/')[-1]
                network_source = "https://13f.info/data/13f/"  + network_source

                headers = {
                    'Accept': 'application/json, text/javascript, */*; q=0.01',
                    'Referer': quarterly_link,
                    'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                    'Sec-Ch-Ua-Mobile': '?0',
                    'Sec-Ch-Ua-Platform': '"Windows"',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'X-Requested-With': 'XMLHttpRequest'
                }

                holdings_response = requests.get(network_source, headers=headers)
                
                if holdings_response.status_code == 200:
                    # time.sleep(10)
                    data = holdings_response.json()
                    
                else:
                    self.config_obj.logger.error("基金{} 資料{}-{}連線狀態非200，無法獲取數據。".format(name, str(quarter), str(form_type)))

                holdings_data = data['data']
                holdings_data = pd.DataFrame(holdings_data, columns = ['SYM','ISSUER NAME','CL','CUSIP','VALUE ($000)','%','SHARES','PRINCIPAL','OPTION TYPE',])
                holdings_data = holdings_data.replace(np.nan, None) # 部分資料為pandas nan，需轉為python None
                holdings_data['HEDGE FUND'] = name
                holdings_data['QUARTER'] = quarter
                holdings_data['FORM TYPE'] = form_type
                holdings_data['FILING_ID'] = filing_id

                # output_folder = os.path.join(self.config_obj.assets_holdings_data, name)
                # if not os.path.exists(output_folder):
                #     os.makedirs(output_folder)
                # file_name = "holdings_data_" + "-".join(str(quarter).split()) + "_" + form_type + "_" + str(count) + ".csv"
                # holdings_data.to_csv(os.path.join(output_folder, file_name), index=False)

                holdings_data = self.remove_existed_records_by_filing_id(holdings_data, holdings_existed_filing_id)
                if len(holdings_data) != 0:
                    self.insert_records_to_DB(table_name=self.holdings_data_table, data=holdings_data)
                    print('{} holdings data have been stored.'.format(len(holdings_data)))
                    self.config_obj.logger.info('{} holdings data have been stored from hedge {}.(DB:{})'.format(len(holdings_data), name, self.holdings_data_table))
                else:
                    self.config_obj.logger.info('No holdings data have been stored from hedge {}.'.format(name))
                    pass

            time.sleep(10)
    
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
    
    def create_query_get_13F_filing_id_in_DB(self, table_name):
            
        query = '''SELECT DISTINCT [FILING_ID] 
        FROM {}
        '''.format(table_name)
        return query

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
        
        cursor.close()
        conn.close()

    def remove_existed_records_by_filing_id(self, hedge_fund_data, existed_filing_id):

        # existed_filing_id = np.append(existed_filing_id, '000143499724000001')
        remove_idx = []
        for index, row in hedge_fund_data.iterrows():
            if row['FILING_ID'] in existed_filing_id:
                remove_idx.append(index)
        adjusted_hedge_fund_data = hedge_fund_data.drop(remove_idx, axis=0)
        return adjusted_hedge_fund_data

        