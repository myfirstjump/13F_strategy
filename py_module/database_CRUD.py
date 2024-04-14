from py_module.config import Configuration

import pymssql
import pandas as pd
import numpy as np
import sys
# np.set_printoptions(threshold=sys.maxsize)
import copy
import os
import datetime

class DatabaseManipulation(object):
    def __init__(self):
        self.config_obj = Configuration()
        self.hedge_fund_portfolio_table = '[US_DB].[dbo].[HEDGE_FUND_PORTFOLIO]'
        self.holdings_data_table = '[US_DB].[dbo].[HOLDINGS_DATA]'
        self.us_stock_info_table = '[US_DB].[dbo].[USStockInfo]'
        self.us_stock_price_table = '[US_DB].[dbo].[USStockPrice]'
        self.us_stock_gics_table = '[US_DB].[dbo].[Company_GICS]'
    
    def Update_GICs_to_DB(self):
        '''
        1.讀取資料源: excel, csv, ...，通常為原有GICs表格資料不足，透過查詢後需要補充至資料庫的數據。
            結構應該至少包含SYMBOL、SECTOR欄位。
        2.同時Read, Update資料，並將update log存檔
        '''

        # 1.讀取資料源: excel, csv, ...
        source_path = os.path.join(self.config_obj.reference_folder, '2024-04-07_新增GICs產業表(Peter).xlsx')
        data_1 = pd.read_excel(source_path, sheet_name='4%')
        data_2 = pd.read_excel(source_path, sheet_name='1%')
        data_3 = pd.read_excel(source_path, sheet_name='ALL')

        data = pd.concat([data_1, data_2, data_3])
        data = data.drop(['CUSIP', ], axis=1)
        data = data.dropna(subset = ['SECTOR'])
        data['SECTOR'] = data['SECTOR'].str.replace('industry\xa0', 'Industrials')
        data['SECTOR'] = data['SECTOR'].str.replace('Industrial\xa0', 'Industrials')
        data['SECTOR'] = data['SECTOR'].str.replace('Communication\xa0Services', 'Communication Services')
        data['SECTOR'] = data['SECTOR'].str.replace('Consumer Staples ', 'Consumer Staples')
        data['SECTOR'] = data['SECTOR'].str.replace('Healthcare', 'Health Care')
        data['SECTOR'] = data['SECTOR'].str.replace('Technology', 'Information Technology')
        def apply_to_GICs_number(row):
            my_dict = self.config_obj.gics_dict
            for k_, v_ in my_dict.items():
                if v_ == row['SECTOR']:
                    return k_
            return None
        data['GICS_2_digit'] = data.apply(apply_to_GICs_number, axis=1)
        data = data.drop_duplicates(subset=['SYMBOL']).reset_index(drop=True)

        # print(data)
        print(data['SECTOR'].unique())
        print(data['GICS_2_digit'].unique())

        # 2.同時Read, Update資料，並將update log存檔
        query = self.create_query_get_GICs_Tickers_in_DB(self.us_stock_gics_table)
        db_data = self.sql_execute(query)
        db_data = pd.DataFrame(db_data)
        # print(db_data)

        # 找出交集
        intersection = pd.merge(data, db_data, left_on='SYMBOL', right_on='Ticker', how='inner')

        # 找出差异
        data_only = data[~data['SYMBOL'].isin(db_data['Ticker'])]
        db_data_only = db_data[~db_data['Ticker'].isin(data['SYMBOL'])]

        print("交集項目:")
        print(intersection.sort_values(by='SYMBOL'))
        # print("\n差異項目:")
        # print("在 data 中但不在 db_data 中的項目:")
        # print(data_only)
        # print("\n在 db_data 中但不在 data 中的項目:")
        # print(db_data_only)

        data_only['GICS_4_digit'] = '-'
        data_only['GICS_6_digit'] = '-'
        data_only['GICS_8_digit'] = '-'
        data_only = data_only.rename(columns={"ISSUER_NAME": "CompanyName", "SYMBOL":"Ticker"})
        data_only = data_only[['CompanyName', 'Ticker', 'GICS_2_digit', 'GICS_4_digit', 'GICS_6_digit', 'GICS_8_digit']]
        data_only = data_only.fillna('-')
        # data_only = data_only.sort_values(by='Ticker')

        print("在 data 中但不在 db_data 中的項目:")
        print(data_only)
        # self.insert_records_to_DB(table_name=self.us_stock_gics_table, data=data_only)



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
    
    def create_query_get_GICs_Tickers_in_DB(self, table_name):
            
        query = '''SELECT DISTINCT [Ticker] 
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

        elif table_name == '[US_DB].[dbo].[Company_GICS]':
            cursor.executemany(
                    """INSERT INTO {}
                    (
                    [CompanyName]
                    ,[Ticker]
                    ,[GICS_2_digit]
                    ,[GICS_4_digit]
                    ,[GICS_6_digit]
                    ,[GICS_8_digit]
                    ) 
                    VALUES(%s,%s,%s,%s,%s,%s)""".format(table_name)
                    , data_tuple
            )
            conn.commit()

        cursor.close()
        conn.close()