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
        query = self.create_query_get_GICs_Tickers_in_DB(self.config_obj.us_stock_gics_table)
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
        # self.insert_records_to_DB(table_name=self.config_obj.us_stock_gics_table, data=data_only)

    def generate_monthly_stock_info(self, source_table, target_table, before_month):

        '''
        讀取DB裡面的每日price數據，進行月統計計算後回存至DB另一個table。
        由於是月統計，最好設定before_month為某年某月1日，讓統計資料完整。
        '''
        # Step 1: Fetch unique stock IDs from the source table
        query_stock_ids = f"""
        SELECT DISTINCT stock_id
        FROM {source_table}
        """
        stock_ids = self.sql_execute(query_stock_ids)

        # Step 2: Fetch existing stock IDs from the target table to exclude
        query_existing_ids = f"""
        SELECT DISTINCT stock_id
        FROM {target_table}
        """
        existing_ids = self.sql_execute(query_existing_ids)
        existing_ids_set = {row['stock_id'] for row in existing_ids}

        # Filter out stock IDs that already exist in the target table
        stock_ids_to_process = [stock for stock in stock_ids if stock['stock_id'] not in existing_ids_set]
        # stock_ids_to_process = [{'stock_id':'GLBZ'}] ## Test

        # Step 3: Iterate over each stock_id and calculate monthly returns, average volume, and max drawdown
        for idx, stock in enumerate(stock_ids_to_process):
            stock_id = stock['stock_id']
            self.config_obj.logger.warning(f"計算{stock_id}月資料({idx+1}/{len(stock_ids_to_process)})。")
            
            # Fetch daily data for the current stock
            if 'USStockPrice' in source_table:
                query_daily_data = f"""
                SELECT [date], [Close], [Open], [Volume]
                FROM {source_table} WITH(NOLOCK)
                WHERE stock_id = '{stock_id}'
                AND [date] < '{before_month}'
                """
            elif "TW_STOCK" in source_table:
                query_daily_data = f"""
                SELECT [date], [close], [open], [Trading_Volume]
                FROM {source_table} WITH(NOLOCK)
                WHERE stock_id = '{stock_id}'
                AND [date] < '{before_month}'
                """
            daily_data = self.sql_execute(query_daily_data)

            # Convert to DataFrame and process
            df = pd.DataFrame(daily_data)
            if len(df) < 2000: ### 2500大約為8年的交易日數量(1年250日左右)，若低於8年則不列入計算。
                self.config_obj.logger.warning(f"交易年數較低(約{len(df)//250}年)，不進行後續計算。")
                continue

            if "TW_STOCK" in source_table:
                df = df.rename({'close': 'Close', 'open': 'Open', 'Trading_Volume':'Volume' }, axis='columns')

            df['date'] = pd.to_datetime(df['date'])
            df['month'] = df['date'].dt.to_period('M').astype(str)

            # Calculate max drawdown for each month
            def calculate_max_drawdown(group):
                cumulative_max = group['Close'].cummax()
                drawdowns = (group['Close'] - cumulative_max) / cumulative_max
                drawdowns = round(drawdowns, 6)
                return drawdowns.min()

            # Calculate monthly returns, average volume, and max drawdown using apply
            def calculate_metrics(group):
                group = group.sort_values(by='date')
                open_price = group['Open'].iloc[0]
                if open_price == 0 or pd.isna(open_price):
                    monthly_return = None
                    pct_to_low = None
                    pct_to_high = None
                else:
                    monthly_return = (group['Close'].iloc[-1] - open_price) / open_price
                    monthly_return = round(monthly_return, 6)
                    pct_to_low = (group['Close'].min() - open_price) / open_price
                    pct_to_low = round(pct_to_low, 6)
                    pct_to_high = (group['Close'].max() - open_price) / open_price
                    pct_to_high = round(pct_to_high, 6)

                avg_volume = round(group['Volume'].mean()) if not pd.isna(group['Volume']).all() else None
                max_drawdown = calculate_max_drawdown(group) if not group['Close'].isnull().all() else None

                return pd.Series({
                    'monthly_return': monthly_return,
                    'avg_volume': avg_volume,
                    'max_drawdown': max_drawdown,
                    'pct_to_low': pct_to_low,
                    'pct_to_high': pct_to_high
                })

            monthly_data = df.groupby('month').apply(calculate_metrics).reset_index()

            # Add stock_id and market columns
            monthly_data['stock_id'] = stock_id
            monthly_data['market'] = 'US' if 'USStockPrice' in source_table else 'TW'
            monthly_data = monthly_data[['month', 'stock_id', 'monthly_return', 'avg_volume', 'max_drawdown', 'pct_to_low', 'pct_to_high', 'market']]

            # Replace invalid values (e.g., inf) with None for SQL compatibility
            monthly_data.replace([float('inf'), float('-inf')], None, inplace=True)

            # Drop rows with NaN values
            monthly_data.dropna(inplace=True)

            # Step 4: Insert the processed data into the target table
            table_name, inserted_rows = self.insert_records_to_DB(target_table, monthly_data)
            self.config_obj.logger.warning(f"完成資料匯入(實際筆數{inserted_rows}/全部{len(monthly_data)})。")

        return f"Monthly data for all stocks saved to {target_table}"


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
        if table_name == '[US_DB].[dbo].[Company_GICS]':
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

        elif table_name == '[SEASONAL_STAT_DB].[dbo].[MONTHLY_INFO_8COL]':
            cursor.executemany(
                    """INSERT INTO {}
                    (
                    [month]
                    ,[stock_id]
                    ,[monthly_return]
                    ,[avg_volume]
                    ,[max_drawdown]
                    ,[pct_to_low]
                    ,[pct_to_high]
                    ,[market]
                    ) 
                    VALUES(%s,%s,%d,%d,%d,%d,%d,%s)""".format(table_name)
                    , data_tuple
            )
            conn.commit()

        inserted_rows = cursor.rowcount
        cursor.close()
        conn.close()

        return table_name, inserted_rows