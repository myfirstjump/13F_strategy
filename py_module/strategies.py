from py_module.config import Configuration

import pymssql
import pandas as pd
import numpy as np
import sys
np.set_printoptions(threshold=sys.maxsize)
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

    def main_strategy_flow(self):
        '''
        美股13F投資策略回測程序
            0. 設定需求資料物件。{'date': holdings_time, '市值': market_value, '加碼': scaling_in_sum, '減碼': scaling_out_sum, 'XIRR':xirr}
            1. 連結至Database，獲取hedge fund清單、時間列(需要max date作為最後市值計算)
            開始各hedge fund回測
                開始各年份迴圈
                    3. records SYM查表找尋price table open值
                    4. 計算shares差異、市值差異(加碼、減碼)
                    5. 紀錄交易時間、應收付損益
                    6. 計算最大回撤dd
                6.計算年化報酬
                7.計算最終獲利
                8.繪製資金曲線圖
                9.各時間點加碼、減碼；淨加碼、減碼
                10.更新下一輪參數 ex. previous_data = price_data
        '''

        '''0'''
        ### Final Output Form
        summary_data = [] # 包含項目: {'date': holdings_time, '市值': market_value, '加碼': scaling_in_sum, '減碼': scaling_out_sum, 'XIRR':xirr}
        null_sym_counter = 0

        '''1'''
        query = self.create_query_data_table(self.hedge_fund_portfolio_table)
        fund_data = self.sql_execute(query)
        fund_data = pd.DataFrame(fund_data)
        hedge_fund_list = fund_data['HEDGE_FUND'].unique()
        # hedge_fund_list = ['Appaloosa', ]
        hedge_fund_list = list(hedge_fund_list)
        # hedge_fund_list = ['Citadel Advisors', 'Renaissance Technologies', 'Millennium Management']
        # hedge_fund_list.remove('Citadel Advisors')
        # hedge_fund_list.remove('Renaissance Technologies')
        # hedge_fund_list.remove('Millennium Management')

        print("總共包含{}個對沖基金資料".format(len(hedge_fund_list)))
        print('Hedge Funds:', )
        print(hedge_fund_list)

        # 找到price data中的date欄位，對日期進行排序，找到最大的日期
        query = self.get_all_price_date(self.us_stock_price_table) # 為了取得時間欄位
        all_date_list = self.sql_execute(query)
        all_date_list = pd.DataFrame(all_date_list)['date'].values
        sorted_dates = sorted(all_date_list)
        sorted_dates = pd.to_datetime(sorted_dates)
        min_date = min(sorted_dates)
        max_date = max(sorted_dates)
        print('歷史價格從{}到{}'.format(min_date, max_date))

        for idx, hedge_fund in enumerate(hedge_fund_list):

            ### Calculation parameters
            previous_data = None
            previous_holdings = None
            xirr_calculate_dict = {'date':[], 'amounts':[]}

            each_fund_data = self.each_fund_data_adjust(fund_data, hedge_fund)
            quarters_list = each_fund_data['QUARTER'].values
            date_list = each_fund_data['BASE_DATE'].values # 進場時間點使用13F公布時間
            filing_list = each_fund_data['FILING_ID'].values

            print(" === === === 第{}個對沖基金：{}，包含{}個季度資料。 === === === ".format(idx+1, hedge_fund, len(quarters_list)))
            # print(each_fund_data)
            
            for idx_q, (quarter, holdings_time, filing_number) in enumerate(zip(quarters_list, date_list, filing_list)):
                hedge_fund_data = {'date': None, '市值': None, '加碼': None, '減碼': None, 'XIRR': None}
                holdings_time = self.adjust_holdings_time(holdings_time, sorted_dates) # 以13F報告公布期限為基準(5/15, 8/15, 11/15, 2/14)
                print("     第{}個季度：{}，時間為{}".format(idx_q+1, quarter, holdings_time))

                query = self.create_query_holdings(hedge_fund, quarter, filing_number)
                holdings_data = self.sql_execute(query)
                holdings_data = pd.DataFrame(holdings_data)
                # print(holdings_data)
                # print(holdings_data['SHARES'].isnull().values.any())

                if idx_q > 0: #扣除第一季，每季要計算的內容

                    merged_data = self.shares_difference_between_quarters(previous_holdings, holdings_data)
                    '''
                          SYM  SHARES_current  SHARES_previous  shares_change
                    0     AGN         2937121          4261406       -1324285
                    1    ETP1        11961842         11961842              0
                    2     WPZ         9966502          9487301         479201
                    3    GOOG          475000           483000          -8000
                    4    META         1907350          2182350        -275000
                    5     PNC         1950973          1950973              0
                    6     BAC         8782641                0        8782641
                    '''
                    null_sym_counter = null_sym_counter + merged_data['SYM'].isna().sum()
                    sym_str = merged_data['SYM'].dropna().values
                    
                    # print("SYMs:", sym_str)
                    if len(sym_str) == 1:
                        sym_str = str(sym_str)
                        sym_str = sym_str.replace('[', '(')
                        sym_str = sym_str.replace(']', ')')
                    else:
                        sym_str = tuple(sym_str)
                    
                    query = self.create_query_get_open_price_by_date_n_sym(sym_str, holdings_time)
                    # print(query)
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
                    market_value, scaling_in, scaling_out, scaling_even = self.calculate_scaling_in_and_out(merged_data, price_data)
                    # print("Shares Increased:", scaling_in)
                    # print("Shares Decreased:", scaling_out)
                    # print("Shares Unchanged:", scaling_even)
                    scaling_in_sum = sum([i for i in  scaling_in.values()])
                    scaling_out_sum = sum([i for i in  scaling_out.values()])
                    
                    xirr_calculate_dict['date'].append(holdings_time)
                    xirr_calculate_dict['amounts'].append(-(scaling_in_sum - scaling_out_sum))
                    
                else: #第一季要計算的內容
                    null_sym_counter = null_sym_counter + holdings_data['SYM'].isna().sum()
                    sym_str = holdings_data['SYM'].dropna().values # TBD: 確認Drop數量
                    if len(sym_str) == 1:
                        sym_str = str(sym_str)
                        sym_str = sym_str.replace('[', '(')
                        sym_str = sym_str.replace(']', ')')
                    else:
                        sym_str = tuple(sym_str)
                    query = self.create_query_get_open_price_by_date_n_sym(sym_str, holdings_time)
                    price_data = self.sql_execute(query)
                    price_data = pd.DataFrame(price_data)
                    market_value = self.market_value_by_join_holdings_and_price(holdings_data, price_data)

                    scaling_in_sum = 0
                    scaling_out_sum = 0
                    xirr_calculate_dict['date'].append(holdings_time)
                    xirr_calculate_dict['amounts'].append(-market_value)
                # 計算XIRR
                temp_xirr_calculate_dict = copy.deepcopy(xirr_calculate_dict)
                if idx_q == 0: # 第一季直接帶pyxirr公式計算結果為10%，沒有研究計算公式，故直接assign 0。
                    xirr = 0
                else:
                    xirr = self.calculate_XIRR(temp_xirr_calculate_dict, holdings_time, market_value)
                previous_holdings = holdings_data.copy()

                hedge_fund_data = {'date': holdings_time, '市值': market_value, '加碼': scaling_in_sum, '減碼': scaling_out_sum, 'XIRR':xirr}
                summary_data.append({'hedge_fund': hedge_fund, **hedge_fund_data})
            
            # 以今日計算各指標(架構同上)
            holdings_time = max_date # 可以自訂，此處以DB中最大有交易日期為主(2024-01-09)
            query = self.create_query_get_open_price_by_join_holdings_n_price(sym_str, holdings_time, hedge_fund, quarter, filing_number)
            price_data = self.sql_execute(query)
            price_data = pd.DataFrame(price_data)
            market_value = sum(price_data['Open'] * price_data['SHARES'])
            xirr = self.calculate_XIRR(xirr_calculate_dict, holdings_time, market_value)
            hedge_fund_data = {'date': holdings_time, '市值': market_value, '加碼': 0, '減碼': 0, 'XIRR':xirr}
            summary_data.append({'hedge_fund': hedge_fund, **hedge_fund_data})

        summary_table = pd.DataFrame(summary_data)
        path = os.path.join(self.config_obj.backtest_summary, str(datetime.datetime.now()).split()[0] + '_summary_table.csv')
        summary_table.to_csv(path, index=False)
        print("NULL SYM COUNTER:", null_sym_counter)

    def sql_execute(self, query):

        # conn = pymssql.connect(host='localhost', user = 'myfirstjump', password='myfirstjump', database='US_DB')
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

    def each_fund_data_adjust(self, fund_data, hedge_fund):
        # adjusted_fund_data = fund_data[fund_data['HEDGE_FUND']==hedge_fund]
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
        df['BASE_DATE'] = df.apply(calculate_base_date, axis=1)# 新增 'BASE DATE' 基準日期欄位 (即5/15, 8/14, 11/14, 2/14)
        df['DATE_FILED'] = pd.to_datetime(df['DATE_FILED'])# 將 'DATE_FILED' 轉換為日期時間格式

        df = df[~(df['FORM_TYPE'] == 'NEW HOLDINGS')] # 刪除NEW HOLDINGS的Records
        df = df[~((df['FORM_TYPE'] == 'RESTATEMENT') & (df['DATE_FILED'] > df['BASE_DATE']))] # 刪除 RESTATEMENT 大於 基準日期的Records。
        sorting_key = df.groupby('BASE_DATE')['FILING_ID'].idxmax() # 取group中最大FILING_ID
        df = df.loc[sorting_key].reset_index(drop=True)
        return df
    
    def create_query_data_table(self, data_table):
        '''
        其中依照[DATE_FILED]做升續排列。
        '''
        query = '''SELECT * FROM {} WITH(NOLOCK) ORDER BY [DATE_FILED] ASC'''.format(data_table)
        return query
    
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
    def adjust_holdings_time(self, holdings_time, sorted_dates):
        '''
        在輸入時間點為13F報告公布時間時，該日不一定有開市，所以依據時間調整。
        '''
        index = sorted_dates.searchsorted(holdings_time) # 找到日期在排序後的列表中的位置
        target_date  = sorted_dates[index] if index < len(sorted_dates) else sorted_dates[-1] # 如果日期正好在列表中，返回該日期；否則返回下一個最接近的日期
        # print('原始日期:', holdings_time)
        # print('index: ', index)
        # print('修正日期:', target_date)
        return target_date
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
    def create_query_get_open_price_by_date_n_sym(self, SYMs_tuple, date):
        '''
        依據holdings去查表price，透過stock_id(即holdings表中的SYM) join兩張表格，並加入SHARES資訊至price表。
        '''
        price_table = self.us_stock_price_table
        query = ''' SELECT [date], [stock_id] SYM, [Open]
            FROM {} WITH(NOLOCK) WHERE [date] = '{}' AND [stock_id] IN {}
            '''.format(price_table, date, SYMs_tuple)
        return query

    def calculate_scaling_in_and_out(self, merged_data, price_data):
        scaling_in = {}
        scaling_out = {}
        scaling_even = {}
        market_value = 0
        scaling_data = merged_data.merge(price_data, on=['SYM'])
        # print('Scaling_data:')
        # print(scaling_data)
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

        return market_value, scaling_in, scaling_out, scaling_even

    def market_value_by_join_holdings_and_price(self, holdings_data, price_data):
        market_value = 0
        merged_data = holdings_data.merge(price_data, on=['SYM'])
        # print('Holdings & Price:')
        # print(merged_data)

        for index, row in merged_data.iterrows():
            shares = row['SHARES']
            Open_current = row['Open']
            market_value = market_value + shares * Open_current
        return market_value
    
    def shares_difference_between_quarters(self, previous_holdings, holdings_data):
        scaling_in = {}
        scaling_out = {}
        scaling_even = {}

        previous_holdings = previous_holdings[['SYM', 'SHARES']]
        holdings_data = holdings_data[['SYM', 'SHARES']]

        # 將兩個資料表合併
        merged_data = holdings_data.merge(previous_holdings, on=['SYM'], how='outer', suffixes=('_current', '_previous'))
        merged_data['SHARES_current'].fillna(0, inplace=True)
        merged_data['SHARES_previous'].fillna(0, inplace=True)
        merged_data = merged_data.astype({'SHARES_current': int, 'SHARES_previous': int})
        # 計算持股數量變化
        merged_data['shares_change'] = merged_data['SHARES_current'] - merged_data['SHARES_previous']
        
        # print(merged_data)
        # 根據持股數量變化分類
        # for index, row in merged_data.iterrows():
        #     stock_id = row['SYM']
        #     shares_change = row['shares_change']
        #     Open_current = row['Open_current']

        #     if shares_change > 0:
        #         scaling_in[stock_id] = shares_change * Open_current
        #     elif shares_change < 0:
        #         scaling_out[stock_id] = abs(shares_change) * Open_current
        #     else:
        #         scaling_even[stock_id] = 0
        
        # return scaling_in, scaling_out, scaling_even

        return merged_data

    def get_all_price_date(self, price_table):

        query = '''
        SELECT DISTINCT [date]
        FROM {}
        '''.format(price_table)
        return query
    
    def calculate_XIRR(self, data, holdings_time, market_value):
        '''
        將data資料加上最新時間/市值兌現，計算XIRR值。
        '''
        data['date'].append(holdings_time)
        data['amounts'].append(market_value)
        f = lambda d: d.date()
        python_date = [f(d) for d in data['date']]
        amounts = [int(a) for a in data['amounts']]
        result = xirr(python_date, amounts)
        # x = {'date':python_date, 'amounts':amounts}
        # print(pd.DataFrame.from_dict(x))
        # print("XIRR:", result)
        return result