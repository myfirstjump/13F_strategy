from py_module.config import Configuration

import pymssql
import pandas as pd
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
        self.holdings_data_table = 'US_DB.dbo.HOLDINGS_DATA'
        self.us_stock_info_table = 'US_DB.dbo.USStockInfo'
        self.us_stock_price_table = '[US_DB].[dbo].[USStockPrice]'

    def main_strategy_flow(self):
        '''
        美股13F投資策略回測程序
            0. 設定需求資料物件。(initial_capital, scaling_in, scaling_out, realized_profit_loss, trading_time)
            1. 連結至Database，獲取hedge fund清單。
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
        market_value_dict = {}
        quarter_scaling_in_dict = {}
        quarter_scaling_out_dict = {}
        summary_data = []

        '''1'''
        query = self.create_query_data_table(self.hedge_fund_portfolio_table) #'''TBD: 處理Restatement數據'''
        fund_data = self.sql_execute(query)
        fund_data = pd.DataFrame(fund_data)
        hedge_fund_list = fund_data['HEDGE_FUND'].unique()
        print("總共包含{}個對沖基金資料".format(len(hedge_fund_list)))
        print('Hedge Funds:', )
        print(hedge_fund_list)

        query = self.get_all_price_date(self.us_stock_price_table)
        all_date_list = self.sql_execute(query)
        all_date_list = pd.DataFrame(all_date_list)['date'].values
        # 對日期進行排序
        sorted_dates = sorted(all_date_list)
        # 找到最大的日期
        max_date = max(sorted_dates)

        for idx, hedge_fund in enumerate(hedge_fund_list):

            
            ### Calculation parameters
            previous_data = None
            market_value_list = []
            quarter_scaling_in_list = []
            quarter_scaling_out_list = []
            xirr_calculate_dict = {'date':[], 'amounts':[]}

            data = fund_data[fund_data['HEDGE_FUND']==hedge_fund][['QUARTER','DATE_FILED']]
            quarters_list = data['QUARTER'].values #'''TBD: 處理Restatement數據'''
            date_list = data['DATE_FILED'].values #'''TBD: 依據13F公布時間入場'''
            # print(quarters_list)
            # print(date_list)
            print(" === === === 第{}個對沖基金：{}，包含{}個季度資料。 === === === ".format(idx+1, hedge_fund, len(quarters_list)))
            
            scaling_in = []
            scaling_out = []
            realized_profit_loss = []
            trading_time = []
            for idx_q, quarter in enumerate(quarters_list):
                hedge_fund_data = {'date': None, '市值': None, '加碼': None, '減碼': None, 'XIRR': None}
                    

                holdings_time = date_list[idx_q]
                # print("     第{}個季度：{}，時間為{}".format(idx_q+1, quarter, holdings_time))
                query = self.create_query_holdings(hedge_fund, quarter)
                holdings_data = self.sql_execute(query)
                holdings_data = pd.DataFrame(holdings_data)
                '''3'''
                sym_str = holdings_data['SYM'].dropna().values
                if len(sym_str) == 1:
                    sym_str = str(sym_str)
                    sym_str = sym_str.replace('[', '(')
                    sym_str = sym_str.replace(']', ')')
                else:
                    sym_str = tuple(sym_str)
                query = self.create_query_get_open_price(sym_str, holdings_time, hedge_fund, quarter)
                # print(query)
                price_data = self.sql_execute(query)
                price_data = pd.DataFrame(price_data)
                # print("Price_data:")
                # print(price_data)
                
                
                if len(price_data) == 0:
                    print('SYM：{}，無對應Price資料'.format(sym_str))
                    break
                else:
                    pass
                '''4'''
                market_value = sum(price_data['Open'] * price_data['SHARES'])
                market_value_list.append(market_value)
                if idx_q > 0: #扣除第一季，每季要計算的內容
                    scaling_in, scaling_out, scaling_even = self.shares_difference_between_quarters(previous_data, price_data)
                    scaling_in_sum = sum([i for i in  scaling_in.values()])
                    scaling_out_sum = sum([i for i in  scaling_out.values()])
                    # print("Shares Increased:", scaling_in)
                    # print("Shares Decreased:", scaling_out)
                    # print("Shares Unchanged:", scaling_even)
                    quarter_scaling_in_list.append(scaling_in_sum)
                    quarter_scaling_out_list.append(scaling_out_sum)
                    xirr_calculate_dict['date'].append(holdings_time)
                    xirr_calculate_dict['amounts'].append(-(scaling_in_sum - scaling_out_sum))
                else: #第一季要計算的內容
                    scaling_in_sum = 0
                    scaling_out_sum = 0
                    xirr_calculate_dict['date'].append(holdings_time)
                    xirr_calculate_dict['amounts'].append(-market_value)
                
                temp_xirr_calculate_dict = copy.deepcopy(xirr_calculate_dict)
                xirr = self.calculate_XIRR(temp_xirr_calculate_dict, holdings_time, market_value)
                # if idx_q == (len(quarters_list)-1): #最後一季，計算
                #     xirr_calculate_dict['date'].append(holdings_time)
                #     xirr_calculate_dict['amounts'].append(market_value)
                
                previous_data = price_data.copy()

                hedge_fund_data = {'date': holdings_time, '市值': market_value, '加碼': scaling_in_sum, '減碼': scaling_out_sum, 'XIRR':xirr}
                summary_data.append({'hedge_fund': hedge_fund, **hedge_fund_data})

            market_value_dict[hedge_fund] = market_value_list
            quarter_scaling_in_dict[hedge_fund] = quarter_scaling_in_list
            quarter_scaling_out_dict[hedge_fund] = quarter_scaling_out_list

        summary_table = pd.DataFrame(summary_data)
        # print('市值：', market_value_dict)
        # print('每季加碼：', quarter_scaling_in_dict)
        # print('每季減碼：', quarter_scaling_out_dict)
        # print('年化報酬率：', final_xirr)
        print(summary_table)
        path = os.path.join(self.config_obj.backtest_summary, str(datetime.datetime.now()).split()[0] + '_summary_table.csv')
        print(path)
        summary_table.to_csv(path, index=False)

    def sql_execute(self, query):

        conn = pymssql.connect(host='localhost', user = 'myfirstjump', password='myfirstjump', database='US_DB')
        cursor = conn.cursor(as_dict=True)
        cursor.execute(query)
        # data = [row for row in cursor]
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
        query = '''SELECT * FROM {} WITH(NOLOCK) WHERE [FORM_TYPE] = '13F-HR' ORDER BY [DATE_FILED] ASC'''.format(data_table)
        return query
    
    def create_query_holdings(self, fund, quarter):
        '''
        依據fund和quarter篩選holdings資料表的query語句
        
        '''
        data_table = self.holdings_data_table
        query = '''SELECT * FROM {} WITH(NOLOCK) 
        WHERE [HEDGE_FUND] = '{}' 
        AND [QUARTER] = '{}' 
        AND [OPTION_TYPE] IS NULL
        AND SUBSTRING([CUSIP], 7, 2) = '10'
        '''.format(data_table, fund, quarter)
        return query

    def create_query_get_open_price(self, SYMs_tuple, date, fund, quarter):
        '''
        依據holdings去查表price，透過stock_id(即holdings表中的SYM) join兩張表格，並加入SHARES資訊至price表。
        '''
        price_table = self.us_stock_price_table
        holdings_table = self.holdings_data_table
        query = ''' SELECT DISTINCT tb_price.[date], tb_price.[stock_id], tb_price.[Open], tb_holdings.[SHARES]
            FROM {} tb_price WITH(NOLOCK)
			RIGHT JOIN {} tb_holdings WITH(NOLOCK) on tb_price.stock_id = tb_holdings.SYM
			WHERE tb_price.[stock_id] IN {}
            AND tb_price.[date] = '{}'
            AND tb_holdings.[QUARTER] = '{}' 
            AND tb_holdings.[OPTION_TYPE] IS NULL 
            AND tb_holdings.[HEDGE_FUND] = '{}' 
            AND tb_holdings.[FORM_TYPE] = '13F-HR'
            AND tb_holdings.[PRINCIPAL] IS NULL 
            '''.format(price_table, holdings_table, SYMs_tuple, date, quarter, fund)
        return query

    def shares_difference_between_quarters(self, previous_data, current_data):
        scaling_in = {}
        scaling_out = {}
        scaling_even = {}

        # 將兩個資料表合併
        merged_data = current_data.merge(previous_data, on=['stock_id'], suffixes=('_current', '_previous'))

        # 計算持股數量變化
        merged_data['shares_change'] = merged_data['SHARES_current'] - merged_data['SHARES_previous']

        # print(merged_data)
        # 根據持股數量變化分類
        for index, row in merged_data.iterrows():
            stock_id = row['stock_id']
            shares_change = row['shares_change']
            Open_current = row['Open_current']

            if shares_change > 0:
                scaling_in[stock_id] = shares_change * Open_current
            elif shares_change < 0:
                scaling_out[stock_id] = abs(shares_change) * Open_current
            else:
                scaling_even[stock_id] = 0

        return scaling_in, scaling_out, scaling_even
        

    def get_all_price_date(self, price_table):

        query = '''
        SELECT DISTINCT [date]
        FROM {}
        '''.format(price_table)
        return query
    
    def calculate_XIRR(self, data, holdings_time, market_value):
        data['date'].append(holdings_time)
        data['amounts'].append(market_value)
        result = xirr(data['date'], data['amounts'])
        return result