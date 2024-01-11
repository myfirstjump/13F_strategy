import pymssql
import pandas as pd
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
        self.hedge_fund_portfolio_table = '[US_DB].[dbo].[HEDGE_FUND_PORTFOLIO]'
        self.holdings_data_table = 'US_DB.dbo.HOLDINGS_DATA'
        self.us_stock_info_table = 'US_DB.dbo.USStockInfo'
        self.us_stock_price_table = '[US_DB].[dbo].[USStockPrice]'

    def main_strategy_flow(self):
        '''
        美股13F投資策略回測程序
            1. 連結至Database，獲取hedge fund清單。
            開始各hedge fund回測
                2. 確認起始年份，設定需求資料物件。(initial_capital, scaling_in, scaling_out, realized_profit_loss, trading_time)
                開始各年份迴圈
                    3. records SYM查表找尋price table open值
                    4. 計算shares差異、市值差異(加碼、減碼)
                    5. 紀錄交易時間、應收付損益
                    6. 計算最大回撤dd
                6.計算年化報酬
                7.計算最終獲利
                8.繪製資金曲線圖
                9.各時間點加碼、減碼；淨加碼、減碼
        '''

        '''1'''
        query = self.create_query_data_table(self.hedge_fund_portfolio_table)
        fund_data = self.sql_execute(query)
        fund_data = pd.DataFrame(fund_data)
        print("總共包含{}個對沖基金資料".format(len(fund_data['HEDGE_FUND'].unique())))
        
        for idx, hedge_fund in enumerate(fund_data['HEDGE_FUND'].unique()):

            '''2'''
            data = fund_data[fund_data['HEDGE_FUND']==hedge_fund][['QUARTER','DATE_FILED']]
            quarters_list = data['QUARTER'].values
            date_list = data['DATE_FILED'].values
            # print(quarters_list)
            # print(date_list)
            print(" === 第{}個對沖基金：{}，包含{}個季度資料。 ===".format(idx+1, hedge_fund, len(quarters_list)))
            initial_capital = 0
            scaling_in = []
            scaling_out = []
            realized_profit_loss = []
            trading_time = []
            for idx_q, quarter in enumerate(quarters_list):
                holdings_time = date_list[idx_q]
                print("     第{}個季度：{}，時間為{}".format(idx_q+1, quarter, holdings_time))
                query = self.create_query_holdings(hedge_fund, quarter)
                holdings_data = self.sql_execute(query)
                holdings_data = pd.DataFrame(holdings_data)

                '''3'''
                SYMs = tuple(holdings_data['SYM'].dropna().values) # SYM有空值先去除dropna()
                query = self.create_query_get_open_price(SYMs, holdings_time, hedge_fund, quarter)
                price_data = self.sql_execute(query)
                price_data = pd.DataFrame(price_data)
                if len(price_data) == 0:
                    print('SYM：{}，無對應Price資料'.format(SYMs))
                    break
                else:
                    if idx_q == 0:
                        initial_capital = sum(price_data['Open'] * price_data['SHARES'])
                        break
            print('初始資金：', initial_capital)


                    



        

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
        query = '''SELECT * FROM {} WITH(NOLOCK) ORDER BY [DATE_FILED] ASC'''.format(data_table)
        return query
    
    def create_query_holdings(self, fund, quarter):
        '''
        依據fund和quarter篩選holdings資料表的query語句
        
        '''
        data_table = self.holdings_data_table
        query = '''SELECT * FROM {} WITH(NOLOCK) WHERE [HEDGE_FUND] = '{}' AND [QUARTER] = '{}' AND [OPTION_TYPE] IS NULL'''.format(data_table, fund, quarter)
        return query

    def create_query_get_open_price(self, SYMs_tuple, date, fund, quarter):
        '''
        依據holdings去查表price，透過stock_id(即holdings表中的SYM) join兩張表格，並加入SHARES資訊至price表。
        '''
        price_table = self.us_stock_price_table
        holdings_table = self.holdings_data_table
        query = ''' SELECT tb_price.[date], tb_price.[stock_id], tb_price.[Open], tb_holdings.[SHARES]
            FROM {} tb_price WITH(NOLOCK)
			RIGHT JOIN {} tb_holdings WITH(NOLOCK) on tb_price.stock_id = tb_holdings.SYM
			WHERE tb_price.[stock_id] IN {} AND tb_price.[date] = '{}'
            AND tb_holdings.[QUARTER] = '{}' AND tb_holdings.[OPTION_TYPE] IS NULL AND tb_holdings.[HEDGE_FUND] = '{}' '''.format(price_table, holdings_table, SYMs_tuple, date, quarter, fund)
        return query