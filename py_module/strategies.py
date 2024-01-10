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
        self.hedge_fund_portfolio_table = 'US_DB.dbo.HEDGE_FUND_PORTFOLIO'
        self.holdings_data_table = 'US_DB.dbo.HOLDINGS_DATA'
        self.us_stock_info_table = 'US_DB.dbo.USStockInfo'
        self.us_stock_price_table = 'US_DB.dbo.USStockPrice'

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
        query = self.create_query_hedge_fund_data()
        data = self.sql_execute(query)
        data = pd.DataFrame(data)
        print(data)

        

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
    
    def create_query_hedge_fund_data(self):
    
        data_table = self.hedge_fund_portfolio_table
        query = '''(SELECT * FROM {} WITH(NOLOCK))'''.format(data_table)
        return query


    