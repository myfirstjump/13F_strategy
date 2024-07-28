from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order
from threading import Timer

class IBApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self) # 初始化客戶端
        self.nextOrderId = None

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
        print(f"Error: {reqId}, {errorCode}, {errorString}, {advancedOrderRejectJson}")

    def nextValidId(self, orderId):
        self.nextOrderId = orderId
        self.start()

    def accountSummary(self, reqId, account, tag, value, currency):
        print(f"Account Summary. ReqId: {reqId}, Account: {account}, Tag: {tag}, Value: {value}, Currency: {currency}")

    def start(self):
        # 查詢帳戶資金
        self.reqAccountSummary(1, "All", "TotalCashValue,NetLiquidation")

        # 建立合約
        contract = Contract()
        contract.symbol = "AAPL"
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"
        
        # 建立訂單
        order = Order()
        order.action = "BUY"
        # order.orderType = "MKT" # 市價單
        order.orderType = "LMT" # 限價單
        order.lmtPrice = 150.00  # 設定限價
        order.totalQuantity = 10
        
        # 下單
        self.placeOrder(self.nextOrderId, contract, order)
        
        # 設置5秒後斷開連接
        Timer(5, self.stop).start()

    def stop(self):
        self.done = True
        self.disconnect()

def main():
    app = IBApp()
    app.connect("127.0.0.1", 4001, clientId=0) # IB gateway
    # app.connect("127.0.0.1", 7496, clientId=0) # TWS
    app.run()

if __name__ == "__main__":
    main()
