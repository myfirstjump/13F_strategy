from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order
from threading import Timer

class IBApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)

    def error(self, reqId, errorCode, errorString):
        print(f"Error: {reqId}, {errorCode}, {errorString}")

    def nextValidId(self, orderId):
        self.nextOrderId = orderId
        self.start()

    def start(self):
        # 查詢帳戶資金
        self.reqAccountSummary(1, "All", "TotalCashValue,NetLiquidation")
        
        # 建立訂單
        contract = Contract()
        contract.symbol = "AAPL"
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"
        
        order = Order()
        order.action = "BUY"
        order.orderType = "MKT"
        order.totalQuantity = 10
        
        self.placeOrder(self.nextOrderId, contract, order)
        
        # 設置5秒後斷開連接
        Timer(5, self.stop).start()

    def stop(self):
        self.reqAccountSummary(2, "All", "TotalCashValue,NetLiquidation")
        self.done = True
        self.disconnect()

    def accountSummary(self, reqId, account, tag, value, currency):
        print(f"Account Summary. ReqId: {reqId}, Account: {account}, Tag: {tag}, Value: {value}, Currency: {currency}")