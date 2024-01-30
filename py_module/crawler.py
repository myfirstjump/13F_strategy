from py_module.config import Configuration

import requests
from bs4 import BeautifulSoup
import os
import time
import pymssql
import pandas as pd
import numpy as np

class Crawler(object):

    def __init__(self):
        self.config_obj = Configuration()
        
    
    def web_crawler_13F(self):


        missing_counter = 0
        missing_info = []
        # conn = pymssql.connect(host='localhost', user = 'stock_search', password='1qazZAQ!', database='STOCK_SKILL_DB')
        conn = pymssql.connect(host='localhost', user = 'myfirstjump', password='myfirstjump', database='US_DB')
        cursor = conn.cursor(as_dict=True)
        # hedge_data = [()] #要塞進去資料，裡面是資料需要是tuple格式，外面用list包起來
        # holdings_tuple = [()]

        urls = self.config_obj.hedge_fund_urls
        headers = {
            'user-agent': 'Mozilla/5.0'
        }
        parameters = {}
        print("預計爬取共", len(urls), "支基金資料")

        for idx, name in enumerate(urls):
            print("======================= 第", idx+1, "支基金:", name, " ======================= ")

            response = requests.get(urls[name], headers = headers)
            print("連線網址：", response.url)
            print("連線狀況：", response.status_code)
            # print("連線文字：", response.text)
            soup = BeautifulSoup(response.text, "html.parser")
            print("網頁Title:", soup.title.string)

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
                    'FILING ID': filing_id,
                    'HEDGE FUND': name,
                })
            hedge_fund_data = pd.DataFrame(data)
            hedge_fund_data = hedge_fund_data.replace(np.nan, None) # 部分資料為pandas nan，需轉為python None
            output_folder = self.config_obj.assets_hedge_fund_data
            file_name = "hedge_fund_portfolio_filings_" + str(idx+1) + "_" + "-".join(soup.title.string.split()) + ".csv"
            hedge_fund_data.to_csv(os.path.join(output_folder, file_name), index=False)

            # hedge_tuple = [tuple(row) for row in hedge_fund_data.values]
            # print(hedge_tuple)
            # cursor.executemany(
            #     """INSERT INTO [US_DB].[dbo].[HEDGE_FUND_PORTFOLIO]
            #     (
            #     [QUARTER]
            #     ,[HOLDINGS]
            #     ,[VALUE]
            #     ,[TOP_HOLDINGS]
            #     ,[FORM_TYPE]
            #     ,[DATE_FILED]
            #     ,[FILING_ID]
            #     ,[HEDGE_FUND]
            #     ) 
            #     VALUES(%s,%d,%d,%s,%s,%s,%s,%s)"""
            #     , hedge_tuple
            # )
            # conn.commit()
                
            count = 0
            for (quarter, form_type, filing_id), quarterly_link in holdings_urls.items():
                
                count += 1
                network_source = quarterly_link.split('-')[0].split('/')[-1]
                network_source = "https://13f.info/data/13f/"  + network_source
                print("基金", idx+1, " 資料", quarter, form_type," crawling...",)
                
                # 添加相应的请求头信息
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
                    data = holdings_response.json()  # 解析 JSON 格式的数据
                    
                else:
                    missing_counter += 1
                    missing_info.append("基金" + str(idx+1) + " 資料" + str(quarter) + str(form_type))
                    print("无法获取数据")

                holdings_data = data['data']
                holdings_data = pd.DataFrame(holdings_data, columns = ['SYM','ISSUER NAME','CL','CUSIP','VALUE ($000)','%','SHARES','PRINCIPAL','OPTION TYPE',])
                holdings_data = holdings_data.replace(np.nan, None) # 部分資料為pandas nan，需轉為python None
                holdings_data['HEDGE FUND'] = name
                holdings_data['QUARTER'] = quarter
                holdings_data['FORM TYPE'] = form_type
                holdings_data['FILING_ID'] = filing_id

                output_folder = os.path.join(self.config_obj.assets_holdings_data, name)
                if not os.path.exists(output_folder):
                    os.makedirs(output_folder)
                file_name = "holdings_data_" + "-".join(str(quarter).split()) + "_" + form_type + "_" + str(count) + ".csv"
                holdings_data.to_csv(os.path.join(output_folder, file_name), index=False)

                # holdings_tuple = [tuple(row) for row in holdings_data.values]

                # cursor.executemany(
                #     """INSERT INTO [US_DB].[dbo].[HOLDINGS_DATA]
                #     (
                #     [SYM]
                #     ,[ISSUER_NAME]
                #     ,[CL]
                #     ,[CUSIP]
                #     ,[VALUE]
                #     ,[Percentile]
                #     ,[SHARES]
                #     ,[PRINCIPAL]
                #     ,[OPTION_TYPE]
                #     ,[HEDGE_FUND]
                #     ,[QUARTER]
                #     ,[FORM_TYPE]
                #     ,[FILING_ID]
                #     ) 
                #     VALUES(%s,%s,%s,%s,%d,%d,%d,%s,%s,%s,%s,%s,%s)"""
                #     , holdings_tuple
                # )
                # conn.commit()
            time.sleep(10)
        print("Missing holdings data number:", missing_counter)
        print("Missing holdings info:", missing_info)





        