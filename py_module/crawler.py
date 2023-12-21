from py_module.config import Configuration

import requests
from bs4 import BeautifulSoup
import os
import time
import pandas as pd

class Crawler(object):

    def __init__(self):
        self.config_obj = Configuration()
    
    def web_crawler_13F(self):

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
                form_type = cells[3].text.strip()
                date_filed = cells[4].text.strip()
                filing_id = cells[5].text.strip()
                name = cells[6].text.strip()
                
                holdings_urls[(quarter, form_type)] = quarterly_link
                # 將資料組合成字典並添加到資料列表中
                data.append({
                    'QUARTER': quarter,
                    'HOLDINGS': holdings,
                    'VALUE ($000)': value,
                    'FORM TYPE': form_type,
                    'DATE FILED': date_filed,
                    'FILING ID': filing_id,
                    'NAME': name
                })
            # hedge_fund_data = pd.DataFrame(data)
            # output_folder = self.config_obj.assets_hedge_fund_data
            # file_name = "hedge_fund_portfolio_filings_" + str(idx+1) + "_" + "-".join(soup.title.string.split()) + ".csv"
            # hedge_fund_data.to_csv(os.path.join(output_folder, file_name), index=False)
                

            url = 'https://13f.info/data/13f/000143499723000004'

            # 添加相应的请求头信息
            headers = {
                'Accept': 'application/json, text/javascript, */*; q=0.01',
                'Referer': 'https://13f.info/13f/000143499723000004-sir-capital-management-l-p-q3-2023',
                'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                'Sec-Ch-Ua-Mobile': '?0',
                'Sec-Ch-Ua-Platform': '"Windows"',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'X-Requested-With': 'XMLHttpRequest'
            }

            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                data = response.json()  # 解析 JSON 格式的数据
                print(data)  # 输出获取到的数据
            else:
                print("无法获取数据")



            # for (holding_quarter, holding_form_type), holding_url in holdings_urls.items():
            #     # print("時間:", holding_quarter, " 網址:", holding_url)

            #     holding_response = requests.get(holding_url, headers = headers)
            #     time.sleep(10)
            #     holding_soup = BeautifulSoup(holding_response.text, "html.parser")
            #     # print("連線網址：", holding_response.url, "連線狀況：", holding_response.status_code, "網頁Title:", holding_soup.title.string)
            #     print("Title:", holding_soup.title.string)

            #     holding_table = holding_soup.find('table', id_="filingAggregated")
            #     if holding_table:
            #         print(holding_table.text)
            #     else:
            #         print("未找到表格")
            #         print(holding_table.text)
            #     # 初始化空的資料列表
            #     holdings_data = []

            #     # 遍歷表格的每一列（排除表頭）
            #     for row in holding_table.find_all('tr')[1:]:
            #         '''
            #         get holdings表格資料
            #         '''
            #         # 找到列中的每個資料單元格
            #         cells = row.find_all('td')
                                    
            #         # 提取所需欄位的資料
            #         sym = cells[0].text.strip()
            #         issuer_name = cells[1].text.strip()
            #         cl = cells[2].text.strip()
            #         cusip = cells[3].text.strip()
            #         value = cells[4].text.strip()
            #         percent = cells[5].text.strip()
            #         shares = cells[6].text.strip()
            #         principal = cells[7].text.strip()
            #         option_type = cells[8].text.strip()

            #         holdings_data.append({
            #             'SYM': sym,
            #             'ISSUER NAME': issuer_name,
            #             'CL': cl,
            #             'CUSIP': cusip,
            #             'VALUE ($000)': value,
            #             '%': percent,
            #             'SHARES': shares,
            #             'PRINCIPAL': principal,
            #             'OPTION TYPE': option_type,
            #         })
            #     hedge_fund_holdings_data = pd.DataFrame(holdings_data)
            #     print(hedge_fund_holdings_data)
            #     output_folder = os.path.join(self.config_obj.assets_holdings_data, name)
            #     if not os.path.exists(output_folder):
            #         os.makedirs(output_folder)
            #     file_name = "holdings_data_" + "-".join(holding_soup.title.string.split()) + "_" + holding_form_type + ".csv"
            #     hedge_fund_holdings_data.to_csv(os.path.join(output_folder, file_name), index=False)

            # time.sleep(5)

            

        