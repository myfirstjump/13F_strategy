from py_module.config import Configuration

import requests
from bs4 import BeautifulSoup
import os
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
            print("第", idx+1, "支基金:", name,)

            response = requests.get(urls[name], headers = headers)
            print("連線網址：", response.url)
            print("連線狀況：", response.status_code)
            # print("連線文字：", response.text)
            soup = BeautifulSoup(response.text, "html.parser")
            print("網頁Title:", soup.title.string)
            

            table = soup.find('table')

            # 初始化空的資料列表
            data = []

            # 遍歷表格的每一列（排除表頭）
            for row in table.find_all('tr')[1:]:
                # 找到列中的每個資料單元格
                cells = row.find_all('td')
                quarterly_link = "https://13f.info" + row.find('a')['href']
                print(quarterly_link)
                
                # 提取所需欄位的資料
                quarter = cells[0].text.strip()
                holdings = cells[1].text.strip()
                value = cells[2].text.strip()
                form_type = cells[3].text.strip()
                date_filed = cells[4].text.strip()
                filing_id = cells[5].text.strip()
                name = cells[6].text.strip()
                
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
            # output_folder = self.config_obj.output_folder
            # file_name = "hedge_fund_portfolio_filings_" + str(idx+1) + "_" + "-".join(soup.title.string.split()) + ".csv"
            # hedge_fund_data.to_csv(os.path.join(output_folder, file_name), index=False)



            

        