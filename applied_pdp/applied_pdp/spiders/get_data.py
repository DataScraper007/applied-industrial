import hashlib
import json
import os.path
from urllib.parse import urlencode

import pandas as pd
import scrapy
import pymysql
from scrapy.cmdline import execute

from applied_pdp.items import AppliedPdpItem


class GetDataSpider(scrapy.Spider):
    name = "get_data"

    def __init__(self, start_id, end_id):
        super().__init__()
        self.page_save = r"C:\Nirmal\R&D\page_save\applied"
        DB_CONFIG = {
            "host": "localhost",
            "user": "root",
            "password": "actowiz",
            "database": "applied_com"
        }
        self.connection = pymysql.connect(**DB_CONFIG)
        self.cursor = self.connection.cursor()
        self.cookies = None
        self.header = None
        self.start_id = start_id
        self.end_id = end_id

    def start_requests(self):
        self.cursor.execute(f"SELECT id, url FROM links where id between {self.start_id} and {self.end_id} and status='pending'")
        results = self.cursor.fetchall()

        self.cookies = {
            'JSESSIONID': '693FEDE21DFEB5E8494C845674468A8A.accstorefront-df7b7f46d-8j9jl',
            'punchout': 'false',
            'applied-cart': '29f0fbcd-da78-4713-b971-0fdbda55f0a0',
            'ROUTE': '.accstorefront-df7b7f46d-8j9jl',
            '_gcl_au': '1.1.1699827589.1736232827',
            '_gid': 'GA1.2.1391338562.1736232829',
            'OptanonAlertBoxClosed': '2025-01-07T06:53:53.485Z',
            'gbi_visitorId': 'cm5m47ghm00013573n97qskvz',
            '_clck': '5kd2l2%7C2%7Cfsd%7C0%7C1833',
            'hubspotutk': 'a5fe92b84bbeadf6c25ef8205329778d',
            '__hssrc': '1',
            'OptanonConsent': 'isGpcEnabled=0&datestamp=Tue+Jan+07+2025+17%3A07%3A05+GMT%2B0530+(India+Standard+Time)&version=202405.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=821ca82a-19aa-4d80-9556-de5a5aeb9b21&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=C0004%3A1%2CC0003%3A1%2CC0002%3A1%2CC0001%3A1&intType=1&geolocation=GB%3BENG&AwaitingReconsent=false',
            'cf_clearance': 'shuGNO0Jaisnksq0bAedUV1WEkVJFyHDsLG9ygyGuRI-1736249835-1.2.1.1-5ku_fAO.TVaVdpRGHKigmKhn2gHN5sHoqsqh3ufLwwTKkhAp3b.Qs.BuA3Zype0hsvtFpYmKGTaHAs8GyVO9wNI2mOq1cw.DEpAzxXEt.pHnnjeDOr8agncJAdGBKM4usLxGqex.9vWZb7BJR_knoYvtrXSXjetApgTo8RyWmm6H5m.tja8nZlCl.Odq.vWN_PWCsXpPg3ynbP45lM23NJAau2hfIGQCTjh0alssr2KpHupQb53tA09iq1hKvDTRohUsDkULX8DH5fCkM7gLvEfcGTEok.OFaslr1yP5IS4c0mR_sTpA5wY7rFOlAksV8oj_O4Ko3Jm866pj0vRat45mq7yZ8RVbsg_d0JLETcjvWHSUzf4LhCl4onjrRFNrdiXxjkYBJ9mW_W4.WiZvEWDiXGdXUQGm95scsBZ5yp5ntSmwoFj77nnQ18fCVQcB',
            '_ga': 'GA1.2.1236185319.1736232828',
            '_ga_ZT5EWHFHHL': 'GS1.1.1736240441.2.1.1736249836.60.0.1107253546',
            '_uetsid': 'df6780d0ccd511efb38d1b6bcdaed4fb',
            '_uetvid': 'a3500210757d11efa554158284f54ca3',
            '__hstc': '195597939.a5fe92b84bbeadf6c25ef8205329778d.1736240445873.1736243666767.1736249837869.3',
            '_clsk': '7e0s9f%7C1736257169364%7C1%7C1%7Co.clarity.ms%2Fcollect',
        }

        self.headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'no-cache',
            # 'cookie': 'JSESSIONID=693FEDE21DFEB5E8494C845674468A8A.accstorefront-df7b7f46d-8j9jl; punchout=false; applied-cart=29f0fbcd-da78-4713-b971-0fdbda55f0a0; ROUTE=.accstorefront-df7b7f46d-8j9jl; _gcl_au=1.1.1699827589.1736232827; _gid=GA1.2.1391338562.1736232829; OptanonAlertBoxClosed=2025-01-07T06:53:53.485Z; gbi_visitorId=cm5m47ghm00013573n97qskvz; _clck=5kd2l2%7C2%7Cfsd%7C0%7C1833; hubspotutk=a5fe92b84bbeadf6c25ef8205329778d; __hssrc=1; OptanonConsent=isGpcEnabled=0&datestamp=Tue+Jan+07+2025+17%3A07%3A05+GMT%2B0530+(India+Standard+Time)&version=202405.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=821ca82a-19aa-4d80-9556-de5a5aeb9b21&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=C0004%3A1%2CC0003%3A1%2CC0002%3A1%2CC0001%3A1&intType=1&geolocation=GB%3BENG&AwaitingReconsent=false; cf_clearance=shuGNO0Jaisnksq0bAedUV1WEkVJFyHDsLG9ygyGuRI-1736249835-1.2.1.1-5ku_fAO.TVaVdpRGHKigmKhn2gHN5sHoqsqh3ufLwwTKkhAp3b.Qs.BuA3Zype0hsvtFpYmKGTaHAs8GyVO9wNI2mOq1cw.DEpAzxXEt.pHnnjeDOr8agncJAdGBKM4usLxGqex.9vWZb7BJR_knoYvtrXSXjetApgTo8RyWmm6H5m.tja8nZlCl.Odq.vWN_PWCsXpPg3ynbP45lM23NJAau2hfIGQCTjh0alssr2KpHupQb53tA09iq1hKvDTRohUsDkULX8DH5fCkM7gLvEfcGTEok.OFaslr1yP5IS4c0mR_sTpA5wY7rFOlAksV8oj_O4Ko3Jm866pj0vRat45mq7yZ8RVbsg_d0JLETcjvWHSUzf4LhCl4onjrRFNrdiXxjkYBJ9mW_W4.WiZvEWDiXGdXUQGm95scsBZ5yp5ntSmwoFj77nnQ18fCVQcB; _ga=GA1.2.1236185319.1736232828; _ga_ZT5EWHFHHL=GS1.1.1736240441.2.1.1736249836.60.0.1107253546; _uetsid=df6780d0ccd511efb38d1b6bcdaed4fb; _uetvid=a3500210757d11efa554158284f54ca3; __hstc=195597939.a5fe92b84bbeadf6c25ef8205329778d.1736240445873.1736243666767.1736249837869.3; _clsk=7e0s9f%7C1736257169364%7C1%7C1%7Co.clarity.ms%2Fcollect',
            'pragma': 'no-cache',
            'priority': 'u=0, i',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-arch': '"x86"',
            'sec-ch-ua-bitness': '"64"',
            'sec-ch-ua-full-version': '"131.0.6778.205"',
            'sec-ch-ua-full-version-list': '"Google Chrome";v="131.0.6778.205", "Chromium";v="131.0.6778.205", "Not_A Brand";v="24.0.0.0"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-model': '""',
            'sec-ch-ua-platform': '"Windows"',
            'sec-ch-ua-platform-version': '"10.0.0"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        }

        for result in results:
            index_id, url = result
            hash = hashlib.sha256(url.encode()).hexdigest()
            # if os.path.exists(f"{self.page_save}/{hash}.html"):
            #     yield scrapy.Request(
            #         url=f"file:///{self.page_save}/{hash}.html",
            #         cb_kwargs= {'url': url}
            #     )
            #     break
            yield scrapy.Request(
                url=f"https://api.scrape.do/?token=<apitoken>&url={url}",
                cookies=self.cookies,
                headers=self.headers,
                cb_kwargs={'url': url, 'index_id':index_id},
                dont_filter=True
            )


    def parse(self, response, **kwargs):
        XPATHS = {
            "mpn": '//div[contains(@itemprop,"mpn")]/text()',
            "product_name": '//div[@class="dc-product-name"]/text()',
            "product_code": '//input[@name="productCode"]/@value',
            "product_description": '//div[@class="features" and @itemprop="description"]/ul/li/text()',
            "product_image": '//div[contains(@class,"product-image-desktop")]//div[@class="dc-pdp-image-main"]/img/@data-src',
            "product_price": '//span[@itemprop="price"]/text()',
            "csrf_token": "//input[@name='CSRFToken']/@value"
        }

        item = AppliedPdpItem()
        item['index_id'] = kwargs['index_id']
        item['url'] = kwargs['url']
        item['mpn'] = response.xpath(XPATHS["mpn"]).get()
        item['product_name'] = response.xpath(XPATHS["product_name"]).get().replace('\n', '').strip()
        item['product_code'] = response.xpath(XPATHS["product_code"]).get()
        item['product_image'] = response.xpath(XPATHS["product_image"]).get()
        item['categories'] = self.extract_categories(response)
        item['product_specs'] = self.extract_product_specs(response)
        csrf_token = response.xpath(XPATHS['csrf_token']).get()
        
        params = {
            'productCodes': item['product_code'],
        }
        yield scrapy.Request(
            url="https://api.scrape.do/?token=<apitoken>&url=https://www.applied.com/getprices?" + urlencode(
                params),
            headers=self.headers,
            cookies=self.cookies,
            callback=self.parse_product_price,
            cb_kwargs={'item': item, 'csrf_token': csrf_token},
            dont_filter=True
        )

    def extract_categories(self, response):
        categories = []
        for category in response.xpath('//ul[@class="dc-breadcrumbs"]/li'):
            cat = category.xpath('./a/span/text()').get() or category.xpath('./a/text()').get()
            if cat:
                categories.append(cat)

        return ' | '.join(categories) if categories else "N/A"

    def extract_product_specs(self, response):
        try:
            df = pd.read_html(response.text)[0]
            df = df.fillna('')
            return dict(zip(df[0], df[1]))
        except Exception:
            return {}

    def parse_product_price(self, response, **kwargs):
        try:
            json_data = json.loads(response.text)
            kwargs['item']['product_price'] = json_data['responseObject'][kwargs['item']['product_code']].get(
            'priceData').get('value', 'N/A')
        except:
            kwargs['item']['product_price'] = 'N/A'

        data = {
            'quantity': '1',
            'productCodes': kwargs['item']['product_code'],
            'page': 'SAC',
            'productCode': kwargs['item']['product_code'],
            'CSRFToken': kwargs['csrf_token'],
        }

        yield scrapy.FormRequest(
            url="https://api.scrape.do/?token=<apitoken>&url=https://www.applied.com/inventory/status",
            method='POST',
            headers=self.headers,
            cookies=self.cookies,
            formdata= data,
            callback=self.parse_availability,
            cb_kwargs={'item': kwargs['item']},
            dont_filter=True
        )

    def parse_availability(self, response, **kwargs):
        json_data = json.loads(response.text)
        availability = json_data['responseObject'][0]['status']
        kwargs['item']['availability'] = 'In stock' if availability in ['In Stock', 'Ready To Ship'] else 'Out of stock'

        yield kwargs['item']

if __name__ == '__main__':
    execute(f'scrapy crawl {GetDataSpider.name} -a start_id=1 -a end_id=100'.split())
