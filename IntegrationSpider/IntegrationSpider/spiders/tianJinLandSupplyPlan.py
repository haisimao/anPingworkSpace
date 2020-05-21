# tianJinLandSupplyPlan
# -*- coding: utf-8 -*-
import datetime
import os
import random
import re
import string
import time
import traceback

import requests
from lxml import etree

import pymysql
from bs4 import BeautifulSoup
from scrapy import Request, FormRequest, Selector
from scrapy.spiders import CrawlSpider
import logging

from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
from urllib.request import unquote
import json
# from requests_html import HTMLSession

from IntegrationSpider.items import IntegrationPageItem
from IntegrationSpider.useragent import agent_list
from IntegrationSpider.utils_ import IntegrationException, getSesion, encrypt_md5
from SpiderTools.Tool import reFunction, getVariableName
from SpiderTools.tableAnalysis import htmlTableTransformer


class tianJinLandSupplyPlanSpider(CrawlSpider):
    name = 'tianJinLandSupplyPlan'
    # 配置更换cookies时间
    COOKIES_SWITCH_TIME = datetime.datetime.now()

    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super(tianJinLandSupplyPlanSpider, cls).__new__(cls)
            pathPage = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),'Logs/tianJinLandSupplyPlanPage.txt')
            cls.pathDetail = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))), 'data/天津规划和自然资源局_土地供应计划_天津.csv')
            cls.filePage = open(pathPage, 'w+')
            if os.path.exists(cls.pathDetail):
                cls.fileDetail = open(cls.pathDetail, 'a+')
            else:
                cls.fileDetail = open(cls.pathDetail, 'a+')
                with open(cls.pathDetail, 'a+') as fp:
                    fp.write("""年度,住房用地(含保障性住房用地商品住房用地)供应计划面积(公顷),商服用地供应计划面积(公顷),工矿仓储用地供应计划面积(公顷),基础设施及公益事业等划拨用地供应计划面积(公顷),爬取地址url,唯一标识,\n""")
        return cls.instance

    def __init__(self):
        super(tianJinLandSupplyPlanSpider, self).__init__()
        dispatcher.connect(self.CloseSpider, signals.spider_closed)
        self.targetUrl = 'http://218.69.100.10/newslist.aspx?id=CK530301'
        self.header = {'User-Agent': random.choice(agent_list)}
        self.reStr = '（）\w\.:： 。 \(\)〔〕㎡≤；，≥《》\-\/\%,、\.﹪㎡'

    def CloseSpider(self):
        '''
        关闭spider
        :return:
        '''
        self.filePage.close()
        self.fileDetail.close()
        self.log('爬虫正常关闭,At{}'.format(datetime.datetime.now()), level=logging.INFO)

    def CloseExceptionSpider(self):
        '''
        关闭spider
        :return:
        '''
        self.filePage.close()
        self.fileDetail.close()
        self.log('响应超时爬虫正常关闭,At{}'.format(datetime.datetime.now()), level=logging.ERROR)

    def start_requests(self):
        '''
        先拿到总页数, 按照优先级爬取, 并每十个请求换一个sessionID
        '''
        try:
            for page in range(1, 3):
                requests_data = data = {
'__VIEWSTATE': '''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sYDnvZHnq5njgJEKMjAxMy0wNy0wNGQCGg9kFgJmDxUEE25ld3MuYXNweD9pZD0zMDA0NTYATDIwMTPlubTlhajlm73kvY/miL/nlKjlnLDkvpvlupTorqHliJLjgJDovazoh6rljp/luILlm73lnJ/miL/nrqHlsYDnvZHnq5njgJEKMjAxMy0wNC0xNmQCGw9kFgJmDxUEE25ld3MuYXNweD9pZD0zMDA0NTQAmQHotK/lvbvmnY7lhYvlvLrlia/mgLvnkIblnKjlhajlm73kv53pmpzmgKflronlsYXlt6XnqIvlt6XkvZzkvJrorq7kuIrorrLor53nmoTnsr7npZ7vvIzokL3lrp7ku4rlubTkvY/miL/nlKjlnLDkvpvlupTjgJDovazoh6rljp/luILlm73lnJ/miL/nrqHlsYDnvZEuLi4KMjAxMi0wNy0wNmQCHA9kFgJmDxUEE25ld3MuYXNweD9pZD0zMDA0NTIATOWkqea0peW4gjIwMTHlubTluqblnJ/lnLDkvpvlupTorqHliJLjgJDovazoh6rljp/luILlm73lnJ/miL/nrqHlsYDnvZHnq5njgJEKMjAxMi0wMS0zMWQCHQ9kFgJmDxUEE25ld3MuYXNweD9pZD0zMDA0NTAAYeWkqea0peW4gjIwMDnlubTnu4/okKXmgKfmiL/lnLDkuqflvIDlj5HnlKjlnLDkvpvlupTorqHliJLjgJDovazoh6rljp/luILlm73lnJ/miL/nrqHlsYDnvZHnq5njgJEKMjAxMC0xMi0zMGQCHg9kFgJmDxUEE25ld3MuYXNweD9pZD0zMDA0NTEAYeWkqea0peW4gjIwMTDlubTnu4/okKXmgKfmiL/lnLDkuqflvIDlj5HnlKjlnLDkvpvlupTorqHliJLjgJDovazoh6rljp/luILlm73lnJ/miL/nrqHlsYDnvZHnq5njgJEKMjAxMC0xMi0zMGQCHw9kFgJmDxUEE25ld3MuYXNweD9pZD0zMDA0NDgAZTIwMDnlubTop4TliJLnjq/lpJbnjq/lhoXorqHliJLkvpvlupTnu4/okKXmgKflnLDlnZfmmI7nu4booagg44CQ6L2s6Ieq5Y6f5biC5Zu95Zyf5oi/566h5bGA572R56uZ44CRCjIwMDktMTItMjhkAiAPZBYCZg8VBBNuZXdzLmFzcHg/aWQ9MzAwNDQ5AFsyMDA55bm057uP6JCl5oCn5oi/5Zyw5Lqn5byA5Y+R55So5Zyw6K6h5YiS5a6J5o6S6KGo44CQ6L2s6Ieq5Y6f5biC5Zu95Zyf5oi/566h5bGA572R56uZ44CRCjIwMDktMTItMjhkAiEPZBYCZg8VBBNuZXdzLmFzcHg/aWQ9MzAwNDQ3AEkyMDA55bm05bel5Lia55So5Zyw6K6h5YiS5a6J5o6S6KGo44CQ6L2s6Ieq5Y6f5biC5Zu95Zyf5oi/566h5bGA572R56uZ44CRCjIwMDktMTItMjhkAiIPZBYCZg8VBBNuZXdzLmFzcHg/aWQ9MzAwNDQ2AFIyMDA55bm05L+d6Zqc5oCn5L2P5oi/55So5Zyw6K6h5YiS5a6J5o6S6KGo44CQ6L2s6Ieq5Y6f5biC5Zu95Zyf5oi/566h5bGA572R56uZ44CRCjIwMDktMTItMjhkAgkPDxYEHhBDdXJyZW50UGFnZUluZGV4AgEeC1JlY29yZGNvdW50AidkZBgBBR5fX0NvbnRyb2xzUmVxdWlyZVBvc3RCYWNrS2V5X18WAgUMTGVmdDEkc2VhcmNoBQtMZWZ0MSRyZXNldNXI3qGhLoxzvr8eO1j2HeTugfIJ''',
'__VIEWSTATEGENERATOR': '14DD91A0',
'__EVENTTARGET': 'AspNetPager1',
'__EVENTARGUMENT': str(page),
'__EVENTVALIDATION': '/wEWIwLhw+20CAKdlKkkAuWJhPELAvCUxuwDAvK/saAHAoijlK8BAu2VlowLAoijqMQJAu2V+rQPAu2VglcC7ZWO6QcC0ozARQLSjNT6CwKIo4CKDgKIo5DABwLtlaqhAgKBsYu8CAKBsZ9RAoGxx6oCAoGx288KAoGx7+QNAoGxg5kEAoGx64UCAoGxs/YLAsXD4bsPAsXDzYYEAoGx/7oFAu2VvsYKAqLig7gGAuyjuaoGAujImYgNArXSuJUHAuOzj+oDApHMqaIMAvGOgsgIUNOB7crAAeirbo/qpKOPxUWSV5M=',
'pkid': 'CK530301',
'pkid2': '9',
'newskindid': 'CK530301',
'HiddenFieldPageFinished': '1',
'Left1$ddl_cname': 'CK',
'Left1$tb_search': '',
'Left1$rbl_site': 'title',
'AspNetPager1_input': str(page),
}
                yield FormRequest(self.targetUrl, method='POST', headers=self.header,
                                  # priority=priority,
                                  callback=self.parse_index,
                                  meta={'page': page,
                                        # 'priority': priority
                                        },
                              formdata=requests_data,
                              # headers={'Content-Type': 'application/json'},
                              # dont_filter=True
                              )
        except Exception as e:
            self.log(f'当前爬取页数失败, {datetime.datetime.now()}, 错误: {e}\n{traceback.format_exc()}', level=logging.ERROR)
            raise IntegrationException('爬取阻塞,请重启')

    def parse_index(self, response):
        '''
        拿到总页数,
        :param response:
        :return:
        '''
        try:
            page = response.meta.get('page')
            datas = Selector(text=response.body.decode('gbk'))
            dataItems = datas.xpath('//*[@id="dl_newslist"]/tr/td/table/tr')
            for dataItem in dataItems:
                title = dataItem.xpath('td[2]/a/text()').extract_first()
                url = 'http://218.69.100.10/' + dataItem.xpath('td[2]/a/@href').extract_first()
                ND = dataItem.xpath('td[3]/text()').extract_first()
                yield Request(url, method='GET', callback=self.parse_detail,
                                  meta={
                                      'page': page,
                                      'title': title,
                                      'ND': ND,
                                  },
                                  # body=requests_data, headers={'Content-Type': 'application/json'}
                                  dont_filter=True,
                                  )
        except Exception as e:
            self.log(f'列表页解析失败{page}, 错误: {e}\n{traceback.format_exc()}', level=logging.ERROR)

    def parse_detail(self, response):
        try:
            data = Selector(text=response.body.decode('gbk'))
            items = str(data.xpath('string(.)').extract()[0]).replace('\xa0', '').replace('\u3000', '')
            # TODO 共有字段
            # 年度
            ND_1 = response.meta.get('ND').split('-')[0] if response.meta.get('ND') else ''
            '''商服用地  工矿仓储用地  基础设施及公益事业类划拨用地'''
            # TODO 先匹配 结构 在解析表格  结构优先,不为空先取结构
            ZFYD_2_0, ZFYD_2_1, ZFYD_2_2 = '', '', ''
            SFYD_3_0, SFYD_3_1, SFYD_3_2 = '', '', ''
            GKCC_4_0, GKCC_4_1, GKCC_4_2 = '', '', ''
            JCSS_5_0, JCSS_5_1, JCSS_5_2 = '', '', ''
            if '指标安排' in items:
                # 住房用地(含保障性住房用地商品住房用地)供应计划面积(公顷)
                ZFYD_2_0 = reFunction(f'住房用地\s*([\d\万.]*)公顷[，,]', reFunction('指标安排[\s\S]*布局', items))
                # 商服用地供应计划面积(公顷)
                SFYD_3_0 = reFunction(f'商服用地\s*([\d\.万]*)公顷[，,]', reFunction('指标安排[\s\S]*布局', items))
                # 工矿仓储用地供应计划面积(公顷)
                GKCC_4_0 = reFunction(f'工矿仓储用地\s*([\d\.万]*)公顷[，,]', reFunction('指标安排[\s\S]*布局', items))
                # 基础设施及公益事业等划拨用地供应计划面积(公顷)
                JCSS_5_0 = reFunction(f'基础设施及公益事业类划拨用地\s*([\d\.万]*)公顷[，,]', reFunction('指标安排[\s\S]*布局', items))
            # TODO //table[@class="MsoNormalTable"][1] | //table[@border="1"] 通过表格获取
            if data.xpath('//table[@class="MsoNormalTable"][1] | //table[@border="1"]'):
                soup = BeautifulSoup(response.body.decode('gbk'))
                tables = soup.find('table', attrs={'class':'MsoNormalTable'})
                table = tables if tables else soup.find_all('table', border="1")[0]
                htmlTable = htmlTableTransformer()
                try:
                    tdData = htmlTable.tableTrTdRegulation(table)
                except:
                    try:
                        table.tbody.find_all('tr')[0].extract()
                        tdData = htmlTable.tableTrTdRegulation(table)
                    except:
                        tdData = {}
                # 住房用地(含保障性住房用地商品住房用地)供应计划面积(公顷)
                ZFYD_2_1 = tdData.get('住房用地（含保障性住房用地、商品住房用地）')
                # 商服用地供应计划面积(公顷)
                SFYD_3_1 = tdData.get('商服用地')
                # 工矿仓储用地供应计划面积(公顷)
                GKCC_4_1 = tdData.get('工矿仓储用地')
                # 基础设施及公益事业等划拨用地供应计划面积(公顷)
                JCSS_5_1 = tdData.get('基础设施及公益事业等划拨用地')
            # 住房用地(含保障性住房用地商品住房用地)供应计划面积(公顷)
            ZFYD_2_2List = []
            ZFYD_2_2List.append(reFunction(f'全国住房用地计划供应*\s*([\d\.万]*)公顷[，,]', items))
            ZFYD_2_2List.append(reFunction(f'住房用地计划供应*\s*([\d\.万]*)公顷[，,]', items))
            ZFYD_2_2List.append(reFunction(f'住宅用地*\s*([\d\.万]*)公顷[，,]', items))
            ZFYD_2_2 = list(filter(lambda x: len(x)>1, ZFYD_2_2List))[0] if list(filter(lambda x: len(x)>1, ZFYD_2_2List)) else ''

            # 商服用地供应计划面积(公顷)
            SFYD_3_2_ = reFunction(f'商服用地\s*([\d\.万]*)公顷[，,]', items)
            SFYD_3_2__ = reFunction(f'商业用地计划出让\s*([\d\.万]*)公顷[，,]', items) if reFunction(f'商业用地计划出让\s*([\d\.万]*)公顷，', items) else reFunction(f'商业住房用地计划供应\s*([\d\.万]*)公顷[，,]', items)
            SFYD_3_2 = SFYD_3_2_ if SFYD_3_2_ else SFYD_3_2__

            # 工矿仓储用地供应计划面积(公顷
            GKCC_4_2 = reFunction(f'工矿仓储用地\s*([\d\.万]*)公顷[，,]', items)
            # 基础设施及公益事业等划拨用地供应计划面积(公顷)
            JCSS_5_2 = reFunction(f'基础设施及公益事业类划拨用地\s*([\d\.万]*)公顷[，,]',  items)

            ZFYD_2 = list(filter(None, [ZFYD_2_0, ZFYD_2_1 ,ZFYD_2_2]))[0] if list(filter(None, [ZFYD_2_0, ZFYD_2_1 ,ZFYD_2_2])) else ''
            SFYD_3 = list(filter(None, [SFYD_3_0, SFYD_3_1 ,SFYD_3_2]))[0] if list(filter(None, [SFYD_3_0, SFYD_3_1 ,SFYD_3_2])) else ''
            GKCC_4 = list(filter(None, [GKCC_4_0, GKCC_4_1 ,GKCC_4_2]))[0] if list(filter(None, [GKCC_4_0, GKCC_4_1 ,GKCC_4_2])) else ''
            JCSS_5 = list(filter(None, [JCSS_5_0, JCSS_5_1 ,JCSS_5_2]))[0] if list(filter(None, [JCSS_5_0, JCSS_5_1 ,JCSS_5_2])) else ''

            # 爬取时间
            crawlingTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            # 爬取地址url
            url = response.url
            # 唯一标识
            md5Mark = encrypt_md5(url)

            # 存储数据
            csvFile = [
                ND_1,
                ZFYD_2,
                SFYD_3,
                GKCC_4,
                JCSS_5,
                crawlingTime,
                url,
                md5Mark,
                ]
            results = ''
            for _ in csvFile:
                try:
                    if _ and _ != '|' * len(_):
                        results += _.replace(',', ' ').replace('\n', '').replace('\r', '').replace(r'\xa0', '').replace('\xa0', '') + ','
                    else:
                        results += ','
                except Exception as e:
                    results += ','
                    self.log(f'{getVariableName(_).pop()}字段解析出错, 错误: {e}\n{traceback.format_exc()}',
                             level=logging.ERROR)
            with open(self.pathDetail, 'a+') as fp:
                fp.write(results)
                fp.write('\n')
            self.log(f'数据获取成功', level=logging.INFO)
            yield
        except Exception as e:
            print(response.url)
            self.log(f'详情页数据解析失败, 错误: {e}\n{traceback.format_exc()}', level=logging.ERROR)
