# -*- coding: utf-8 -*-
import copy
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


class laSaResultNoticeSpider(CrawlSpider):
    name = 'laSaResultNotice'
    # 配置更换cookies时间
    COOKIES_SWITCH_TIME = datetime.datetime.now()

    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super(laSaResultNoticeSpider, cls).__new__(cls)
            pathPage = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),'Logs/laSaResultNoticePage.txt')
            cls.pathDetail = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))), 'data/拉萨公共资源交易网_结果公示_拉萨.csv')
            cls.filePage = open(pathPage, 'w+')
            if os.path.exists(cls.pathDetail):
                cls.fileDetail = open(cls.pathDetail, 'a+')
            else:
                cls.fileDetail = open(cls.pathDetail, 'a+')
                with open(cls.pathDetail, 'a+') as fp:
                    fp.write("""文件标题,来源,更新时间,公告时间,公告媒体,公告号,出让方式,成交时间,成交地点,地块编号,地块位置,土地用途,挂牌起始价,竞得单位,成交金额,爬取地址url,唯一标识,\n""")
        return cls.instance

    def __init__(self):
        super(laSaResultNoticeSpider, self).__init__()
        dispatcher.connect(self.CloseSpider, signals.spider_closed)
        self.targetUrl = 'http://ggzy.lasa.gov.cn/Article/SearchArticle'
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
            for page in range(1, 17):
                requests_data = {
'categoryId': '732',
'typeId': '0',
'pageNum': str(page),
'pageSize': '10',
'search': 'false',
'Title': '',
'StartTime': '',
'EndTime': '',
'area': '%E8%AF%B7%E9%80%89%E6%8B%A9',
}
                priority = 17 - int(page)
                yield FormRequest(self.targetUrl, method='POST', headers=self.header, priority=priority, callback=self.parse_index, meta={'page': page, 'priority': priority},
                              formdata=requests_data,
                              # body=requests_data,
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
            datas = Selector(text=response.body.decode('utf-8'))
            dataItems = datas.xpath('//div[@id="listCon"]/ul/li')
            for dataItem in dataItems:
                title = dataItem.xpath('a/text()').extract_first()
                url = 'http://ggzy.lasa.gov.cn' + dataItem.xpath('a/@href').extract_first()
                yield Request(url, method='GET', callback=self.parse_detail,
                                  meta={
                                      'page': page,
                                      'title': title,
                                  },
                                  # body=requests_data, headers={'Content-Type': 'application/json'}
                                  dont_filter=True,
                                  )
        except Exception as e:
            self.log(f'列表页解析失败{page}, 错误: {e}\n{traceback.format_exc()}', level=logging.ERROR)

    def parse_detail(self, response):
        try:
            data = Selector(text=response.body.decode('utf-8'))
            items = str(data.xpath('string(.)').extract()[0]).replace('\xa0', '').replace('\u3000', '')
            WJBT_18 = ''
            LY_19 = ''
            GXSJ_20 = ''
            GGSJ_21 = ''
            GGMT_22 = ''
            GGH_23 = ''
            CRFS_24 = ''
            CJSJ_25 = ''
            CJDD_26 = ''
            DKBH_27 = ''
            DKWZ_28 = ''
            TDYT_29 = ''
            GPQSJ_30 = ''
            JDDW_31 = ''
            CJJE_32 = ''

            # TODO 共有字段
            # 文件标题
            WJBT_18 = response.meta.get('title')
            # 文章来源
            WZLY_19 = data.xpath('//div[@class="news_time"]/span[1]/text()').extract_first().replace('文章来自：', '')
            # 更新时间
            GXSJ_20 = data.xpath('//div[@class="news_time"]/span[1]/text()').extract_first().replace('更新时间：', '')

            # TODO //table[@border="1"]   //table[@border="0"]
            soup = BeautifulSoup(response.body.decode('utf-8'))
            tables = soup.find_all('table')

            tablesCopy = BeautifulSoup(response.body.decode('utf-8')).find_all('table')

            for _ in range(len(tables)):
                table = tables[_]
                tableCopy = tablesCopy[_]
                # table 解析 首先解析第一行, 删除异常行,
                trList = table.tbody.find_all('tr')
                for _ in range(len(trList)):
                    if not trList[_].find_all('td', text=re.compile("公告时间")) and not trList[_].find_all('p', text=re.compile("公告时间")):
                        table.tbody.find_all('tr')[0].extract()  # 处理异常行
                        continue
                    break

                trListCopy = tableCopy.tbody.find_all('tr')
                for _ in range(len(trListCopy)):
                    if not trListCopy[_].find_all('td', text=re.compile("公告时间")) and not trListCopy[_].find_all('p', text=re.compile("公告时间")):
                        tableCopy.tbody.find_all('tr')[0].extract()  # 处理异常行
                        continue
                    break

                for _ in range(2, len(tableCopy.tbody.find_all('tr'))):
                    try:
                        tableCopy.tbody.find_all('tr')[2].extract()
                    except:
                        pass
                htmlTable = htmlTableTransformer()
                tdDataCopy = htmlTable.tableTrTdRegulation(tableCopy)
                # 公告时间
                GGSJ_21 = tdDataCopy.get('公告时间')
                # 公告媒体
                GGMT_22 = tdDataCopy.get('公告媒体')
                # 公告号
                GGH_23 = tdDataCopy.get('公告号')
                # 出让方式
                CRFS_24 = tdDataCopy.get('出让方式')
                # 成交时间
                CJSJ_25 = tdDataCopy.get('成交时间')
                # 成交地点
                CJDD_26 = tdDataCopy.get('成交地点')

                # TODO 解析第二行
                for _ in range(2):
                    try:
                        table.tbody.find_all('tr')[0].extract()
                    except:
                        pass
                htmlTable = htmlTableTransformer()
                tdData = htmlTable.tableTrTdRegulation(table)
                # 地块编号
                DKBH_27 = tdData.get('地块编号')
                # 地块位置
                DKWZ_28 = tdData.get('地块位置')
                # 土地用途
                TDYT_29 = tdData.get('土地用途')
                # 挂牌起始价
                GPQSJ_30 = tdData.get('挂牌起始价（万元）') if tdData.get('挂牌起始价（万元）') else tdData.get('挂牌起始价（元）')
                # 竞得单位
                JDDW_31 = tdData.get('竞得人(单位)') if tdData.get('竞得人(单位)') else tdData.get('竞得单位')
                # 成交金额
                CJJE_32_ = tdData.get('成交价（万元）') if tdData.get('成交价（万元）') else tdData.get('成交金额（元）')
                CJJE_32 = CJJE_32_ if CJJE_32_ else tdData.get('成交价')
                # 爬取时间
                crawlingTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                # 爬取地址url
                url = response.url
                # 唯一标识
                md5Mark = encrypt_md5(url+WJBT_18+GXSJ_20)

                # 存储数据
                csvFile = [
                    WJBT_18,
                    LY_19,
                    GXSJ_20,
                    GGSJ_21,
                    GGMT_22,
                    GGH_23,
                    CRFS_24,
                    CJSJ_25,
                    CJDD_26,
                    DKBH_27,
                    DKWZ_28,
                    TDYT_29,
                    GPQSJ_30,
                    JDDW_31,
                    CJJE_32,
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
                        self.log(f'{getVariableName(_).pop()}字段解析出错, 错误: {e}\n{traceback.format_exc()}', level=logging.ERROR)
                with open(self.pathDetail, 'a+') as fp:
                    fp.write(results)
                    fp.write('\n')
                self.log(f'数据获取成功', level=logging.INFO)
                yield
        except Exception as e:
            print(response.url)
            self.log(f'详情页数据解析失败, 错误: {e}\n{traceback.format_exc()}', level=logging.ERROR)
