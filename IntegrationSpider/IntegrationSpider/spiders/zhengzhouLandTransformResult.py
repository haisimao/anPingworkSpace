# zhengzhouLandTransformResult
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
from IntegrationSpider.settings import DUPLICATE_SWITCH
from IntegrationSpider.useragent import agent_list
from IntegrationSpider.utils_ import IntegrationException, getSesion, encrypt_md5
from SpiderTools.Tool import reFunction, getVariableName
from SpiderTools.tableAnalysis import htmlTableTransformer
from db import RedisClient


class zhengzhouLandTransformResultSpider(CrawlSpider):
    name = 'zhengzhouLandTransformResult'
    # 配置更换cookies时间
    COOKIES_SWITCH_TIME = datetime.datetime.now()

    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super(zhengzhouLandTransformResultSpider, cls).__new__(cls)
            pathPage = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),'Logs/zhengzhouLandTransformResultPage.txt')
            cls.pathDetail = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))), 'data/郑州市自然资源和规划局_土地招标拍卖挂牌出让结果_郑州.csv')
            cls.filePage = open(pathPage, 'w+')
            if os.path.exists(cls.pathDetail):
                cls.fileDetail = open(cls.pathDetail, 'a+')
            else:
                cls.fileDetail = open(cls.pathDetail, 'a+')
                with open(cls.pathDetail, 'a+') as fp:
                    fp.write("""标题,来源,时间,挂牌时间,土地编号,土地坐落,使用权面积,竞得人,成交价,爬取地址url,唯一标识,\n""")
        return cls.instance

    def __init__(self):
        super(zhengzhouLandTransformResultSpider, self).__init__()
        dispatcher.connect(self.CloseSpider, signals.spider_closed)
        self.redisClient = RedisClient('zhengzhou', 'LandTransformResult')
        self.duplicateUrl = 0
        self.targetUrl = 'http://zzland.zhengzhou.gov.cn/tdcr/index_{}.jhtml'
        self.header = {'User-Agent': random.choice(agent_list)}
        self.reStr = '（）\w\.:： 。 \(\)〔〕㎡㎡≤；，≥《》\-\/\%,、\.﹪㎡'

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
            for page in range(1, 51):
                priority = 51 - int(page)
                yield Request(self.targetUrl.format(page), method='GET', headers=self.header,
                              priority=priority,
                              callback=self.parse_index,
                              meta={'page': page, 'priority': priority},
                              # headers={'Content-Type': 'application/json'},
                              dont_filter=True
                              )
            yield Request('http://zzland.zhengzhou.gov.cn/tdcr/index.jhtml', method='GET', headers=self.header,
                              callback=self.parse_index,
                              meta={'page': 1, 'priority': 1},
                              # headers={'Content-Type': 'application/json'},
                              dont_filter=True
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
            dataItems = datas.xpath('//div[@class="box-content"]/ul/li')
            for dataItem in dataItems:
                title = dataItem.xpath('a/h1/text()').extract_first()
                url = dataItem.xpath('a/@href').extract_first()
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
            BT_1 = ''
            LY_2 = ''
            SJ_3 = ''
            GPSJ_4 = ''
            TDBH_5 = ''
            TDZL_6 = ''
            SYQMJ_7 = ''
            JDR_8 = ''
            CJJ_9 = ''
            # TODO 共有字段
            # 标题
            BT_1 = response.meta.get('title')
            LY = data.xpath('//div[@class="content-small-title"]/text()').extract_first()
            # 来源
            LY_2 = reFunction(f'来源：\s*([{self.reStr}]*)\s', LY)
            # 时间
            SJ_3 = reFunction(f'时间：\s*([{self.reStr}]*)\s', LY)
            # 挂牌时间
            try:
                GPSJ_4 = reFunction('挂牌时间：(\d{4}年\d{1,2}月\d{1,2}日至\d{4}年\d{1,2}月\d{1,2}日)', data.xpath('//div[@class="news_content_content"]/p[1]/text()').extract_first())
            except:
                GPSJ_4 = ''
            for item in re.findall(f'郑政出[{self.reStr}]*。', items):
                # 土地编号
                TDBH_5 = reFunction(f'郑政出([（）\w\.:： 。 \(\)〔〕〔〕㎡㎡≤；≥《》\-\/\%\.﹪㎡]*)[,，]', item)
                # 土地坐落
                TDZL_6 = reFunction(f'位于([（）\w\.:： 。 、\(\)〔〕〔〕㎡㎡≤；≥《》\-\/\%\.﹪㎡]*)[,，]', item)
                # 使用权面积
                SYQMJ_7 = reFunction(f'使用权面积([（）\w\.:： 。 、\(\)〔〕〔〕㎡㎡≤；≥《》\-\/\%\.﹪㎡]*)[,，]', item)
                # 竞得人
                JDR_8 = reFunction(f'竞得人为([（）\w\.:： 。 、\(\)〔〕〔〕㎡㎡≤；≥《》\-\/\%\.﹪㎡]*)[,，]', item)
                # 成交价
                CJJ_9 = reFunction(f'成交价([（）\w\.:：  、\(\)〔〕〔〕㎡㎡≤；≥《》\-\/\%\.﹪㎡]*)。', item)

            # 爬取时间
            crawlingTime = time.strftime("%Y-%m-%d", time.localtime())
            # 爬取地址url
            url = response.url
            # 唯一标识
            md5Mark = encrypt_md5(url + BT_1 + SJ_3)

            # 是否需要判断重复 请求
            if DUPLICATE_SWITCH:
                if self.redisClient.isExist(md5Mark):  # 存在, 去重计数
                    self.duplicateUrl += 1

            if self.duplicateUrl < 50:
                # 重复效验通过, 存储数据
                csvFile = [
                    BT_1,
                    LY_2,
                    SJ_3,
                    GPSJ_4,
                    TDBH_5,
                    TDZL_6,
                    SYQMJ_7,
                    JDR_8,
                    CJJ_9,
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
            else:
                self.crawler.engine.close_spider(self, 'response msg info %s, job duplicated!' % response.url)
        except Exception as e:
            print(response.url)
            self.log(f'详情页数据解析失败, 请求:{response.url}, 错误: {e}\n{traceback.format_exc()}', level=logging.ERROR)
