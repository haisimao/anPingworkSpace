# hefeiLandSupply
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
from IntegrationSpider.settings import DUPLICATE_SWITCH, DUPLICATE_SWITCH_LIST
from IntegrationSpider.useragent import agent_list
from IntegrationSpider.utils_ import IntegrationException, getSesion, encrypt_md5
from SpiderTools.Tool import reFunction, getVariableName
from SpiderTools.tableAnalysis import htmlTableTransformer
from db import RedisClient


class hefeiLandSupplySpider(CrawlSpider):
    # TODO
    name = 'hefeiLandSupply'
    # 配置更换cookies时间
    COOKIES_SWITCH_TIME = datetime.datetime.now()

    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super(hefeiLandSupplySpider, cls).__new__(cls)
            # TODO
            pathPage = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),'Logs/hefeiLandSupplyPage.txt')
            # TODO
            cls.pathDetail = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))), 'data/合肥土地市场_土地供应_合肥.csv')
            cls.filePage = open(pathPage, 'w+')
            if os.path.exists(cls.pathDetail):
                cls.fileDetail = open(cls.pathDetail, 'a+')
            else:
                cls.fileDetail = open(cls.pathDetail, 'a+')
                with open(cls.pathDetail, 'a+') as fp:
                    # TODO
                    fp.write("""爬取地址url,唯一标识,\n""")
        return cls.instance

    def __init__(self):
        super(hefeiLandSupplySpider, self).__init__()
        dispatcher.connect(self.CloseSpider, signals.spider_closed)
        # TODO
        self.redisClient = RedisClient('hefei', 'hefeiLandSupply')
        self.duplicateUrl = 0
        # TODO
        self.targetUrl = 'http://ggzy.hefei.gov.cn/hftd/tdgy/?Paging={}'
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
        '''
        try:
            for page in range(1, 8):
                yield Request(self.targetUrl.format(page), method='GET', headers=self.header,
                              callback=self.parse_index,
                              meta={'page': page},
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
            dataItems = datas.xpath('//div[@class="ewb-r-bd-con"]/ul/li')
            for dataItem in dataItems:
                title = dataItem.xpath('div/a/text()').extract_first()
                url = 'http://ggzy.hefei.gov.cn' + dataItem.xpath('div/a/@href').extract_first()[1:]
                msgTime = dataItem.xpath('span/text()').extract_first()
                yield Request(url, method='GET', callback=self.parse_detail,
                                  meta={
                                      'page': page,
                                      'title': title,
                                      'msgTime': msgTime,
                                  },
                                  # body=requests_data, headers={'Content-Type': 'application/json'}
                                  dont_filter=True,
                                  )
        except Exception as e:
            self.log(f'列表页解析失败{page}, 错误: {e}\n{traceback.format_exc()}', level=logging.ERROR)

    def parse_detail(self, response):
        # TODO 主动关闭爬虫问题
        try:
            data = Selector(text=response.body.decode('utf-8'))
            items = str(data.xpath('string(.)').extract()[0]).replace('\xa0', '').replace('\u3000', '')

            # TODO 共有字段  reFunction(f'时间：\s*([{self.reStr}]*)\s', LY)

            # 写入数据
            if self.name in DUPLICATE_SWITCH_LIST:
                if self.redisClient.isExist(md5Mark):  # 存在, 去重计数
                    self.duplicateUrl += 1

            if self.duplicateUrl < 50:
                if TDYT_37:
                    # 重复效验通过, 存储数据
                    csvFile = [

                        crawlingTime,
                        url,
                        md5Mark,
                    ]
                    results = ''
                    for _ in csvFile:
                        try:
                            if _ and _ != '|' * len(_):
                                results += _.replace(',', ' ').replace('\n', '').replace('\t', '').replace('\r', '').replace(
                                    r'\xa0', '').replace('\xa0', '') + ','
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





