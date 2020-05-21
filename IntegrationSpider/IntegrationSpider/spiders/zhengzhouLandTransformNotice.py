# zhengzhouLandTransformNotice
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


class zhengzhouLandTransformNoticeSpider(CrawlSpider):
    # TODO
    name = 'zhengzhouLandTransformNotice'
    # 配置更换cookies时间
    COOKIES_SWITCH_TIME = datetime.datetime.now()

    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super(zhengzhouLandTransformNoticeSpider, cls).__new__(cls)
            # TODO
            pathPage = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),'Logs/zhengzhouLandTransformNoticePage.txt')
            # TODO
            cls.pathDetail = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))), 'data/郑州市自然资源和规划局_土地协议出让公告_郑州.csv')
            cls.filePage = open(pathPage, 'w+')
            if os.path.exists(cls.pathDetail):
                cls.fileDetail = open(cls.pathDetail, 'a+')
            else:
                cls.fileDetail = open(cls.pathDetail, 'a+')
                with open(cls.pathDetail, 'a+') as fp:
                    # TODO
                    fp.write("""标题,来源,时间,编号,土地位置,使用权面积,规划用地性质,出让年限,爬取地址url,唯一标识,\n""")
        return cls.instance

    def __init__(self):
        super(zhengzhouLandTransformNoticeSpider, self).__init__()
        dispatcher.connect(self.CloseSpider, signals.spider_closed)
        # TODO
        self.redisClient = RedisClient('zhengzhou', 'zhengzhouLandTransformNotice')
        self.duplicateUrl = 0
        self.targetUrl = 'http://zzland.zhengzhou.gov.cn/xycrgg/index_{}.jhtml'
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
            for page in range(1, 3):
                priority = 4 - int(page)
                yield Request(self.targetUrl.format(page), method='GET', headers=self.header,
                              priority=priority,
                              callback=self.parse_index,
                              meta={'page': page, 'priority': priority},
                              # headers={'Content-Type': 'application/json'},
                              dont_filter=True
                              )
            yield Request('http://zzland.zhengzhou.gov.cn/xycrgg/index.jhtml', method='GET', headers=self.header,
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
            '''data.xpath("string(path)")
            path -- xpath提取的路径  这里提取到父标签
           '''
            # TODO 共有字段
            # 标题
            BT_10 = response.meta.get('title')
            LY = data.xpath('//div[@class="content-small-title"]/text()').extract_first()
            # 来源
            LY_11 = reFunction(f'来源：\s*([{self.reStr}]*)\s', LY)
            # 时间
            SJ_12 = reFunction(f'时间：\s*([{self.reStr}]*)\s', LY)
            # 编号
            BH_13 = ''.join(data.xpath("string(//table[1]/tbody/tr[2]/td[1])").extract())
            # 土地位置
            TDWZ_14 = ''.join(data.xpath("string(//table[1]/tbody/tr[2]/td[2])").extract())
            # 使用权面积
            SYQMJ_15 = ''.join(data.xpath("string(//table[1]/tbody/tr[2]/td[3])").extract())
            # TODO 规划用地性质
            GHYDXZ_16 = ''.join(data.xpath("string(//table[1]/tbody/tr[2]/td[4])").extract())
            # 出让年限
            CRNX_17 = ''.join(data.xpath("string(//table[1]/tbody/tr[2]/td[5])").extract())
            # 爬取时间
            crawlingTime = time.strftime("%Y-%m-%d", time.localtime())
            # 爬取地址url
            url = response.url
            # 唯一标识
            md5Mark = encrypt_md5(url + BT_10 + SJ_12)

            # 是否需要判断重复 请求
            if DUPLICATE_SWITCH:
                if self.redisClient.isExist(md5Mark):  # 存在, 去重计数
                    self.duplicateUrl += 1

            if self.duplicateUrl < 50:
                # 重复效验通过, 存储数据
                csvFile = [
                    BT_10,
                    LY_11,
                    SJ_12,
                    BH_13,
                    TDWZ_14,
                    SYQMJ_15,
                    GHYDXZ_16,
                    CRNX_17,
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

