# nanjingLandDetail
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


class nanjingLandDetailSpider(CrawlSpider):
    name = 'nanjingLandDetail'
    # 配置更换cookies时间
    COOKIES_SWITCH_TIME = datetime.datetime.now()

    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super(nanjingLandDetailSpider, cls).__new__(cls)
            pathPage = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),'Logs/nanjingLandDetailPage.txt')
            cls.pathDetail = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))), 'data/南京市国有建设用地使用权公开出让网上交易系统_地块详情_南京.csv')
            cls.filePage = open(pathPage, 'w+')
            if os.path.exists(cls.pathDetail):
                cls.fileDetail = open(cls.pathDetail, 'a+')
            else:
                cls.fileDetail = open(cls.pathDetail, 'a+')
                with open(cls.pathDetail, 'a+') as fp:
                    fp.write("""标题,公告编号,地块编号,地块名称,容积率,用地性质,规划面积,实际岀让面积,公告发布时间,保证金金额,挂牌起始价,竟争保障房建设资金起始价,最高限价,加价幅度,报名开始时时间,报名截至时间,报价截至时间,保证金截至时间,限时竟价开始时间,最新报价,最近报价时间,竟得者,竟得价,报价轮次,报价人,金额报价,单位地价,报价时间,爬取地址url,唯一标识,\n""")
        return cls.instance

    def __init__(self):
        super(nanjingLandDetailSpider, self).__init__()
        dispatcher.connect(self.CloseSpider, signals.spider_closed)
        self.redisClient = RedisClient('nanjing', 'LandDetail')
        self.duplicateUrl = 0
        self.targetUrl = 'https://jy.landnj.cn/default.aspx?page={}'
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
            for page in range(1, 5):
                priority = 5 - int(page)
                yield Request(self.targetUrl.format(page), method='GET', headers=self.header, priority=priority, callback=self.parse_index, meta={'page': page, 'priority': priority},
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
            dataItems = datas.xpath('//*[@id="ctl00_ContentPlaceHolder1_GridView1"]/tr[position()>1 and position()<12]')
            for dataItem in dataItems:
                # title = dataItem.xpath('a/text()').extract_first()
                url = 'https://jy.landnj.cn' + dataItem.xpath('td[1]/a/@href').extract_first()
                yield Request(url, method='GET', callback=self.parse_detail,
                                  meta={
                                      'page': page,
                                      # 'title': title,
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
            # TODO 共有字段
            # 标题
            BT_1 = ''.join(data.xpath('//*[@id="ctl00_ContentPlaceHolder1_UpdatePanel2"]/div/span/text()').extract())
            # 公告编号
            GGBH_2 = reFunction(f'公告编号：\s*([{self.reStr}]*)\s',items)
            # 地块编号
            DKBH_3 = reFunction(f'地块编号：\s*([{self.reStr}]*)\s',items)
            # 地块名称
            DKMC_4 = reFunction(f'地块名称：\s*([{self.reStr}]*)\s',items)
            # 容积率
            RJL_5 = reFunction(f'容积率：\s*([{self.reStr}]*)\s',items)
            # 用地性质
            YDXZ_6 = reFunction(f'用地性质：\s*([{self.reStr}]*)\s',items)
            # 规划面积
            GHMJ_7 = reFunction(f'规划面积：\s*([{self.reStr}]*)\s',items)
            # 实际岀让面积
            SJCRMJ_8 = reFunction(f'实际出让面积：\s*([{self.reStr}]*)\s',items)
            # 公告发布时间
            GGFBSJ_9 = reFunction(f'公告发布时间：\s*([{self.reStr}]*)\s',items)
            # 保证金金额
            BZJJE_10 = reFunction(f'保证金金额：\s*([{self.reStr}]*)\s',items)
            # 挂牌起始价
            GPQSJ_11 = reFunction(f'挂牌起始价：\s*([{self.reStr}]*)\s',items)
            # 竟争保障房建设资金起始价
            JZBZ_12 = reFunction(f'竞争保障房建设资金起始价：\s*([{self.reStr}]*)\s',items)
            # 最高限价
            ZGXJ_13 = reFunction(f'最高限价：\s*([{self.reStr}]*)\s',items)
            # 加价幅度
            JJFD_14 = reFunction(f'加价幅度：\s*([{self.reStr}]*)\s',items)
            # 报名开始时时间
            BMKS_15 = reFunction(f'报名开始时间：\s*([{self.reStr}]*)\s',items)
            # 报名截至时间
            BMJZ_16 = reFunction(f'报名截至时间：\s*([{self.reStr}]*)\s',items)
            # 报价截至时间
            BJJZ_17 = reFunction(f'报价截至时间：\s*([{self.reStr}]*)\s',items)
            # 保证金截至时间
            BZJJZ_18 = reFunction(f'保证金截至时间：\s*([{self.reStr}]*)\s',items)
            # 限时竟价开始时间
            ZSJJKS_19 = reFunction(f'限时竞价开始时间：\s*([{self.reStr}]*)\s',items)
            # 最新报价
            ZXBJ_20 = reFunction(f'最新报价：\s*([{self.reStr}]*)\s',items)
            # 最近报价时间
            ZXBJ_21 = reFunction(f'最新报价时间：\s*([{self.reStr}]*)\s',items)
            # 竟得者
            JDZ_22 = reFunction(f'竞得者：\s*([{self.reStr}]*)\s',items)
            # 竟得价
            ZDJ_23 = reFunction(f'竞得价：\s*([{self.reStr}]*)\s',items)
            # 报价轮次
            BJLC_24 = data.xpath('//*[@id="ctl00_ContentPlaceHolder1_GVLandPrice"]/tr[2]/td[1]/text()').extract_first()
            # 报价人
            BJR_25 = data.xpath('//*[@id="ctl00_ContentPlaceHolder1_GVLandPrice"]/tr[2]/td[2]/span/text()').extract_first()
            # 金额报价
            JEBJ_26 = data.xpath('//*[@id="ctl00_ContentPlaceHolder1_GVLandPrice"]/tr[2]/td[3]/span/text()').extract_first()
            # 单位地价
            DWDJ_27 = data.xpath('//*[@id="ctl00_ContentPlaceHolder1_GVLandPrice"]/tr[2]/td[4]/span/text()').extract_first()
            # 报价时间
            BJSJ_28 = data.xpath('//*[@id="ctl00_ContentPlaceHolder1_GVLandPrice"]/tr[2]/td[5]/text()').extract_first()

            # 爬取时间
            crawlingTime = time.strftime("%Y-%m-%d", time.localtime())
            # 爬取地址url
            url = response.url
            # 唯一标识
            md5Mark = encrypt_md5(url + BT_1 + GGBH_2)

            # 是否需要判断重复 请求
            if DUPLICATE_SWITCH:
                if self.redisClient.isExist(md5Mark):  # 存在, 去重计数
                    self.duplicateUrl += 1

            if self.duplicateUrl < 50:
                # 重复效验通过, 存储数据
                csvFile = [
                    BT_1,
                    GGBH_2,
                    DKBH_3,
                    DKMC_4,
                    RJL_5,
                    YDXZ_6,
                    GHMJ_7,
                    SJCRMJ_8,
                    GGFBSJ_9,
                    BZJJE_10,
                    GPQSJ_11,
                    JZBZ_12,
                    ZGXJ_13,
                    JJFD_14,
                    BMKS_15,
                    BMJZ_16,
                    BJJZ_17,
                    BZJJZ_18,
                    ZSJJKS_19,
                    ZXBJ_20,
                    ZXBJ_21,
                    JDZ_22,
                    ZDJ_23,
                    BJLC_24,
                    BJR_25,
                    JEBJ_26,
                    DWDJ_27,
                    BJSJ_28,
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
            self.log(f'详情页数据解析失败, 错误: {e}\n{traceback.format_exc()}', level=logging.ERROR)
