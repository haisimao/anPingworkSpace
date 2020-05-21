# zhengzhouAppropriateResult
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


class zhengzhouAppropriateResultSpider(CrawlSpider):
    # TODO
    name = 'zhengzhouAppropriateResult'
    # 配置更换cookies时间
    COOKIES_SWITCH_TIME = datetime.datetime.now()

    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super(zhengzhouAppropriateResultSpider, cls).__new__(cls)
            # TODO
            pathPage = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),'Logs/zhengzhouAppropriateResultPage.txt')
            # TODO
            cls.pathDetail = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))), 'data/郑州市自然资源和规划局_划拨供地结果_郑州.csv')
            cls.filePage = open(pathPage, 'w+')
            if os.path.exists(cls.pathDetail):
                cls.fileDetail = open(cls.pathDetail, 'a+')
            else:
                cls.fileDetail = open(cls.pathDetail, 'a+')
                with open(cls.pathDetail, 'a+') as fp:
                    # TODO
                    fp.write("""标题,来源,时间,序号,批准文号,用地单位,供地方式,批准时间,位置,用途,面积,容积率,供应方案文号,爬取地址url,唯一标识,\n""")
        return cls.instance

    def __init__(self):
        super(zhengzhouAppropriateResultSpider, self).__init__()
        dispatcher.connect(self.CloseSpider, signals.spider_closed)
        # TODO
        self.redisClient = RedisClient('zhengzhou', 'zhengzhouAppropriateResult')
        self.duplicateUrl = 0
        self.targetUrl = 'http://zzland.zhengzhou.gov.cn/hbgd/index_{}.jhtml'
        self.header = {'User-Agent': random.choice(agent_list)}
        self.reStr = '（）\w\.:： 。 \(\)〔〕㎡㎡≤；，≥《》\-\/\%,，、\.﹪㎡'

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
            yield Request('http://zzland.zhengzhou.gov.cn/hbgd/index.jhtml', method='GET', headers=self.header,
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
            BT_47 = ''
            LY_55 = ''
            LYSJ_48 = ''
            XH_49 = ''
            PZWH_50 = ''
            YDDW_51 = ''
            GDFS_52 = ''
            PZSJ_53 = ''
            WZ_54 = ''
            YT_55 = ''
            MJ_56 = ''
            RJL_57 = ''
            GYWAFA_58 = ''

            # TODO 共有字段
            # 标题
            BT_47 = response.meta.get('title')
            LY = data.xpath('//div[@class="content-small-title"]/text()').extract_first()
            # 来源
            LY_55 = reFunction(f'来源：\s*([{self.reStr}]*)\s', LY)
            # 时间
            LYSJ_48 = reFunction(f'时间：\s*([{self.reStr}]*)\s', LY)
            # 解析 table 若出错 使用正则
            htmlTable = htmlTableTransformer()
            if '宗地编号' not in items:
                try:
                    soup = BeautifulSoup(response.body.decode('utf-8'))
                    table = soup.find_all('table')[0]

                    if not table.tbody.find_all('tr')[0].find_all(text=re.compile("序号|受让人")):
                        table.tbody.find_all('tr')[0].extract()
                    tdsData = htmlTable.tableTrTdRegulationToList(table)

                    for _ in range(len(list(tdsData.values())[0])):
                        # if response.url == 'http://zzland.zhengzhou.gov.cn/hbgd/1715241.jhtml':
                        #     print()
                        # 序号
                        XH_49 = tdsData.get('序号')[_] if tdsData.get('序号') else ''
                        # 批准文号
                        PZWH_50 = tdsData.get('批准文号')[_] if tdsData.get('批准文号') else ''
                        # 用地单位
                        YDDW_51_ = tdsData.get('用地单位（受让人）')[_] if  tdsData.get('用地单位（受让人）') else tdsData.get('受让人')[_] if tdsData.get('受让人') else ''
                        YDDW_51 = YDDW_51_ if  YDDW_51_ else tdsData.get('单位')[_]
                        # 供地方式
                        GDFS_52 = tdsData.get('供地方式')[_] if tdsData.get('供地方式') else tdsData.get('供应方式')[_] if tdsData.get('供应方式') else ''
                        # 批准时间
                        PZSJ_53 = tdsData.get('批准时间')[_] if tdsData.get('批准时间') else tdsData.get('签订日期')[_] if tdsData.get('签订日期') else ''
                        # 位置
                        WZ_54_0 = tdsData.get('土地位置')
                        WZ_54_1 = tdsData.get('土地座落')
                        WZ_54_2 = tdsData.get('宗地位置')
                        WZ_54_3 = tdsData.get('位置')
                        WZ_54_ = list(filter(None, [WZ_54_0, WZ_54_1, WZ_54_2, WZ_54_3]))
                        WZ_54 = WZ_54_[0][_] if WZ_54_ else ''
                        # 用途
                        YT_55_0 = tdsData.get('用途')
                        YT_55_1 = tdsData.get('土地用途')
                        YT_55_2 = tdsData.get('用途明细')
                        YT_55_ = list(filter(None, [YT_55_0, YT_55_1, YT_55_2]))
                        YT_55 = YT_55_[0][_] if YT_55_  else ''
                        # 面积
                        MJ_56_0 = tdsData.get('面积（平方米）')
                        MJ_56_1 = tdsData.get('划拨面积')
                        MJ_56_2 = tdsData.get('出让/划拨面积')
                        MJ_56_3 = tdsData.get('面积（公顷）')
                        MJ_56_ = list(filter(None, [MJ_56_0, MJ_56_1, MJ_56_2, MJ_56_3]))
                        MJ_56 = MJ_56_[0][_] if MJ_56_ else ''
                        # 容积率
                        RJL_57 = tdsData.get('容积率')[_] if tdsData.get('容积率') else ''
                        # 供应方案文号
                        GYWAFA_58 = tdsData.get('供应方案文号')[_] if tdsData.get('供应方案文号') else ''
                        # 爬取时间
                        crawlingTime = time.strftime("%Y-%m-%d", time.localtime())
                        # 爬取地址url
                        url = response.url
                        # 唯一标识
                        md5Mark = encrypt_md5(url + BT_47 + LYSJ_48)

                        # 是否需要判断重复 请求
                        if DUPLICATE_SWITCH:
                            if self.redisClient.isExist(md5Mark):  # 存在, 去重计数
                                self.duplicateUrl += 1

                        if self.duplicateUrl < 50:
                            # 重复效验通过, 存储数据
                            csvFile = [
                                BT_47,
                                LY_55,
                                LYSJ_48,
                                XH_49,
                                PZWH_50,
                                YDDW_51,
                                GDFS_52,
                                PZSJ_53,
                                WZ_54,
                                YT_55,
                                MJ_56,
                                RJL_57,
                                GYWAFA_58,
                                crawlingTime,
                                url,
                                md5Mark,
                            ]
                            results = ''
                            for _ in csvFile:
                                try:
                                    if _ and _ != '|' * len(_):
                                        results += _.replace(',', ' ').replace('\n', '').replace('\r', '').replace(
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
                    pass
            else:
                # 进行正则匹配
                # 序号
                XH_49 = reFunction(f'宗地编号([{self.reStr}]*)地块位置', items)
                # 用地单位
                YDDW_51 = reFunction(f'受让单位([{self.reStr}]*)备注：', items)
                # 位置
                WZ_54 = reFunction(f'地块位置([{self.reStr}]*)土地用途', items)
                # 用途
                YT_55 = reFunction(f'土地用途([{self.reStr}]*)土地面积', items)
                # 面积
                MJ_56 = reFunction(f'土地面积\(公顷\)([{self.reStr}]*)项目名称', items)
                # 爬取时间
                crawlingTime = time.strftime("%Y-%m-%d", time.localtime())
                # 爬取地址url
                url = response.url
                # 唯一标识
                md5Mark = encrypt_md5(url + BT_47 + LYSJ_48)

                # 是否需要判断重复 请求
                if DUPLICATE_SWITCH:
                    if self.redisClient.isExist(md5Mark):  # 存在, 去重计数
                        self.duplicateUrl += 1

                if self.duplicateUrl < 50:
                    # 重复效验通过, 存储数据
                    csvFile = [
                        BT_47,
                        LY_55,
                        LYSJ_48,
                        XH_49,
                        PZWH_50,
                        YDDW_51,
                        GDFS_52,
                        PZSJ_53,
                        WZ_54,
                        YT_55,
                        MJ_56,
                        RJL_57,
                        GYWAFA_58,
                        crawlingTime,
                        url,
                        md5Mark,
                    ]
                    results = ''
                    for _ in csvFile:
                        try:
                            if _ and _ != '|' * len(_):
                                results += _.replace(',', ' ').replace('\n', '').replace('\r', '').replace(r'\xa0',
                                                                                                           '').replace(
                                    '\xa0', '') + ','
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

