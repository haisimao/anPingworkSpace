# zhengzhouLandAgreementTransformResult
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


class zhengzhouLandAgreementTransformResultSpider(CrawlSpider):
    # TODO
    name = 'zhengzhouLandAgreementTransformResult'
    # 配置更换cookies时间
    COOKIES_SWITCH_TIME = datetime.datetime.now()

    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super(zhengzhouLandAgreementTransformResultSpider, cls).__new__(cls)
            # TODO
            pathPage = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),'Logs/zhengzhouLandAgreementTransformResultPage.txt')
            # TODO
            cls.pathDetail = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))), 'data/郑州市自然资源和规划局_土地协议出让结果_郑州.csv')
            cls.filePage = open(pathPage, 'w+')
            if os.path.exists(cls.pathDetail):
                cls.fileDetail = open(cls.pathDetail, 'a+')
            else:
                cls.fileDetail = open(cls.pathDetail, 'a+')
                with open(cls.pathDetail, 'a+') as fp:
                    # TODO
                    fp.write("""标题,来源,时间,行政区,电子监管号,项目名称,项目位置,面积,土地来源,土地用途,供地方式,土地使用年限,行业分类,土地级别,成交价格,分期支付约定—支付期号,分期支付约定—约定支付日期,分期支付约定—约定支付金额,分期支付约定—备注,土地使用权人,约定容积率——下限,约定容积率——上限,约定交地时间,约定开工时间,约定竣工时间,实际开工时间,实际竣工时间,批准单位,合同签订日期,爬取地址url,唯一标识,\n""")
        return cls.instance

    def __init__(self):
        super(zhengzhouLandAgreementTransformResultSpider, self).__init__()
        dispatcher.connect(self.CloseSpider, signals.spider_closed)
        # TODO
        self.redisClient = RedisClient('zhengzhou', 'zhengzhouLandAgreementTransformResult')
        self.duplicateUrl = 0
        self.targetUrl = 'http://zzland.zhengzhou.gov.cn/xycrjg/index_{}.jhtml'
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
            for page in range(1, 2):
                priority = 4 - int(page)
                yield Request(self.targetUrl.format(page), method='GET', headers=self.header,
                              priority=priority,
                              callback=self.parse_index,
                              meta={'page': page, 'priority': priority},
                              # headers={'Content-Type': 'application/json'},
                              dont_filter=True
                              )
            yield Request('http://zzland.zhengzhou.gov.cn/xycrjg/index.jhtml', method='GET', headers=self.header,
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
            BT_18 = ''
            LY_19 = ''
            SJ_20 = ''
            XZQ_21 = ''
            DZJGH_22 = ''
            XMMC_23 = ''
            XMWZ_24 = ''
            MJ_25 = ''
            TDLY_26 = ''
            TSYT_27 = ''
            GDFS_28 = ''
            TDSYNX_29 = ''
            HYFL_30 = ''
            TDJB_31 = ''
            CJJG_32 = ''
            ZFQH_33 = ''
            YDZFRQ_34 = ''
            YDZFJE_35 = ''
            BZ_36 = ''
            TDSTQR_37 = ''
            SX_38 = ''
            XX_39 = ''
            YDJDSJ_40 = ''
            YDKGSJ_41 = ''
            YDJGSJ_42 = ''
            SJKGSJ_43 = ''
            SJJGSJ_44 = ''
            PZDW_45 = ''
            HTQDRQ_46 = ''

            # TODO 共有字段
            # 标题
            BT_18 = response.meta.get('title')
            LY = data.xpath('//div[@class="content-small-title"]/text()').extract_first()
            # 来源
            LY_19 = reFunction(f'来源：\s*([{self.reStr}]*)\s', LY)
            # 时间
            SJ_20 = reFunction(f'时间：\s*([{self.reStr}]*)\s', LY)

            # 解析 table 若出错 使用正则
            htmlTable = htmlTableTransformer()
            if '宗地编号' not in items and '行政区' not in items:
                try:
                    soup = BeautifulSoup(response.body.decode('utf-8'))
                    table = soup.find_all('table')[0]
                    if not table.tbody.find_all('tr')[0].find_all(text=re.compile("用地单位|受让人")):
                        table.tbody.find_all('tr')[0].extract()
                    tdsData = htmlTable.tableTrTdRegulationToList(table)

                    for _ in range(len(list(tdsData.values())[0])):
                        # 项目位置
                        XMWZ_24 = tdsData.get('土地座落')[_] if tdsData.get('土地座落') else tdsData.get('宗地位置')[_] if tdsData.get('宗地位置') else ''
                        # 面积
                        MJ_25_0 = tdsData.get('出让面积（公顷）')
                        MJ_25_1 = tdsData.get('出让面积')
                        MJ_25_2 = tdsData.get('出让/划拨面积')
                        MJ_25_ = list(filter(None, [MJ_25_0, MJ_25_1, MJ_25_2]))
                        MJ_25 = MJ_25_[0][_] if MJ_25_ else ''
                        # 土地用途
                        TSYT_27 = tdsData.get('土地用途')[_] if tdsData.get('土地用途') else tdsData.get('用途明细')[_] if tdsData.get('用途明细') else ''
                        # 供地方式
                        GDFS_28 = tdsData.get('供应方式')[_] if tdsData.get('供应方式') else ''
                        # 土地级别
                        TDJB_31 = tdsData.get('土地级别')[_] if tdsData.get('土地级别') else ''
                        # 成交价格
                        CJJG_32_0 = tdsData.get('出让价款')
                        CJJG_32_1 = tdsData.get('出让价款（万元）')
                        CJJG_32_2 = tdsData.get('出让/划拨价歀')
                        CJJG_32_ = list(filter(None, [CJJG_32_0, CJJG_32_1, CJJG_32_2]))
                        CJJG_32 = CJJG_32_[0][_] if CJJG_32_ else ''
                        # 土地使用权人
                        TDSTQR_37 = tdsData.get('用地单位')[_] if tdsData.get('用地单位') else tdsData.get('受让人')[_] if tdsData.get('受让人') else ''
                        # 合同签订日期
                        HTQDRQ_46 = tdsData.get('签订日期')[_] if tdsData.get('签订日期') else ''

                        # 爬取时间
                        crawlingTime = time.strftime("%Y-%m-%d", time.localtime())
                        # 爬取地址url
                        url = response.url
                        # 唯一标识
                        md5Mark = encrypt_md5(url + LY_19 + SJ_20)

                        # 是否需要判断重复 请求
                        if DUPLICATE_SWITCH:
                            if self.redisClient.isExist(md5Mark):  # 存在, 去重计数
                                self.duplicateUrl += 1

                        if self.duplicateUrl < 50:
                            # 重复效验通过, 存储数据
                            csvFile = [
                                BT_18,
                                LY_19,
                                SJ_20,
                                XZQ_21,
                                DZJGH_22,
                                XMMC_23,
                                XMWZ_24,
                                MJ_25,
                                TDLY_26,
                                TSYT_27,
                                GDFS_28,
                                TDSYNX_29,
                                HYFL_30,
                                TDJB_31,
                                CJJG_32,
                                ZFQH_33,
                                YDZFRQ_34,
                                YDZFJE_35,
                                BZ_36,
                                TDSTQR_37,
                                SX_38,
                                XX_39,
                                YDJDSJ_40,
                                YDKGSJ_41,
                                YDJGSJ_42,
                                SJKGSJ_43,
                                SJJGSJ_44,
                                PZDW_45,
                                HTQDRQ_46,
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
                # 行政区
                XZQ_21 = reFunction(f'行政区:([{self.reStr}]*)电子监管号', items)
                # 电子监管号
                DZJGH_22 = reFunction(f'电子监管号：([{self.reStr}]*)项目名称', items)
                # 项目名称
                XMMC_23_ = reFunction(f'项目名称:([{self.reStr}]*)项目位置', items)
                XMMC_23 = XMMC_23_ if XMMC_23_ else reFunction(f'宗地编号([{self.reStr}]*)地块位置', items)
                # 项目位置
                XMWZ_24_ = reFunction(f'项目位置:([{self.reStr}]*)面积(公顷):	', items)
                XMWZ_24 = XMWZ_24_ if XMWZ_24_ else reFunction(f'地块位置([{self.reStr}]*)土地用途', items)
                # 面积
                MJ_25_ = reFunction(f'面积\(公顷\):([{self.reStr}]*)土地来源', items)
                MJ_25 = MJ_25_ if MJ_25_ else reFunction(f'土地面积\(公顷\)([{self.reStr}]*)出让年限', items)
                # 土地来源
                TDLY_26 = reFunction(f'土地来源:([{self.reStr}]*)土地用途', items)
                # 土地用途
                TSYT_27_ = reFunction(f'土地用途:([{self.reStr}]*)供地方式', items)
                TSYT_27 = TSYT_27_ if TSYT_27_ else data.xpath('string(//table/tbody/tr[5]/td[1])').extract_first()
                # 供地方式
                GDFS_28 = reFunction(f'供地方式:([{self.reStr}]*)土地使用年限', items)
                # 土地使用年限
                TDSYNX_29_ = reFunction(f'土地使用年限:([{self.reStr}]*)行业分类', items)
                TDSYNX_29 = TDSYNX_29_ if TDSYNX_29_ else reFunction(f'出让年限([{self.reStr}]*)成交价\(万元\)', items)
                # 行业分类
                HYFL_30 = reFunction(f'行业分类:([{self.reStr}]*)土地级别', items)
                # 土地级别
                TDJB_31 = reFunction(f'土地级别:([{self.reStr}]*)成交价格\(万元\)', items)
                # 成交价格
                CJJG_32_ = reFunction(f'成交价格\(万元\):([{self.reStr}]*)分期支付约定', items)
                CJJG_32 = CJJG_32_ if CJJG_32_ else reFunction(f'成交价格\(万元\)([{self.reStr}]*)明细用途', items)
                # 分期支付约定—支付期号
                ZFQH_33 = data.xpath('//table/tbody/tr[10]/td[1]/text()').extract_first()
                # 分期支付约定—约定支付日期
                YDZFRQ_34 = data.xpath('//table/tbody/tr[10]/td[2]/text()').extract_first()
                # 分期支付约定—约定支付金额
                YDZFJE_35 = data.xpath('//table/tbody/tr[10]/td[3]/text()').extract_first()
                # 分期支付约定—备注
                BZ_36 = data.xpath('string(//table/tbody/tr[10]/td[4])').extract_first()
                # 土地使用权人
                TDSTQR_37_ = reFunction(f'土地使用权人:([{self.reStr}]*)约定容积率', items)
                TDSTQR_37 = TDSTQR_37_ if TDSTQR_37_ else reFunction(f'受让单位([{self.reStr}]*)备注', items)
                # 约定容积率——下限
                SX_38 = reFunction(f'下限:([{self.reStr}]*)上限', items)
                # 约定容积率——上限
                XX_39 = reFunction(f'上限:([{self.reStr}]*)约定交地时间', items)
                # 约定交地时间
                YDJDSJ_40 = reFunction(f'约定交地时间:([{self.reStr}]*)约定开工时间', items)
                # 约定开工时间
                YDKGSJ_41 = reFunction(f'约定开工时间:([{self.reStr}]*)约定竣工时间', items)
                # 约定竣工时间
                YDJGSJ_42 = reFunction(f'约定竣工时间:([{self.reStr}]*)实际开工时间', items)
                # 实际开工时间
                SJKGSJ_43 = reFunction(f'实际开工时间:([{self.reStr}]*)实际竣工时间', items)
                # 实际竣工时间
                SJJGSJ_44 = reFunction(f'实际竣工时间:([{self.reStr}]*)批准单位', items)
                # 批准单位
                PZDW_45 = reFunction(f'批准单位:([{self.reStr}]*)合同签订日期', items)
                # 合同签订日期
                HTQDRQ_46 = reFunction(f'合同签订日期:([{self.reStr}]*)\s', items)

                crawlingTime = time.strftime("%Y-%m-%d", time.localtime())
                # 爬取地址url
                url = response.url
                # 唯一标识
                md5Mark = encrypt_md5(url + LY_19 + SJ_20)

                # 是否需要判断重复 请求
                if DUPLICATE_SWITCH:
                    if self.redisClient.isExist(md5Mark):  # 存在, 去重计数
                        self.duplicateUrl += 1

                if self.duplicateUrl < 50:
                    # 重复效验通过, 存储数据
                    csvFile = [
                        BT_18,
                        LY_19,
                        SJ_20,
                        XZQ_21,
                        DZJGH_22,
                        XMMC_23,
                        XMWZ_24,
                        MJ_25,
                        TDLY_26,
                        TSYT_27,
                        GDFS_28,
                        TDSYNX_29,
                        HYFL_30,
                        TDJB_31,
                        CJJG_32,
                        ZFQH_33,
                        YDZFRQ_34,
                        YDZFJE_35,
                        BZ_36,
                        TDSTQR_37,
                        SX_38,
                        XX_39,
                        YDJDSJ_40,
                        YDKGSJ_41,
                        YDJGSJ_42,
                        SJKGSJ_43,
                        SJJGSJ_44,
                        PZDW_45,
                        HTQDRQ_46,
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

