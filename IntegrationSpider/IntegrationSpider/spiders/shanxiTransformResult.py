# shanxiTransformResult
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


class shanxiTransformNoticeSpider(CrawlSpider):
    # TODO
    name = 'shanxiTransformResult'
    # 配置更换cookies时间
    COOKIES_SWITCH_TIME = datetime.datetime.now()

    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super(shanxiTransformNoticeSpider, cls).__new__(cls)
            # TODO
            pathPage = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),'Logs/shanxiTransformResultPage.txt')
            # TODO
            cls.pathDetail = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))), 'data/山西省自然资源厅_出让结果_山西.csv')
            cls.filePage = open(pathPage, 'w+')
            if os.path.exists(cls.pathDetail):
                cls.fileDetail = open(cls.pathDetail, 'a+')
            else:
                cls.fileDetail = open(cls.pathDetail, 'a+')
                with open(cls.pathDetail, 'a+') as fp:
                    # TODO
                    fp.write("""文件标题,时间,来源,文件编号,宗地编号,编号,地块位置,土地位置,土地面积(亩),土地面积(平方米),土地用途,成交价(万元),竞得人,公示期,联系单位,单位地址,邮政编码,联系电话,爬取地址url,唯一标识,\n""")
        return cls.instance

    def __init__(self):
        super(shanxiTransformNoticeSpider, self).__init__()
        dispatcher.connect(self.CloseSpider, signals.spider_closed)
        # TODO
        self.redisClient = RedisClient('shanxi', 'shanxiTransformResult')
        self.duplicateUrl = 0
        self.targetUrl = 'http://zrzyt.shanxi.gov.cn/zwgk/zwgkjbml/tdgl_836/crjg/index_{}.shtml'
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
            for page in range(1, 14):
                yield Request(self.targetUrl.format(page), method='GET', headers=self.header,
                              callback=self.parse_index,
                              meta={'page': page},
                              # headers={'Content-Type': 'application/json'},
                              dont_filter=True
                              )
        except Exception as e:
            self.log(f'当前爬取页数失败, {datetime.datetime.now()}, 错误: {e}\n{traceback.format_exc()}', level=logging.ERROR)
            raise IntegrationException('爬取阻塞,请重启')
        else:
            yield Request('http://zrzyt.shanxi.gov.cn/zwgk/zwgkjbml/tdgl_836/crjg/index.shtml', method='GET',
                      headers=self.header,
                      callback=self.parse_index,
                      meta={'page': 'index', 'priority': 1},
                      # headers={'Content-Type': 'application/json'},
                      dont_filter=True
                      )

    def parse_index(self, response):
        '''
        拿到总页数,
        :param response:
        :return:
        '''
        try:
            page = response.meta.get('page')
            datas = Selector(text=response.body.decode('utf-8'))
            dataItems = datas.xpath('//ul[@class="zwgk_right_content"]/li')
            for dataItem in dataItems:
                title = dataItem.xpath('a/text()').extract_first()
                url = 'http://zrzyt.shanxi.gov.cn/zwgk/zwgkjbml/tdgl_836/crjg' + dataItem.xpath('a/@href').extract_first()[1:]
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
        # TODO 主动关闭爬虫问题
        try:
            data = Selector(text=response.body.decode('utf-8'))
            items = str(data.xpath('string(.)').extract()[0]).replace('\xa0', '').replace('\u3000', '')
            WJBT_27 = ''
            SJ_28 = ''
            LY_29 = ''
            WJBT_30 = ''
            ZDBH_31 = ''
            BH_32 = ''
            DKWZ_33 = ''
            TDWZ_34 = ''
            TDMJM_35 = ''
            TDMJPFM_36 = ''
            TDYT_37 = ''
            CJJ_38 = ''
            JDR_39 = ''
            GSQ_40 = ''
            LXDW_41 = ''
            DWDZ_42 = ''
            YZBM_43 = ''
            LXDH_44 = ''
            # TODO 共有字段  reFunction(f'时间：\s*([{self.reStr}]*)\s', LY)
            # 文件标题
            WJBT_27 = response.meta.get('title')
            # 时间
            SJ_28 = data.xpath('//div[@class="ztzx_frame_subtitle_l"]/span[1]/text()').extract_first()
            # 来源
            LY_29 = data.xpath('//div[@class="ztzx_frame_subtitle_l"]/span[2]/text()').extract_first()
            # 文件编号
            WJBT_30 = data.xpath('//div[@class="ztzx_frame_content"]/div[1]/text()').extract_first()
            # 公示期
            GSQ_40 = reFunction(f'公示期：([（）\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)。', items)
            # 联系单位
            LXDW_41 = reFunction('联系单位：([（）\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)
            # 单位地址
            DWDZ_42 = reFunction('单位地址：([（）\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)
            # 邮政编码
            YZBM_43 = reFunction('邮政编码：([（）\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)
            # 联系电话
            LXDH_44 = reFunction('联系电话：([（）\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)
            # 爬取时间
            crawlingTime = time.strftime("%Y-%m-%d", time.localtime())
            # 爬取地址url
            url = response.url
            # 唯一标识
            md5Mark = encrypt_md5(url + WJBT_27 + SJ_28)

            soup = BeautifulSoup(response.body.decode('utf-8').replace('thead', 'tbody'))
            table = soup.find('table')
            htmlTable = htmlTableTransformer()
            if table:
                if '竣工时间' in items:
                    try:
                        tdData = htmlTable.tableTrTdUNregulationToList(table)
                        for _ in range(len(list(tdData.values())[0])):
                            # 宗地编号
                            ZDBH_31 = tdData.get('地块编号')[_] if tdData.get('地块编号') else ''
                            # 地块位置
                            DKWZ_33 = tdData.get('位置')[_] if tdData.get('位置') else ''
                            # 土地位置
                            TDWZ_34 = tdData.get('位置')[_] if tdData.get('位置') else ''
                            # 土地面积(亩)
                            TDMJM_35 = tdData.get('出让面积平方米/亩')[_] if tdData.get('出让面积平方米/亩') else ''
                            # 土地面积(平方米)
                            TDMJPFM_36 = tdData.get(list(tdData.keys())[7])[_] if tdData.get(list(tdData.keys())[7]) else ''
                            # 土地用途
                            TDYT_37 = tdData.get('用途')[_] if tdData.get('用途') else ''
                            # 成交价(万元)
                            CJJ_38 = tdData.get('成交价(万元)')[_] if tdData.get('成交价(万元)') else tdData.get('成交价（万元）')[_] if tdData.get('成交价（万元）') else ''
                            # 竞得人
                            JDR_39 = tdData.get('受让人')[_] if tdData.get('受让人') else ''
                            # 写入数据
                            if self.name in DUPLICATE_SWITCH_LIST:
                                if self.redisClient.isExist(md5Mark):  # 存在, 去重计数
                                    self.duplicateUrl += 1

                            if self.duplicateUrl < 50:
                                if TDYT_37:
                                    # 重复效验通过, 存储数据
                                    csvFile = [
                                        WJBT_27,
                                        SJ_28,
                                        LY_29,
                                        WJBT_30,
                                        ZDBH_31,
                                        BH_32,
                                        DKWZ_33,
                                        TDWZ_34,
                                        TDMJM_35,
                                        TDMJPFM_36,
                                        TDYT_37,
                                        CJJ_38,
                                        JDR_39,
                                        GSQ_40,
                                        LXDW_41,
                                        DWDZ_42,
                                        YZBM_43,
                                        LXDH_44,
                                        crawlingTime,
                                        url,
                                        md5Mark,
                                    ]
                                    results = ''
                                    for _ in csvFile:
                                        try:
                                            if _ and _ != '|' * len(_):
                                                results += _.replace(',', ' ').replace('\n', '').replace('\r',
                                                                                                         '').replace(
                                                    r'\xa0', '').replace('\xa0', '') + ','
                                            else:
                                                results += ','
                                        except Exception as e:
                                            results += ','
                                            self.log(
                                                f'{getVariableName(_).pop()}字段解析出错, 错误: {e}\n{traceback.format_exc()}',
                                                level=logging.ERROR)
                                    with open(self.pathDetail, 'a+') as fp:
                                        fp.write(results)
                                        fp.write('\n')
                                    self.log(f'数据获取成功', level=logging.INFO)
                                    yield
                    except:
                        for tdData in table.find_all('tr')[2:]:
                            # 宗地编号
                            ZDBH_31 = tdData.find_all('td')[4].string.strip()
                            # 地块位置
                            DKWZ_33 = tdData.find_all('td')[5].string.strip()
                            # 土地位置
                            TDWZ_34 = tdData.find_all('td')[5].string.strip()
                            # 土地面积(亩)
                            TDMJM_35 = tdData.find_all('td')[6].string.strip()
                            # 土地面积(平方米)
                            TDMJPFM_36 = tdData.find_all('td')[7].string.strip()
                            # 土地用途
                            TDYT_37 = tdData.find_all('td')[8].string.strip()
                            # 成交价(万元)
                            CJJ_38 = tdData.find_all('td')[9].string.strip()
                            # 竞得人
                            JDR_39 = tdData.find_all('td')[3].string.strip()
                            # 写入数据
                            if self.name in DUPLICATE_SWITCH_LIST:
                                if self.redisClient.isExist(md5Mark):  # 存在, 去重计数
                                    self.duplicateUrl += 1

                            if self.duplicateUrl < 50:
                                if TDYT_37:
                                    # 重复效验通过, 存储数据
                                    csvFile = [
                                        WJBT_27,
                                        SJ_28,
                                        LY_29,
                                        WJBT_30,
                                        ZDBH_31,
                                        BH_32,
                                        DKWZ_33,
                                        TDWZ_34,
                                        TDMJM_35,
                                        TDMJPFM_36,
                                        TDYT_37,
                                        CJJ_38,
                                        JDR_39,
                                        GSQ_40,
                                        LXDW_41,
                                        DWDZ_42,
                                        YZBM_43,
                                        LXDH_44,
                                        crawlingTime,
                                        url,
                                        md5Mark,
                                    ]
                                    results = ''
                                    for _ in csvFile:
                                        try:
                                            if _ and _ != '|' * len(_):
                                                results += _.replace(',', ' ').replace('\n', '').replace('\r',
                                                                                                         '').replace(
                                                    r'\xa0', '').replace('\xa0', '') + ','
                                            else:
                                                results += ','
                                        except Exception as e:
                                            results += ','
                                            self.log(
                                                f'{getVariableName(_).pop()}字段解析出错, 错误: {e}\n{traceback.format_exc()}',
                                                level=logging.ERROR)
                                    with open(self.pathDetail, 'a+') as fp:
                                        fp.write(results)
                                        fp.write('\n')
                                    self.log(f'数据获取成功', level=logging.INFO)
                                    yield
                elif '转让方' not in items:
                    if len(table.find_all('tr')[1].find_all('td')) < 5:
                        table.find_all('tr')[1].extract()
                        table.find_all('tr')[0].find_all('td')[-1].extract()
                    tdData = htmlTable.tableTrTdRegulationToList(table)
                    for _ in range(len(list(tdData.values())[0])):
                        # 宗地编号
                        ZDBH_31 = tdData.get('宗地编号')[_] if tdData.get('宗地编号') else ''
                        # 编号
                        BH_32 = tdData.get('编号')[_] if tdData.get('编号') else ''
                        # 地块位置
                        DKWZ_33 = tdData.get('地块位置')[_] if tdData.get('地块位置') else ''
                        # 土地位置
                        TDWZ_34 = tdData.get('土地位置')[_] if tdData.get('土地位置') else ''
                        # 土地面积(亩)
                        TDMJM_35 = tdData.get('土地面积(亩)')[_] if tdData.get('土地面积(亩)') else ''
                        # 土地面积(平方米)
                        TDMJPFM_36 = tdData.get('土地面积(平方米)')[_] if tdData.get('土地面积(平方米)') else ''
                        # 土地用途
                        TDYT_37 = tdData.get('土地用途')[_] if tdData.get('土地用途') else ''
                        # 成交价(万元)
                        CJJ_38 = tdData.get('成交价(万元)')[_] if tdData.get('成交价(万元)') else tdData.get('成交价（万元）')[_] if tdData.get('成交价（万元）') else ''
                        # 竞得人
                        JDR_39 = tdData.get('竞得人')[_] if tdData.get('竞得人') else ''

                        # 写入数据
                        if self.name in DUPLICATE_SWITCH_LIST:
                            if self.redisClient.isExist(md5Mark):  # 存在, 去重计数
                                self.duplicateUrl += 1

                        if self.duplicateUrl < 50:
                            if TDYT_37:
                                # 重复效验通过, 存储数据
                                csvFile = [
                                    WJBT_27,
                                    SJ_28,
                                    LY_29,
                                    WJBT_30,
                                    ZDBH_31,
                                    BH_32,
                                    DKWZ_33,
                                    TDWZ_34,
                                    TDMJM_35,
                                    TDMJPFM_36,
                                    TDYT_37,
                                    CJJ_38,
                                    JDR_39,
                                    GSQ_40,
                                    LXDW_41,
                                    DWDZ_42,
                                    YZBM_43,
                                    LXDH_44,
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
                elif '地块基本情况' in items:
                    # 宗地编号
                    ZDBH_31 = reFunction('宗地编号\s*([（）【】\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)
                    # 地块位置
                    DKWZ_33 = reFunction('地块位置\s*([（）【】\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)
                    # 土地面积(亩)
                    TDMJM_35 = reFunction('土地面积\(公顷\)\s*([（）【】\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)
                    # 土地用途
                    TDYT_37 = reFunction('土地用途\s*([（）【】\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)
                    # 成交价(万元)
                    CJJ_38 = reFunction('成交价\(万元\)\s*([（）【】\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)
                    # 竞得人
                    JDR_39 = reFunction('受让单位\s*([（）【】\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)

                    # 写入数据
                    if self.name in DUPLICATE_SWITCH_LIST:
                        if self.redisClient.isExist(md5Mark):  # 存在, 去重计数
                            self.duplicateUrl += 1

                    if self.duplicateUrl < 50:
                        if TDYT_37:
                            # 重复效验通过, 存储数据
                            csvFile = [
                                WJBT_27,
                                SJ_28,
                                LY_29,
                                WJBT_30,
                                ZDBH_31,
                                BH_32,
                                DKWZ_33,
                                TDWZ_34,
                                TDMJM_35,
                                TDMJPFM_36,
                                TDYT_37,
                                CJJ_38,
                                JDR_39,
                                GSQ_40,
                                LXDW_41,
                                DWDZ_42,
                                YZBM_43,
                                LXDH_44,
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
            elif '转让方' in items:
                    # 编号
                    BH_32 = reFunction('不动产权登记证号：([（）【】\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)
                    # 地块位置
                    DKWZ_33 = reFunction('宗地位置：([（）\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)
                    # 土地面积(平方米)
                    TDMJPFM_36 = reFunction('面\s*积：([（）\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)
                    # 土地用途
                    TDYT_37 = reFunction('土地用途：([（）\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)
                    # 成交价(万元)
                    # CJJ_38
                    # 竞得人
                    JDR_39 = reFunction('受让方：([（）\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)
                    # 写入数据
                    if self.name in DUPLICATE_SWITCH_LIST:
                        if self.redisClient.isExist(md5Mark):  # 存在, 去重计数
                            self.duplicateUrl += 1

                    if self.duplicateUrl < 50:
                        if TDYT_37:
                            # 重复效验通过, 存储数据
                            csvFile = [
                                WJBT_27,
                                SJ_28,
                                LY_29,
                                WJBT_30,
                                ZDBH_31,
                                BH_32,
                                DKWZ_33,
                                TDWZ_34,
                                TDMJM_35,
                                TDMJPFM_36,
                                TDYT_37,
                                CJJ_38,
                                JDR_39,
                                GSQ_40,
                                LXDW_41,
                                DWDZ_42,
                                YZBM_43,
                                LXDH_44,
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




