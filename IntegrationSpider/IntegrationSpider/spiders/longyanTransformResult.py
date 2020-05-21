# longyanTransformResult
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
from pydispatch import dispatcher
from scrapy import Request, FormRequest, Selector
from scrapy.spiders import CrawlSpider
import logging

from scrapy import signals
# from scrapy.xlib.pydispatch import dispatcher
# from urllib.request import unquote
import json
# from requests_html import HTMLSession

from IntegrationSpider.items import IntegrationPageItem
from IntegrationSpider.settings import DUPLICATE_SWITCH, DUPLICATE_SWITCH_LIST
from IntegrationSpider.useragent import agent_list
from IntegrationSpider.utils_ import IntegrationException, getSesion, encrypt_md5
from SpiderTools.Tool import reFunction, getVariableName
from SpiderTools.tableAnalysis import htmlTableTransformer
from db import RedisClient


class longyanTransformResultSpider(CrawlSpider):
    # TODO
    name = 'longyanTransformResult'
    # 配置更换cookies时间
    COOKIES_SWITCH_TIME = datetime.datetime.now()

    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super(longyanTransformResultSpider, cls).__new__(cls)
            # TODO
            pathPage = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),'Logs/longyanTransformResultPage.txt')
            # TODO
            cls.pathDetail = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))), 'data/龙岩市公共资源交易中心_出让结果_龙岩.csv')
            cls.filePage = open(pathPage, 'w+')
            if os.path.exists(cls.pathDetail):
                cls.fileDetail = open(cls.pathDetail, 'a+')
            else:
                cls.fileDetail = open(cls.pathDetail, 'a+')
                with open(cls.pathDetail, 'a+') as fp:
                    # TODO
                    fp.write("""文件标题,信息时间,正文标题,公告编号,出让时间,公告类型,地块编号,地块位置,土地用途,土地面积(公顷),出让年限,成交价(万元),受让单位,土地现状,土地使用条件,备注,公示期,联系方式,单位地址,邮政编码,联系电话,联系人,联系单位,电子邮件,爬取地址url,唯一标识,\n""")
        return cls.instance

    def __init__(self):
        super(longyanTransformResultSpider, self).__init__()
        dispatcher.connect(self.CloseSpider, signals.spider_closed)
        # TODO
        self.redisClient = RedisClient('longyan', 'longyanTransformResult')
        self.duplicateUrl = 0
        # TODO
        self.targetUrl = 'https://www.lyggzy.com.cn/lyztb/tdky/084001/?pageing={}'
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
            for page in range(1, 7):
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
            dataItems = datas.xpath('//div[@class="r-bd"]/ul[1]/li')
            for dataItem in dataItems:
                title = dataItem.xpath('a/text()').extract_first()
                url = 'https://www.lyggzy.com.cn/' + dataItem.xpath('a/@href').extract_first()[1:]
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
            htmlTable = htmlTableTransformer()
            WJBT_48 = ''
            XXSJ_49 = ''
            ZWBT_50 = ''
            GGBH_51 = ''
            CRSJ_52 = ''
            GGNX_53 = ''
            DKBH_54 = ''
            DKWZ_55 = ''
            TDYT_56 = ''
            TDMJ_57 = ''
            CRNX_58 = ''
            CJJ_59 = ''
            SRDW_60 = ''
            TDXZTJ_61 = ''
            TDSYTJ_62 = ''
            BZ_63 = ''
            GSQ_64 = ''
            LXFS_65 = ''
            DWDZ_66 = ''
            YZBM_67 = ''
            LXDH_68 = ''
            LXR_69 = ''
            LXDW_77 = ''
            DZYJ_70 = ''
            # TODO 共有字段  reFunction(f'时间：\s*([{self.reStr}]*)\s', LY)
            # 文件标题
            WJBT_48 = response.meta.get('title').strip()
            # 信息时间
            XXSJ_49 = reFunction('[\d\-]*', data.xpath('//p[@class="sub-cp"]/text()').extract_first())
            # 正文标题
            ZWBT_50 = WJBT_48
            # 公告编号
            GGBH_51 = ''.join(data.xpath('//div[@class="substance"]/p[position() <5]/.//*[contains(text(),"号")]/ancestor::p/.//text()').extract())
            # 出让时间
            CRSJ_52 = reFunction('定于\s*([（）【】\w\.—\(\)〔〕㎡≤≥《》\-\/\%\.﹪]*)[,；，、在]', items)
            # 公告类型
            GGNX_53 = '出让结果'

            # 爬取时间
            crawlingTime = time.strftime("%Y-%m-%d", time.localtime())
            # 爬取地址url
            url = response.url
            # 唯一标识
            md5Mark = encrypt_md5(url + WJBT_48 + XXSJ_49)

            # 公示期
            GSQ_64 = reFunction('公示期：*\s*([（）\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)[\s。]', items)
            # 联系方式
            # LXFS_65
            # 联系单位
            LXDW_77 = reFunction('联系单位：*([（）\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)
            # 单位地址
            DWDZ_66 = reFunction('单位地址：*([（）\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)
            # 邮政编码
            YZBM_67 = reFunction('邮政编码：*([（）\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)
            # 联系电话
            LXDH_68 = reFunction('联系电话：*([（）\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)
            # 联系人
            LXR_69 = reFunction('联\s*系\s*人：*([（）\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)
            # 电子邮件
            DZYJ_70 = reFunction('电子邮件：*([（）\w\.:： —\(\)〔〕㎡㎡≤≥《》@\-\/\%,；，、\.﹪]*)\s', items)

            if '宗地编号' in items or '土地位置' in items:
                soup = BeautifulSoup(response.body.decode('utf-8'))
                table = soup.find('table')
                tdData = htmlTable.tableTrTdRegulationToList(table)
                for _ in range(len(list(tdData.values())[0])):
                    # 地块编号
                    DKBH_54 = tdData.get('宗地编号')[_] if tdData.get('宗地编号') else ''
                    # 地块位置
                    DKWZ_55 = tdData.get('宗地位置')[_] if tdData.get('宗地位置') else tdData.get('土地位置')[_] if tdData.get('土地位置') else ''
                    # 土地用途
                    TDYT_56 = tdData.get('土地用途')[_] if tdData.get('土地用途') else tdData.get('规划土地用途')[_] if tdData.get('规划土地用途') else ''
                    # 土地面积(公顷)
                    TDMJ_57 = tdData.get('土地面积（m2）')[_] if tdData.get('土地面积（m2）') else tdData.get('出让土地面积（㎡）')[_] if tdData.get('出让土地面积（㎡）') else ''
                    # 出让年限
                    CRNX_58 = tdData.get('使用年限')[_] if tdData.get('使用年限') else tdData.get('出让年限')[_] if tdData.get('出让年限') else ''
                    # 成交价(万元)
                    CJJ_59 =  tdData.get('成交价(万元)')[_] if tdData.get('成交价(万元)') else tdData.get('成交价（人民币）')[_] if tdData.get('成交价（人民币）') else ''
                    # 受让单位
                    SRDW_60 = tdData.get('受让单位')[_] if tdData.get('受让单位') else tdData.get('竞买人（单位）')[_] if tdData.get('竞买人（单位）') else ''
                    # 土地使用条件
                    TDSYTJ_62 = tdData.get('土地使用条件')[_] if tdData.get('土地使用条件') else ''

                    # 数据写入
                    if self.name in DUPLICATE_SWITCH_LIST:
                        if self.redisClient.isExist(md5Mark):  # 存在, 去重计数
                            self.duplicateUrl += 1

                    if self.duplicateUrl < 50:
                        if DKWZ_55:
                            # 重复效验通过, 存储数据
                            csvFile = [
                                WJBT_48,
                                XXSJ_49,
                                ZWBT_50,
                                GGBH_51,
                                CRSJ_52,
                                GGNX_53,
                                DKBH_54,
                                DKWZ_55,
                                TDYT_56,
                                TDMJ_57,
                                CRNX_58,
                                CJJ_59,
                                SRDW_60,
                                TDXZTJ_61,
                                TDSYTJ_62,
                                BZ_63,
                                GSQ_64,
                                LXFS_65,
                                DWDZ_66,
                                YZBM_67,
                                LXDH_68,
                                LXR_69,
                                LXDW_77,
                                DZYJ_70,
                                crawlingTime,
                                url,
                                md5Mark,
                            ]
                            results = ''
                            for _ in csvFile:
                                try:
                                    if _ and _ != '|' * len(_):
                                        results += _.replace(',', ' ').replace('\n', '').replace('\t', '').replace(
                                            '\r',
                                            '').replace(
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
            elif '地块编号' in items:
                for item in ['地块编号' + _ for _ in re.findall('一([\s\S]*)二、', items)[0].split('地块编号')[1:]]:
                    # 地块编号
                    DKBH_54 = reFunction('地块编号：*\s*([\w\-]*)\s', item)
                    # 地块位置
                    DKWZ_55 = reFunction('地块位置：*\s*([（）\w\.:：—\(\)〔〕㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', item)
                    # 土地用途
                    TDYT_56 = reFunction('土地用途：*\s*([（）\w\.:：—\(\)〔〕㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', item)
                    # 土地面积(公顷)
                    TDMJ_57 = reFunction('土地面积\(公顷\)：*\s*([（）\w\.:：—\(\)〔〕㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', item)
                    # 出让年限
                    CRNX_58 = reFunction('出让年限：*\s*([（）\w\.:：—\(\)〔〕㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', item)
                    # 成交价(万元)
                    CJJ_59 = reFunction('成交价\(万元\)：*\s*([（）\w\.:：—\(\)〔〕㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', item)
                    # 受让单位
                    SRDW_60 = reFunction('受让单位：*\s*([（）\w\.:：—\(\)〔〕㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', item)
                    # 土地现状
                    TDXZTJ_61 = reFunction('土地现状：*\s*([（）\w\.:：—\(\)〔〕㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', item)
                    # 土地使用条件
                    TDSYTJ_62 = reFunction('土地使用条件：*\s*([（）\w\.:：—\(\)〔〕㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', item)
                    # 备注
                    BZ_63 = reFunction('备注：*\s*([（）\w\.:：—\(\)〔〕㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', item)

                    # 数据写入
                    if self.name in DUPLICATE_SWITCH_LIST:
                        if self.redisClient.isExist(md5Mark):  # 存在, 去重计数
                            self.duplicateUrl += 1

                    if self.duplicateUrl < 50:
                        if DKWZ_55:
                            # 重复效验通过, 存储数据
                            csvFile = [
                                WJBT_48,
                                XXSJ_49,
                                ZWBT_50,
                                GGBH_51,
                                CRSJ_52,
                                GGNX_53,
                                DKBH_54,
                                DKWZ_55,
                                TDYT_56,
                                TDMJ_57,
                                CRNX_58,
                                CJJ_59,
                                SRDW_60,
                                TDXZTJ_61,
                                TDSYTJ_62,
                                BZ_63,
                                GSQ_64,
                                LXFS_65,
                                DWDZ_66,
                                YZBM_67,
                                LXDH_68,
                                LXR_69,
                                LXDW_77,
                                DZYJ_70,
                                crawlingTime,
                                url,
                                md5Mark,
                            ]
                            results = ''
                            for _ in csvFile:
                                try:
                                    if _ and _ != '|' * len(_):
                                        results += _.replace(',', ' ').replace('\n', '').replace('\t', '').replace(
                                            '\r',
                                            '').replace(
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
            else:
                # 地块位置
                DKWZ_55 = reFunction('地理位置：*\s*([（）\w\.:：—\(\)〔〕㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)
                # 出让年限
                CRNX_58 = reFunction('出让年限：*\s*([（）\w\.:：—\(\)〔〕㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)
                # 成交价(万元)
                CJJ_59 = reFunction('成交价格（人民币）：*\s*([（）\w\.:：—\￥ (\)〔〕㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)
                # 受让单位
                SRDW_60 = reFunction('竞得人名称：*\s*([（）\w\.:：—\(\)〔〕㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)
                # 土地现状
                TDXZTJ_61 = reFunction('土地现状：*\s*([（）\w\.:：—\(\)〔〕㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)

                # 数据写入
                if self.name in DUPLICATE_SWITCH_LIST:
                    if self.redisClient.isExist(md5Mark):  # 存在, 去重计数
                        self.duplicateUrl += 1

                if self.duplicateUrl < 50:
                    if DKWZ_55:
                        # 重复效验通过, 存储数据
                        csvFile = [
                            WJBT_48,
                            XXSJ_49,
                            ZWBT_50,
                            GGBH_51,
                            CRSJ_52,
                            GGNX_53,
                            DKBH_54,
                            DKWZ_55,
                            TDYT_56,
                            TDMJ_57,
                            CRNX_58,
                            CJJ_59,
                            SRDW_60,
                            TDXZTJ_61,
                            TDSYTJ_62,
                            BZ_63,
                            GSQ_64,
                            LXFS_65,
                            DWDZ_66,
                            YZBM_67,
                            LXDH_68,
                            LXR_69,
                            LXDW_77,
                            DZYJ_70,
                            crawlingTime,
                            url,
                            md5Mark,
                        ]
                        results = ''
                        for _ in csvFile:
                            try:
                                if _ and _ != '|' * len(_):
                                    results += _.replace(',', ' ').replace('\n', '').replace('\t', '').replace(
                                        '\r',
                                        '').replace(
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





