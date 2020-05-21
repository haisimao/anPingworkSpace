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


class guiZhouTransformNoticeSpider(CrawlSpider):
    name = 'guiZhouTransformNotice'
    # 配置更换cookies时间
    COOKIES_SWITCH_TIME = datetime.datetime.now()

    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super(guiZhouTransformNoticeSpider, cls).__new__(cls)
            pathPage = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),'Logs/guiZhouTransformNoticePage.txt')
            cls.pathDetail = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))), 'data/贵州省自然资源厅_出让转让公告_贵州.csv')
            cls.filePage = open(pathPage, 'w+')
            if os.path.exists(cls.pathDetail):
                cls.fileDetail = open(cls.pathDetail, 'a+')
            else:
                cls.fileDetail = open(cls.pathDetail, 'a+')
                with open(cls.pathDetail, 'a+') as fp:
                    fp.write("""文件标题,发布时间,文章来源,索引号,信息分类,发布机构,发文日期,文号,是否有效,信息名称,正文标题,宗地编号/地块编号,宗地总面积/挂牌面积(m2),土地坐落/宗地坐落,使用年限,岀让年限,容积率,建筑密度(%)/建筑密度,绿地率/|绿化率(%),建筑限高/建筑限高(米),土地用途明细/土地用途,投资强度,保证金(万元)/保证金,估价报告备案号,起始价/起始价(万元),加价幅度,挂牌开始时间,挂牌截止时间,获取出让文件时间,获取出让文件地点,报名时间,报名地点,保证金截止时间,确认竞买资格时间,联系地址,联系人,联系电话,开户单位,开户银行,银行帐号,爬取时间,爬取地址url,唯一标识,\n""")
        return cls.instance

    def __init__(self):
        super(guiZhouTransformNoticeSpider, self).__init__()
        dispatcher.connect(self.CloseSpider, signals.spider_closed)
        self.targetUrl = 'http://zrzy.guizhou.gov.cn/zfxxgk/zfxxgkml/zdlyxxgkml/tdcrzrgg/index_{}.html'
        self.header = {'User-Agent': random.choice(agent_list)}
        self.reStr = '（）\w\.:： 。\(\)〔〕≤；，≥《》\-\/\%,、\.'


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
            for page in range(1, 13):
                priority = 13 - int(page)
                yield Request(self.targetUrl.format(page), method='GET', headers=self.header, priority=priority, callback=self.parse_index, meta={'page': page, 'priority': priority},
                              # body=requests_data,
                              # headers={'Content-Type': 'application/json'},
                              # dont_filter=True
                              )
            yield Request('http://zrzy.guizhou.gov.cn/zfxxgk/zfxxgkml/zdlyxxgkml/tdcrzrgg/index.html', method='GET', headers=self.header, callback=self.parse_index, meta={'page': 0},)
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
            dataItems = datas.xpath('//ul[@class="list_ul"]/li')
            for dataItem in dataItems:
                url = dataItem.xpath('a/@href').extract_first()
                yield Request(url, method='GET', callback=self.parse_detail,
                                  meta={
                                      'page': page,
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
            WJBT_1 = ''
            FBSJ_2 = ''
            WZLY_3 = ''
            SYH_4 = ''
            XXFL_5 = ''
            FBJG_6 = ''
            FBRQ_7 = ''
            WH_8 = ''
            SFYX_9 = ''
            XXMC_10 = ''
            ZWBT_11 = ''
            ZDBH_12 = ''
            ZDZMJ_13 = ''
            ZDZL_14 = ''
            SYNX_15 = ''
            CRNX_16 = ''
            RJL_17 = ''
            JZMD_18 = ''
            LDL_19 = ''
            JZXG_20 = ''
            TDYT_21 = ''
            TZQD_22 = ''
            BZJ_23 = ''
            GJBGBAH_24 = ''
            QSJ_25 = ''
            JJFD_26 = ''
            GPKSSJ_27 = ''
            GPJZSJ_28 = ''
            HQCRWJSJ_29 = ''
            HQCRWJDD_30 = ''
            BMSJ_31 = ''
            BMDD_32 = ''
            BZJJZSJ_33 = ''
            QRJMZGSJ_34 = ''
            LXDZ_35 = ''
            LXR_36 = ''
            LXDH_37 = ''
            KHDW_38 = ''
            KHYH_39 = ''
            YHZH_40 = ''

            # TODO 共有字段
            # 文件标题
            WJBT_1 = data.xpath('//div[@class="title"]/h1/text()').extract_first()
            # 发布时间  reFunction('', items)
            FBSJ_2 = reFunction('(\d{4}年\d{2}月\d{2}日 \d{2}:\d{2})\';', data.xpath('//div[@class="toolbar"]/script[1]/text()').extract_first())
            # 文章来源
            WZLY_3 = reFunction(f'document.write\(\'文章来源：([{self.reStr}]*)\'\);', data.xpath('//div[@class="toolbar"]/script[2]/text()').extract_first())
            # 索引号
            SYH_4  = data.xpath('//div[@class="xxgk_xl_top"]/ul/li[1]/span/text()').extract_first()
            # 信息分类
            XXFL_5 = data.xpath('//div[@class="xxgk_xl_top"]/ul/li[2]/span/text()').extract_first()
            # 发布机构
            FBJG_6 = reFunction(f'str_1 = "([{self.reStr}]*)";',data.xpath('//div[@class="xxgk_xl_top"]/ul/li[3]/span/script/text()').extract_first())
            # 发文日期
            FBRQ_7 = reFunction(f'str_1 = "([{self.reStr}]*)";',data.xpath('//div[@class="xxgk_xl_top"]/ul/li[4]/span/script/text()').extract_first())
            # 文号
            WH_8 = data.xpath('//div[@class="xxgk_xl_top"]/ul/li[5]/span/text()').extract_first()
            # 是否有效
            SFYX_9 = reFunction(f'var  isok=\'([{self.reStr}]*)\';',data.xpath('//div[@class="xxgk_xl_top"]/ul/li[6]/script/text()').extract_first())
            # 信息名称
            XXMC_10 = data.xpath('//div[@class="xxgk_xl_top"]/ul/li[7]/span/text()').extract_first()
            # 正文标题
            ZWBT_11 = data.xpath('//tr[@class="firstRow"]/td/text()').extract_first()

            if '主要规划指标' not in items:
                # item_ = reFunction('一、[\s\S]*二、', items)
                for item in ['宗地编号' + _ for _ in re.findall('一([\s\S]*)二', items)[0].split('宗地编号')[1:]]:
                    # 联系电话
                    LXDH_37 = reFunction(f'联系电话：\s*([{self.reStr}]*)\s*开户单位', reFunction('八、[\s\S]*', items))
                    # 宗地编号 / 地块编号
                    ZDBH_12_ = '|'.join(re.findall(f'[宗地块]*(?:[\s]*)编号：(?:[\s]*)([{self.reStr}]*)宗地总面积', item))
                    ZDBH_12 += '|' + ZDBH_12_ if ZDBH_12_ else '|' +  '|'.join(re.findall(f'[宗地块]*(?:[\s]*)编号：(?:[\s]*)([{self.reStr}]*)\s*', item))
                    # 宗地总面积 / 挂牌面积(m2)
                    ZDZMJ_13_ = '|'.join(re.findall(f'宗地总面积：(?:[\s]*)([{self.reStr}]*)宗地坐落', item))
                    ZDZMJ_13 += '|' + ZDZMJ_13_ if ZDZMJ_13_ else '|' +  '|'.join(re.findall(f'宗地总面积：(?:[\s]*)([{self.reStr}]*)\s*', item))
                    # 土地坐落 / 宗地坐落
                    ZDZL_14 += '|' + '|'.join(re.findall(f'宗地坐落：(?:[\s]*)([{self.reStr}]*)\s*出让年限', item))
                    # ZDZL_14 += '|' + ZDZL_14_ if ZDZL_14_ else '|'.join(re.findall(f'宗地坐落：(?:[\s]*)([{self.reStr}]*)\s*', item))

                    # 岀让年限
                    CRNX_16_ = '|'.join(re.findall(f'出让年限：(?:[\s]*)([{self.reStr}]*)\s*容积率', item))
                    CRNX_16 += '|' + reFunction('^[|]*\d{1,3}年', CRNX_16_)
                    # CRNX_16 += '|' + CRNX_16_ if CRNX_16_ else '|'.join(re.findall(f'出让年限：(?:[\s]*)([{self.reStr}]*)\s*', item))
                    # 容积率
                    RJL_17 += '|' + '|'.join(re.findall(f'容积率：(?:[\s]*)([{self.reStr}]*)\s*建筑密度\(%\)', item))
                    # RJL_17 += '|' + RJL_17_ if RJL_17_ else '|'.join(re.findall(f'容积率：(?:[\s]*)([{self.reStr}]*)\s*', item))
                    # 建筑密度( %) / 建筑密度
                    JZMD_18 += '|' + '|'.join(re.findall(f'建筑密度\(%\)：(?:[\s]*)([{self.reStr}]*)\s*绿化率', item))
                    # JZMD_18 += '|' + JZMD_18_ if JZMD_18_ else '|'.join(re.findall(f'建筑密度\(%\)：(?:[\s]*)([{self.reStr}]*)\s*', item))
                    # 绿地率 / | 绿化率( %)
                    LDL_19 += '|' + '|'.join(re.findall(f'绿化率\(%\)：(?:[\s]*)([{self.reStr}]*)\s*建筑限高', item))
                    # LDL_19 += '|' + LDL_19_ if LDL_19_ else '|'.join(re.findall(f'绿化率\(%\)：(?:[\s]*)([{self.reStr}]*)\s*', item))
                    # 建筑限高 / 建筑限高(米)
                    JZXG_20 += '|' + '|'.join(re.findall(f'建筑限高\(米\)：(?:[\s]*)([{self.reStr}]*)\s*土地用途明细', item))
                    # JZXG_20 += '|' + JZXG_20_ if JZXG_20_ else '|'.join(re.findall(f'建筑限高\(米\)：(?:[\s]*)([{self.reStr}]*)\s*', item))
                    # 土地用途明细 / 土地用途
                    TDYT_21 += '|' + '|'.join(re.findall(f'土地用途明细：(?:[\s]*)([{self.reStr}]*)\s*投资强度', item))
                    # TDYT_21 += '|' + TDYT_21_ if TDYT_21_ else '|'.join(re.findall(f'土地用途明细：(?:[\s]*)([{self.reStr}]*)\s*', item))
                    # 投资强度
                    TZQD_22 += '|' + '|'.join(re.findall(f'投资强度：(?:[\s]*)([{self.reStr}]*)\s*保证金', item))
                    # TZQD_22 += '|' + TZQD_22_ if TZQD_22_ else '|'.join(re.findall(f'投资强度：(?:[\s]*)([{self.reStr}]*)\s*', item))
                    # 保证金(万元) / 保证金
                    BZJ_23 += '|' + '|'.join(re.findall(f'保证金：(?:[\s]*)([{self.reStr}]*)\s*估价报告备案号', item))
                    # BZJ_23 += '|' + BZJ_23_ if BZJ_23_ else '|'.join(re.findall(f'保证金：(?:[\s]*)([{self.reStr}]*)\s*', item))
                    # 估价报告备案号
                    GJBGBAH_24_ = '|'.join(re.findall(f'估价报告备案号(?:[\s]*)([{self.reStr}]*)\s*现状土地条件', item))
                    GJBGBAH_24__ = '|' + GJBGBAH_24_ if GJBGBAH_24_ else '|'.join(re.findall(f'估价报告备案号(?:[\s]*)([{self.reStr}]*)\s*起始价', item))
                    GJBGBAH_24 += '|' + reFunction('^\w{10, 16}', GJBGBAH_24__)

                    # 起始价 / 起始价(万元)
                    QSJ_25 += '|' + '|'.join(re.findall(f'起始价：(?:[\s]*)([{self.reStr}]*)\s*加价幅度', item))
                    # QSJ_25 += '|' + QSJ_25_ if QSJ_25_ else '|'.join(re.findall(f'起始价：(?:[\s]*)([{self.reStr}]*)\s*', item))
                    # 加价幅度
                    JJFD_26 += '|' + '|'.join(re.findall(f'加价幅度：(?:[\s]*)([{self.reStr}]*)\s*挂牌开始时间', item))
                    # JJFD_26 += '|' + JJFD_26_ if JJFD_26_ else '|'.join(re.findall(f'加价幅度：(?:[\s]*)([{self.reStr}]*)\s', item))
                    # 挂牌开始时间
                    GPKSSJ_27 += '|' + '|'.join(re.findall(f'挂牌开始时间：(?:[\s]*)([{self.reStr}]*)\s*挂牌截止时间', item))
                    # GPKSSJ_27 += '|' + GPKSSJ_27_ if GPKSSJ_27_ else '|'.join(re.findall(f'挂牌开始时间：(?:[\s]*)([{self.reStr}]*)\s*', item))
                    # 挂牌截止时间
                    GPJZSJ_28 += '|' + '|'.join(re.findall(f'挂牌截止时间：(?:[\s]*)([{self.reStr}]*)\s*(?:宗地编号|二)', item))
                    # GPJZSJ_28 += '|' + GPJZSJ_28_ if GPJZSJ_28_ else '|'.join(re.findall(f'挂牌截止时间：(?:[\s]*)([{reStr}]*)(?:宗地编号|二|\s*)', item))
            else:
                soup = BeautifulSoup(response.body.decode('utf-8'))
                table = soup.find('table')
                if not table:
                    for item in ['宗地编号' + _ for _ in re.findall('一([\s\S]*)二', items)[0].split('宗地编号')[1:]]:
                        # 联系电话
                        LXDH_37 = reFunction(f'联系电话：\s*([{self.reStr}]*)\s*开户单位', reFunction('八、[\s\S]*', items))
                        # 宗地编号 / 地块编号
                        ZDBH_12_ = '|'.join(re.findall(f'[宗地块]*(?:[\s]*)编号：(?:[\s]*)([{self.reStr}]*)宗地总面积', item))
                        ZDBH_12__ = ZDBH_12_ if ZDBH_12_ else '|' +  '|'.join(re.findall(f'[宗地块]*(?:[\s]*)编号：(?:[\s]*)([{self.reStr}]*)\s*', item))
                        ZDBH_12 += ZDBH_12__
                        # 宗地总面积 / 挂牌面积(m2)
                        ZDZMJ_13_ = '|'.join(re.findall(f'宗地总面积：(?:[\s]*)([{self.reStr}]*)宗地坐落', item))
                        ZDZMJ_13__ = ZDZMJ_13_ if ZDZMJ_13_ else '|' +  '|'.join(re.findall(f'宗地总面积：(?:[\s]*)([{self.reStr}]*)\s*', item))
                        ZDZMJ_13 += ZDZMJ_13__
                        # 土地坐落 / 宗地坐落
                        ZDZL_14 += '|' + '|'.join(re.findall(f'宗地坐落：(?:[\s]*)([{self.reStr}]*)\s*出让年限', item))
                        # ZDZL_14 += '|' + ZDZL_14_ if ZDZL_14_ else '|'.join(re.findall(f'宗地坐落：(?:[\s]*)([{self.reStr}]*)\s*', item))
                        # 岀让年限
                        # CRNX_16 += '|' + CRNX_16_ if CRNX_16_ else '|'.join(re.findall(f'出让年限：(?:[\s]*)([{self.reStr}]*)\s*', item))

                        CRNX_16_ = '|'.join(re.findall(f'出让年限：(?:[\s]*)([{self.reStr}]*)\s*容积率', item))
                        CRNX_16 += '|' + reFunction('^[|]*\d{1,3}年', CRNX_16_)
                        # 容积率
                        RJL_17 += '|' + '|'.join(re.findall(f'容积率：(?:[\s]*)([{self.reStr}]*)\s*建筑密度\(%\)', item))
                        # RJL_17 += '|' + RJL_17_ if RJL_17_ else '|'.join(re.findall(f'容积率：(?:[\s]*)([{self.reStr}]*)\s*', item))
                        # 建筑密度( %) / 建筑密度
                        JZMD_18 += '|' + '|'.join(re.findall(f'建筑密度\(%\)：(?:[\s]*)([{self.reStr}]*)\s*绿化率', item))
                        # JZMD_18 += '|' + JZMD_18_ if JZMD_18_ else '|'.join(re.findall(f'建筑密度\(%\)：(?:[\s]*)([{self.reStr}]*)\s*', item))
                        # 绿地率 / | 绿化率( %)
                        LDL_19 += '|' + '|'.join(re.findall(f'绿化率\(%\)：(?:[\s]*)([{self.reStr}]*)\s*建筑限高', item))
                        # LDL_19 += '|' + LDL_19_ if LDL_19_ else '|'.join(re.findall(f'绿化率\(%\)：(?:[\s]*)([{self.reStr}]*)\s*', item))
                        # 建筑限高 / 建筑限高(米)
                        JZXG_20 += '|' + '|'.join(re.findall(f'建筑限高\(米\)：(?:[\s]*)([{self.reStr}]*)\s*土地用途明细', item))
                        # JZXG_20 += '|' + JZXG_20_ if JZXG_20_ else '|'.join(re.findall(f'建筑限高\(米\)：(?:[\s]*)([{self.reStr}]*)\s*', item))
                        # 土地用途明细 / 土地用途
                        TDYT_21 += '|' + '|'.join(re.findall(f'土地用途明细：(?:[\s]*)([{self.reStr}]*)\s*投资强度', item))
                        # TDYT_21 += '|' + TDYT_21_ if TDYT_21_ else '|'.join(re.findall(f'土地用途明细：(?:[\s]*)([{self.reStr}]*)\s*', item))
                        # 投资强度
                        TZQD_22 += '|' + '|'.join(re.findall(f'投资强度：(?:[\s]*)([{self.reStr}]*)\s*保证金', item))
                        # TZQD_22 += '|' + TZQD_22_ if TZQD_22_ else '|'.join(re.findall(f'投资强度：(?:[\s]*)([{self.reStr}]*)\s*', item))
                        # 保证金(万元) / 保证金
                        BZJ_23 += '|' + '|'.join(re.findall(f'保证金：(?:[\s]*)([{self.reStr}]*)\s*估价报告备案号', item))
                        # BZJ_23 += '|' + BZJ_23_ if BZJ_23_ else '|'.join(re.findall(f'保证金：(?:[\s]*)([{self.reStr}]*)\s*', item))
                        # 估价报告备案号  现状土地条件
                        GJBGBAH_24_ = '|'.join(re.findall(f'估价报告备案号(?:[\s]*)([{self.reStr}]*)\s*现状土地条件', item))
                        GJBGBAH_24__= '|' + GJBGBAH_24_ if GJBGBAH_24_ else '|'.join(re.findall(f'估价报告备案号(?:[\s]*)([{self.reStr}]*)\s*起始价', item))
                        GJBGBAH_24 += '|' + reFunction('^\w{10, 16}', GJBGBAH_24__)

                        # 起始价 / 起始价(万元)
                        QSJ_25 += '|' + '|'.join(re.findall(f'起始价：(?:[\s]*)([{self.reStr}]*)\s*加价幅度', item))
                        # QSJ_25 += '|' + QSJ_25_ if QSJ_25_ else '|'.join(re.findall(f'起始价：(?:[\s]*)([{self.reStr}]*)\s*', item))
                        # 加价幅度
                        JJFD_26 += '|' + '|'.join(re.findall(f'加价幅度：(?:[\s]*)([{self.reStr}]*)\s*挂牌开始时间', item))
                        # JJFD_26 += '|' + JJFD_26_ if JJFD_26_ else '|'.join(re.findall(f'加价幅度：(?:[\s]*)([{self.reStr}]*)\s', item))
                        # 挂牌开始时间
                        GPKSSJ_27 += '|' + '|'.join(re.findall(f'挂牌开始时间：(?:[\s]*)([{self.reStr}]*)\s*挂牌截止时间', item))
                        # GPKSSJ_27 += '|' + GPKSSJ_27_ if GPKSSJ_27_ else '|'.join(re.findall(f'挂牌开始时间：(?:[\s]*)([{self.reStr}]*)\s*', item))
                        # 挂牌截止时间
                        GPJZSJ_28 += '|' + '|'.join(re.findall(f'挂牌截止时间：(?:[\s]*)([{self.reStr}]*)\s*(?:宗地编号|二)', item))
                        # GPJZSJ_28 += '|' + GPJZSJ_28_ if GPJZSJ_28_ else '|'.join(re.findall(f'挂牌截止时间：(?:[\s]*)([{reStr}]*)(?:宗地编号|二|\s*)', item))
                else:
                    # 联系电话
                    LXDH_37 = reFunction(f'联系电话：\s*([{self.reStr}]*)\s', reFunction('八|七、[\s\S]*', items))
                    htmlTable = htmlTableTransformer()
                    tdData = htmlTable.tableTrTdRegulation(table)
                    # 宗地编号 / 地块编号
                    ZDBH_12 = tdData.get('地块编号')
                    # 宗地总面积 / 挂牌面积(m2)
                    ZDZMJ_13 = tdData.get(r'挂牌面积(m2)')
                    # 土地坐落 / 宗地坐落
                    ZDZL_14 = tdData.get('土地坐落')
                    # 使用年限
                    SYNX_15 = tdData.get('使用年限')
                    # 起始价 / 起始价(万元)
                    QSJ_25 = tdData.get('起始价（万元）')
                    # 土地用途明细 / 土地用途
                    TDYT_21 = tdData.get('土地用途')
                    # 保证金(万元) / 保证金
                    BZJ_23 = tdData.get('保证金（万元）')
                    ZYGHZB = tdData.get('主要规划指标')
                    # 容积率
                    RJL_17 = reFunction('容积率[：]*\s*([（）\w\.:： \(\)〔〕≤≥\-\/\%,、\.﹪]*)[；。，]?', ZYGHZB)
                    # 建筑密度( %) / 建筑密度
                    JZMD_18 = reFunction('建筑密度[：]*\s*([（）\w\.:： \(\)〔〕≤≥\-\/\%,、\.﹪]*)容积率', ZYGHZB)
                    # 绿地率 / | 绿化率( %)
                    LDL_19 = reFunction('绿地率[：]*\s*([（）\w\.:： \(\)〔〕≤≥\-\/\%,、\.﹪]*)[；。，]?', ZYGHZB)
                    # 建筑限高 / 建筑限高(米)
                    JZXG_20 = reFunction('建筑限高[：]*\s*([（）\w\.:： \(\)〔〕≤≥\-\/\%,、\.﹪]*)[；。，]?', ZYGHZB)

            # TODO
            # 获取出让文件时间
            HQCRWJSJ_29 = reFunction(f'申请人可于(?:[\s]*)([{self.reStr}]*)到', reFunction('四、[\s\S]*五、', items))
            # 获取出让文件地点
            HQCRWJDD_30 =reFunction(f'申请人可于(?:[\s]*)(?:[{self.reStr}]*)到\s*([{self.reStr}]*)获取 挂牌', reFunction('四、[\s\S]*五、', items))
            # 报名时间
            BMSJ_31 = reFunction(f'申请人可于(?:[\s]*)([{self.reStr}]*)到', reFunction('五、[\s\S]*六、', items))
            # 报名地点
            BMDD_32 = reFunction(f'申请人可于(?:[\s]*)(?:[{self.reStr}]*)到\s*([{self.reStr}]*)向我局提交书面申请', reFunction('五、[\s\S]*六、', items))
            # 保证金截止时间
            BZJJZSJ_33 = reFunction(f'截止时间为(?:[\s]*)([{self.reStr}]*)\s*。经审', reFunction('五、[\s\S]*六、', items))
            # 确认竞买资格时间
            QRJMZGSJ_34 = reFunction(f'我局将在\s*([{self.reStr}]*)\s*前确认其竞买资格', reFunction('五、[\s\S]*六、', items))

            # TODO 联系地址
            LXDZ_35 = reFunction(f'联系地址：\s*([{self.reStr}]*)\s*联 系', reFunction('八、[\s\S]*', items))
            # 联系人
            LXR_36 = reFunction(f'联 系\s*人：\s*([{self.reStr}]*)\s*联系电话', reFunction('八、[\s\S]*', items))
            # 开户单位
            KHDW_38 = reFunction(f'开户单位：\s*([{self.reStr}]*)\s*开户银行', reFunction('八、[\s\S]*', items))
            # 开户银行
            KHYH_39 = reFunction(f'开户银行：\s*([{self.reStr}]*)\s*银行帐号', reFunction('八、[\s\S]*', items))
            # 银行帐号
            YHZH_40 = reFunction('^\d{17}', reFunction(f'银行帐号：\s*([{self.reStr}]*)\s*', reFunction('八、[\s\S]*', items)))

            # 爬取时间
            crawlingTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            # 爬取地址url
            url = response.url
            # 唯一标识
            md5Mark = encrypt_md5(url)

            # 存储数据
            csvFile = [WJBT_1,
FBSJ_2,
WZLY_3,
SYH_4,
XXFL_5,
FBJG_6,
FBRQ_7,
WH_8,
SFYX_9,
XXMC_10,
ZWBT_11,
ZDBH_12,
ZDZMJ_13,
ZDZL_14,
SYNX_15,
CRNX_16,
RJL_17,
JZMD_18,
LDL_19,
JZXG_20,
TDYT_21,
TZQD_22,
BZJ_23,
GJBGBAH_24,
QSJ_25,
JJFD_26,
GPKSSJ_27,
GPJZSJ_28,
HQCRWJSJ_29,
HQCRWJDD_30,
BMSJ_31,
BMDD_32,
BZJJZSJ_33,
QRJMZGSJ_34,
LXDZ_35,
LXR_36,
LXDH_37,
KHDW_38,
KHYH_39,
YHZH_40,
crawlingTime,
url,
md5Mark,
]
            results = ''
            for _ in csvFile:
                try:
                    if _ and _ != '|' * len(_):
                        results += _.replace(',', ' ').replace('\n', '').replace('\r', '') + ','
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
