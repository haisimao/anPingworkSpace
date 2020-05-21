# shanxiTransformNotice
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


class shanxiTransformNoticeSpider(CrawlSpider):
    # TODO
    name = 'shanxiTransformNotice'
    # 配置更换cookies时间
    COOKIES_SWITCH_TIME = datetime.datetime.now()

    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super(shanxiTransformNoticeSpider, cls).__new__(cls)
            # TODO
            pathPage = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),'Logs/shanxiTransformNoticePage.txt')
            # TODO
            cls.pathDetail = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))), 'data/山西省自然资源厅_出让公告_山西.csv')
            cls.filePage = open(pathPage, 'w+')
            if os.path.exists(cls.pathDetail):
                cls.fileDetail = open(cls.pathDetail, 'a+')
            else:
                cls.fileDetail = open(cls.pathDetail, 'a+')
                with open(cls.pathDetail, 'a+') as fp:
                    # TODO
                    fp.write("""公告类型,文件标题,时间,来源,正文标题,宗地编号,土地位置,出让面积(m2),绿化用地,道路用地,土地用途,岀让年限,容积率,建筑密度,绿地率,建筑空间,起始价(万元),保证金(万元),竞价幅度(万元),报名时间起止日期,挂牌开始时间,挂牌截止时间,保证金到账截止时间,联系地址,联系人,联系电话,爬取地址url,唯一标识,\n""")
        return cls.instance

    def __init__(self):
        super(shanxiTransformNoticeSpider, self).__init__()
        dispatcher.connect(self.CloseSpider, signals.spider_closed)
        # TODO
        self.redisClient = RedisClient('shanxi', 'shanxiTransformNotice')
        self.duplicateUrl = 0
        self.targetUrl = 'http://zrzyt.shanxi.gov.cn/zwgk/zwgkjbml/tdgl_836/crgg/index_{}.shtml'
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
            for page in range(1, 33):
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
            yield Request('http://zrzyt.shanxi.gov.cn/zwgk/zwgkjbml/tdgl_836/crgg/index.shtml', method='GET',
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
                url = 'http://zrzyt.shanxi.gov.cn/zwgk/zwgkjbml/tdgl_836/crgg' + dataItem.xpath('a/@href').extract_first()[1:]
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
            GGLX_1 = ''
            WJBT_2 = ''
            SJ_3 = ''
            LY_4 = ''
            ZWBT_5 = ''
            ZDBH_6 = ''
            TDWZ_7 = ''
            CRMJ_8 = ''
            LHYD_9 = ''
            DLYD_10 = ''
            TDYT_11 = ''
            CRNX_12 = ''
            RJL_13 = ''
            JZMD_14 = ''
            LDL_15 = ''
            JZKJ_16 = ''
            QSJ_17 = ''
            BZJ_18 = ''
            JJFD_19 = ''
            BMRQ_20 = ''
            GPRQ_21 = ''
            GPJZSJ_22 = ''
            BZJDZSJ_23 = ''
            LXDZ_24 = ''
            LXR_25 = ''
            LXDH_26 = ''
            # TODO 共有字段  reFunction(f'时间：\s*([{self.reStr}]*)\s', LY)
            # 公告类型
            GGLX_1 = '出让公告'
            # 文件标题
            WJBT_2 = response.meta.get('title')
            # 时间
            SJ_3 = data.xpath('//div[@class="ztzx_frame_subtitle_l"]/span[1]/text()').extract_first()
            # 来源
            LY_4 = data.xpath('//div[@class="ztzx_frame_subtitle_l"]/span[2]/text()').extract_first()
            # 正文标题
            ZWBT_5 = data.xpath('//div[@class="ztzx_frame_content"]/div[1]/text()').extract_first()
            # 爬取时间
            crawlingTime = time.strftime("%Y-%m-%d", time.localtime())
            # 爬取地址url
            url = response.url
            # 唯一标识
            md5Mark = encrypt_md5(url + WJBT_2 + SJ_3)
            # 报名时间起止日期
            BMRQ_20 = reFunction(f'报名申请时间：\s*([\w]*)；', items) if reFunction(f'报名申请时间：\s*([\w]*)；', items) else reFunction(f'申请人可于(\w*)，向我局提交书面申请', items) if reFunction(f'申请人可于(\w*)，向我局提交书面申请', items) else reFunction(f'申请时间为：(\w*)', items) if reFunction(f'申请时间为：(\w*)', items) else reFunction(f'申请人可于(\w*)到', items)
            GPTime = reFunction(f'网上挂牌（报价）时间：\s*([\w]*)', items) if reFunction(f'网上挂牌（报价）时间：\s*([\w]*)', items) else reFunction(f'挂牌时间为：\s*([\w]*)', items)
            try:
                if GPTime:
                    # 挂牌开始时间
                    GPRQ_21 = GPTime.split('至')[0]
                    # 挂牌截止时间
                    GPJZSJ_22 = GPTime.split('至')[1]
                else:
                    GPRQ_21 = reFunction(f'挂牌时间为：\s*([\s\S]*)', reFunction('六、([\s\S]*)七、', items))
                    GPJZSJ_22 = reFunction(f'挂牌时间为：\s*([\s\S]*)', reFunction('六、([\s\S]*)七、', items))
            except Exception as e:
                self.log(f'详情页数据挂牌时间解析失败, 请求:{response.url}, 信息: {e}', level=logging.DEBUG)
                GPRQ_21 = ''
                GPJZSJ_22 = ''
            # 保证金到账截止时间
            BZJDZSJ_23 = reFunction(f'保证金到账截止时间为：\s*([\w]*)', items) if reFunction(f'保证金到账截止时间为：\s*([\w]*)', items) else reFunction(f'保证金交纳截止时间：\s*([\w]*)', items) if reFunction(f'保证金交纳截止时间：\s*([\w]*)', items) else reFunction(f'保证金的截止时间为\s*([\w]*)', items)
            # 联系地址
            LXDZ_24 = reFunction('联系地址：\s*([（）\w\.:： \(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)', items) if reFunction(f'联系地址：\s*([（）\w\.:： \(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)', items) else reFunction('单位地址：\s*([（）\w\.\(\)〔〕㎡≤≥《》\-\/\%,；，、\.﹪]*)', items)
            # 联系人
            LXR_25 = reFunction(f'联\s系\s人：\s*([（）\w\.:： \(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)', items)
            # 联系电话
            LXDH_26 = reFunction(f'联系电话：\s*([（）\w\.:： \(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)', items)
            if '挂牌出让宗地的基本情况和规划指标等要求' not in items and '宗地编号' not in items:
                # 处理 table 情况
                soup = BeautifulSoup(response.body.decode('utf-8'))
                table = soup.find('table')
                try:
                    tdReplace = table.tbody.find_all('tr')[0].find('td', colspan='4') if table.tbody.find_all('tr')[0].find('td', colspan='4') else table.tbody.find_all('tr')[0].find('td', colspan="2")
                    number = table.tbody.find_all('tr')[0].index(tdReplace)
                    tdList = table.tbody.find_all('tr')[1].find_all('td')
                    for _ in range(1, len(tdList) + 1):
                        table.tbody.find_all('tr')[0].insert(number + _, tdList[_ - 1])
                    tdReplace.extract()
                    table.tbody.find_all('tr')[1].extract()
                except:
                    soup = BeautifulSoup(response.body.decode('utf-8'))
                    table = soup.find('table')
                    tdReplace = table.thead.find_all('tr')[0].find('td', colspan='4') if table.thead.find_all('tr')[0].find('td', colspan='4') else table.thead.find_all('tr')[0].find('td', colspan="2")
                    number = table.thead.find_all('tr')[0].index(tdReplace)
                    tdList = table.thead.find_all('tr')[1].find_all('td')
                    for _ in range(1, len(tdList) + 1):
                        table.thead.find_all('tr')[0].insert(number + _, tdList[_ - 1])
                    tdReplace.extract()
                    table.thead.find_all('tr')[1].extract()
                    table.tbody.insert(0, table.thead.find_all('tr')[0])  # 插入 thead 的内容
                    table.thead.extract()
                htmlTable = htmlTableTransformer()
                try:
                    tdData = htmlTable.tableTrTdRegulationToList(table)
                    if not tdData and 'thead' in items:  # 如果没有拿到 则可能存在 thead
                        soup = BeautifulSoup(response.body.decode('utf-8'))
                        table = soup.find('table')
                        tdReplace = table.thead.find_all('tr')[0].find('td', colspan='4') if table.thead.find_all('tr')[0].find('td', colspan='4') else table.thead.find_all('tr')[0].find('td', colspan="2")
                        number = table.thead.find_all('tr')[0].index(tdReplace)
                        tdList = table.thead.find_all('tr')[1].find_all('td')
                        for _ in range(1, len(tdList) + 1):
                            table.thead.find_all('tr')[0].insert(number + _, tdList[_ - 1])
                        tdReplace.extract()
                        table.thead.find_all('tr')[1].extract()
                        table.tbody.insert(0, table.thead.find_all('tr')[0])  # 插入 thead 的内容
                        table.thead.extract()
                        htmlTable = htmlTableTransformer()
                except:
                        tdData = {}
                for _ in range(len(list(tdData.values())[0])):
                    # 宗地编号
                    ZDBH_6 = tdData.get('编号')[_] if tdData.get('编号') else ''
                    # 土地位置
                    TDWZ_7 = tdData.get('土地位置')[_] if tdData.get('土地位置') else ''
                    # 出让面积(m2)
                    CRMJ_8_0 = tdData.get('土地面积')
                    CRMJ_8_1 = tdData.get('土地面积(平方米)')
                    CRMJ_8_ = list(filter(None, [CRMJ_8_0, CRMJ_8_1]))
                    CRMJ_8 = CRMJ_8_[0][_] if CRMJ_8_ else ''
                    # 土地用途
                    TDYT_11 = tdData.get('土地用途')[_] if tdData.get('土地用途') else ''
                    # 岀让年限
                    CRNX_12 = tdData.get('出让年限（年）')[_] if tdData.get('出让年限（年）') else ''
                    # 容积率
                    RJL_13 = tdData.get('容积率')[_] if tdData.get('容积率') else tdData.get('容 积 率')[_] if tdData.get('容 积 率') else ''
                    # 建筑密度
                    # JZMD_14
                    # 绿地率
                    LDL_15 = tdData.get('绿化率')[_] if tdData.get('绿化率') else ''
                    # 建筑空间
                    JZKJ_16 = tdData.get('控制高度（m）')[_] if tdData.get('控制高度（m）') else tdData.get('建筑限高（m）')[_] if tdData.get('建筑限高（m）') else ''
                    # 起始价(万元)
                    QSJ_17 = tdData.get('挂牌起始价(万元)')[_] if tdData.get('挂牌起始价(万元)') else ''
                    # 保证金(万元)
                    BZJ_18 = tdData.get('竞买保证金（万元）')[_] if tdData.get('竞买保证金（万元）') else tdData.get('竞买保证金(万元)')[_] if tdData.get('竞买保证金(万元)') else ''
                    # 竞价幅度(万元)
                    JJFD_19 = tdData.get('増价幅度（万元/次）')[_] if tdData.get('増价幅度（万元/次）') else ''
                    # 是否需要判断重复 请求
                    if DUPLICATE_SWITCH:
                        if self.redisClient.isExist(md5Mark):  # 存在, 去重计数
                            self.duplicateUrl += 1

                    if self.duplicateUrl < 50:
                        if ZDBH_6 and TDYT_11:
                            # 重复效验通过, 存储数据
                            csvFile = [
                                GGLX_1,
                                WJBT_2,
                                SJ_3,
                                LY_4,
                                ZWBT_5,
                                ZDBH_6,
                                TDWZ_7,
                                CRMJ_8,
                                LHYD_9,
                                DLYD_10,
                                TDYT_11,
                                CRNX_12,
                                RJL_13,
                                JZMD_14,
                                LDL_15,
                                JZKJ_16,
                                QSJ_17,
                                BZJ_18,
                                JJFD_19,
                                BMRQ_20,
                                GPRQ_21,
                                GPJZSJ_22,
                                BZJDZSJ_23,
                                LXDZ_24,
                                LXR_25,
                                LXDH_26,
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
            # TODO 判断
            elif '挂牌出让宗地的基本情况和规划指标等要求' in items:
                for item in re.split('\d、', reFunction('一、挂牌出让宗地的基本情况和规划指标等要求：([\s\S]*)二、', items)):
                    # TODO
                    if not item.strip():
                        continue
                    # 宗地编号
                    ZDBH_6 = reFunction(f'^([（）\w\.:： ％\(\)〔〕㎡≤≥《》\-\/\%,；，、\.﹪]*)宗地位于', item)
                    # 土地位置
                    TDWZ_7 = reFunction(f'宗地位于([（）\w\.:： \(\)〔〕㎡≤≥《》\-\/\%；、\.﹪]*)，', item)
                    # 出让面积(m2)
                    CRMJ_8 = reFunction(f'土地出让面积([（）\w\.:： ％\(\)〔〕㎡≤≥《》\-\/\%；、\.﹪]*)，', item)
                    # 土地用途
                    TDYT_11 = reFunction(f'宗地规划用途为([（）\w\.:： ％\(\)〔〕㎡≤≥《》\-\/\%；、\.﹪]*)，', item)
                    # 岀让年限
                    CRNX_12 = reFunction(f'宗地土地出让年期([（）\w\.:： —\(\)，〔〕％㎡≤≥《》\-\/\%；、\.﹪]*)。', item)
                    # 容积率
                    RJL_13 = reFunction(f'容积率([（）\w\.:： \(\)％〔〕㎡≤≥《》\-\/\%；、\.﹪]*)，', item)
                    # 建筑密度
                    JZMD_14 = reFunction(f'建筑密度([（）\w\.:： \(\)〔〕％㎡≤≥《》\-\/\%；、\.﹪]*)，', item)
                    # 绿地率
                    LDL_15 = reFunction(f'绿地率([（）\w\.:： \(\)〔〕％㎡≤≥《》\-\/\%；、\.﹪]*)，', item)
                    # 建筑空间
                    JZKJ_16 = reFunction(f'建筑空间([（）\w\.:： —\(\)〔〕％㎡≤≥《》\-\/\%；、\.﹪]*)，', item)
                    # 起始价(万元)
                    QSJ_17 = reFunction(f'本宗地起始价([（）\w\.:： —\(\)〔〕％㎡≤≥《》\-\/\%；、\.﹪]*)，', item)
                    # 保证金(万元)
                    BZJ_18 = reFunction(f'竞买保证金([（）\w\.:： —\(\)〔〕％㎡≤≥《》\-\/\%；、\.﹪]*)', item)
                    # 竞价幅度(万元)
                    JJFD_19 = reFunction(f'增价幅度([（）\w\.:： —\(\)〔〕％㎡≤≥《》\-\/\%；、\.﹪]*)', item)
                    # 是否需要判断重复 请求
                    if DUPLICATE_SWITCH:
                        if self.redisClient.isExist(md5Mark):  # 存在, 去重计数
                            self.duplicateUrl += 1

                    if self.duplicateUrl < 50:
                        if ZDBH_6 and TDYT_11:
                            # 重复效验通过, 存储数据
                            csvFile = [
                                GGLX_1,
                                WJBT_2,
                                SJ_3,
                                LY_4,
                                ZWBT_5,
                                ZDBH_6,
                                TDWZ_7,
                                CRMJ_8,
                                LHYD_9,
                                DLYD_10,
                                TDYT_11,
                                CRNX_12,
                                RJL_13,
                                JZMD_14,
                                LDL_15,
                                JZKJ_16,
                                QSJ_17,
                                BZJ_18,
                                JJFD_19,
                                BMRQ_20,
                                GPRQ_21,
                                GPJZSJ_22,
                                BZJDZSJ_23,
                                LXDZ_24,
                                LXR_25,
                                LXDH_26,
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
            elif '挂牌出让地块基本情况' in items and '宗地编号' in items:
                for item in ['宗地编号' + _ for _ in re.findall('一([\s\S]*)二、', items)[0].split('宗地编号')[1:]]:
                    # 宗地编号
                    ZDBH_6 = reFunction(f'宗地编号为([（）\w\.:： —\(\)〔〕％㎡≤≥《》\-\/\%；、\.﹪]*),', item)
                    # 土地位置
                    TDWZ_7 = reFunction(f'该地块([（）\w\.:： —\(\)〔〕％㎡≤≥《》，\-\/\%；、\.﹪]*)。出让面积', item)
                    # 出让面积(m2)
                    CRMJ_8 = reFunction(f'出让面积：*([（）\w\.:： —\(\)〔〕％㎡≤≥《》\-\/\%、\.﹪]*)；', item)
                    # 绿化用地
                    LHYD_9 = reFunction(f'绿化用地：*([（）\w\.:： —\(\)〔〕％㎡≤≥《》\-\/\%、\.﹪]*)；', item)
                    # 道路用地
                    DLYD_10 = reFunction(f'道路用地：*([（）\w\.:： —\(\)〔〕％㎡≤≥《》\-\/\%、\.﹪]*)；', item)
                    # 土地用途
                    TDYT_11 = reFunction(f'用途：*([（）\w\.:： —\(\)〔〕％㎡≤≥《》\-\/\%、\.﹪]*)；', item)
                    # 岀让年限
                    CRNX_12 = reFunction(f'出让年限：*([（）\w\.:： —\(\)〔〕％㎡≤≥《》\-\/\%、\.﹪]*)；', item)
                    # 容积率
                    RJL_13 = reFunction(f'容积率：*([（）\w\.:： ，—\(\)〔〕％㎡≤≥《》\-\/\%、\.﹪]*)；', item)
                    # 建筑密度
                    JZMD_14 = reFunction(f'建筑密度：*([（）\w\.:： —\(\)〔〕％㎡≤≥《》\-\/\%、\.﹪]*)；', item)
                    # 绿地率
                    LDL_15 = reFunction(f'绿地率：*([（）\w\.:： —\(\)〔〕％㎡≤≥《》\-\/\%、\.﹪]*)；', item) if reFunction(f'绿地率：*([（）\w\.:： —\(\)〔〕％㎡≤≥《》\-\/\%、\.﹪]*)；', item) else reFunction(f'绿地率（%）([（）\w\.:： —\(\)〔〕％㎡≤≥《》\-\/\%、\.﹪]*)；', item)
                    # 起始价(万元)
                    QSJ_17 = reFunction(f'起始价为：*([（）\w\.:： —\(\)〔〕％㎡≤≥《》\-\/\%、\.﹪]*)，', item)
                    # 保证金(万元)
                    BZJ_18 = reFunction(f'竞买保证金为：*([（）\w\.:： —\(\)〔〕％㎡≤≥《》\-\/\%、\.﹪]*)，', item)
                    # 竞价幅度(万元)
                    JJFD_19 = reFunction(f'竞价幅度为：*([（）\w\.:： —\(\)〔〕％㎡≤≥《》\-\/\%、\.﹪]*)。', item)
                    # 是否需要判断重复 请求
                    if DUPLICATE_SWITCH:
                        if self.redisClient.isExist(md5Mark):  # 存在, 去重计数
                            self.duplicateUrl += 1

                    if self.duplicateUrl < 50:
                        if ZDBH_6 and TDYT_11:
                            # 重复效验通过, 存储数据
                            csvFile = [
                                GGLX_1,
                                WJBT_2,
                                SJ_3,
                                LY_4,
                                ZWBT_5,
                                ZDBH_6,
                                TDWZ_7,
                                CRMJ_8,
                                LHYD_9,
                                DLYD_10,
                                TDYT_11,
                                CRNX_12,
                                RJL_13,
                                JZMD_14,
                                LDL_15,
                                JZKJ_16,
                                QSJ_17,
                                BZJ_18,
                                JJFD_19,
                                BMRQ_20,
                                GPRQ_21,
                                GPJZSJ_22,
                                BZJDZSJ_23,
                                LXDZ_24,
                                LXR_25,
                                LXDH_26,
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
            else:
                if '宗地编号' in items and '地块基本情况' not in items:
                    for item in ['宗地编号' + _ for _ in re.findall('一([\s\S]*)二、', items)[0].split('宗地编号')[1:]]:
                        # 宗地编号
                        ZDBH_6 = reFunction(f'宗地编号：*\s*([（）\w\.:： —\(\)〔〕％㎡≤≥《》\-\/\%、\.﹪]*)\s', item)
                        # 土地位置
                        TDWZ_7 = reFunction(f'宗地坐落：*\s*([（）\w\.:： —\(\)〔〕、，,；，、％㎡≤≥《》\-\/\%、\.﹪]*)\s', item)
                        # 出让面积(m2)
                        CRMJ_8 = reFunction(f'宗地\s*总*面积：*\s*([（）\w\.:： —\(\)〔〕、，,；，、％㎡≤≥《》\-\/\%、\.﹪]*)\s', item)
                        # 土地用途
                        TDYT_11 = reFunction(f'土地用途[明细]*：*\s*([（）\w\.:： —\(\)〔〕、，,；，、％㎡≤≥《》\-\/\%、\.﹪]*)\s', item)
                        # 岀让年限
                        CRNX_12 = reFunction(f'出让年限：*\s*([（）\w\.:： —\(\)〔〕、，,；，、％㎡≤≥《》\-\/\%、\.﹪]*)\s', item)
                        # 容积率
                        RJL_13 = reFunction(f'容积率：*\s*([（）\w\.:： —\(\)〔〕、，,；，、％㎡≤≥《》\-\/\%、\.﹪]*)\s', item)
                        # 建筑密度
                        JZMD_14 = reFunction(f'建筑密度\(%\)：*\s*([（）\w\.:： —\(\)〔〕、，,；，、％㎡≤≥《》\-\/\%、\.﹪]*)\s', item)
                        # 绿地率
                        LDL_15 = reFunction(f'绿[地化]率\(%\)：*\s*([（）\w\.:： —\(\)〔〕、，,；，、％㎡≤≥《》\-\/\%、\.﹪]*)\s', item) if reFunction(f'绿[地化]率\(%\)：*\s*([（）\w\.:： —\(\)〔〕、，,；，、％㎡≤≥《》\-\/\%、\.﹪]*)\s', item) else reFunction(f'绿地率（%）\s*([（）\w\.:： —\(\)〔〕、，,；，、％㎡≤≥《》\-\/\%、\.﹪]*)\s', item)
                        # 建筑空间
                        JZKJ_16 = reFunction(f'建筑限高\(米\)：*\s*([（）\w\.:： —\(\)〔〕、，,；，、％㎡≤≥《》\-\/\%、\.﹪]*)\s', item)
                        # 起始价(万元)
                        QSJ_17 = reFunction(f'起始价：*\s*([（）\w\.:： —\(\)〔〕、，,；，、％㎡≤≥《》\-\/\%、\.﹪]*)\s', item)
                        # 保证金(万元)
                        BZJ_18 = reFunction(f'保证金：*\s*([（）\w\.:： —\(\)〔〕、，,；，、％㎡≤≥《》\-\/\%、\.﹪]*)\s', item)
                        # 竞价幅度(万元)
                        JJFD_19 = reFunction(f'加价幅度：*\s*([（）\w\.:： —\(\)〔〕、，,；，、％㎡≤≥《》\-\/\%、\.﹪]*)\s', item)
                        # 挂牌开始时间
                        GPRQ_21 = reFunction(f'挂牌开始时间：*\s*([（）\w\.:： —\(\)〔〕、，,；，、％㎡≤≥《》\-\/\%、\.﹪]*)\s', item)
                        # 挂牌截止时间
                        GPJZSJ_22 = reFunction(f'挂牌截止时间：*\s*([（）\w\.:： —\(\)〔〕、，,；，、％㎡≤≥《》\-\/\%、\.﹪]*)\s', item)
                        # 联系地址
                        LXDZ_24 = reFunction(f'联系地址：\s*([（）\w\.\(\)〔〕㎡≤≥《》\-\/\%,；，、\.﹪]*)', items).split('联')[0] if reFunction(f'联系地址：\s*([（）\w\.\(\)〔〕㎡≤≥《》\-\/\%,；，、\.﹪]*)', items) else ''
                        # 联系人
                        LXR_25 = reFunction(f'联\s系\s人：\s*([（）\w\.\(\)〔〕㎡≤≥《》\-\/\%,；，、\.﹪]*)', items).split('联')[0] if reFunction(f'联\s系\s人：\s*([（）\w\.\(\)〔〕㎡≤≥《》\-\/\%,；，、\.﹪]*)', items) else ''
                        # 联系电话
                        LXDH_26 = reFunction(f'联系电话：\s*([（）\d\.:： \(\)〔〕㎡≤≥《》\-\/\%,；，、\.﹪]*)', items)
                        # 是否需要判断重复 请求
                        if DUPLICATE_SWITCH:
                            if self.redisClient.isExist(md5Mark):  # 存在, 去重计数
                                self.duplicateUrl += 1

                        if self.duplicateUrl < 50:
                            if ZDBH_6 and TDYT_11:
                                # 重复效验通过, 存储数据
                                csvFile = [
                                    GGLX_1,
                                    WJBT_2,
                                    SJ_3,
                                    LY_4,
                                    ZWBT_5,
                                    ZDBH_6,
                                    TDWZ_7,
                                    CRMJ_8,
                                    LHYD_9,
                                    DLYD_10,
                                    TDYT_11,
                                    CRNX_12,
                                    RJL_13,
                                    JZMD_14,
                                    LDL_15,
                                    JZKJ_16,
                                    QSJ_17,
                                    BZJ_18,
                                    JJFD_19,
                                    BMRQ_20,
                                    GPRQ_21,
                                    GPJZSJ_22,
                                    BZJDZSJ_23,
                                    LXDZ_24,
                                    LXR_25,
                                    LXDH_26,
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
                elif '地块基本情况' in items:
                    # todo
                    soup = BeautifulSoup(response.body.decode('utf-8'))
                    table = soup.find('table')
                    htmlTable = htmlTableTransformer()
                    tdData = htmlTable.tableTrTdRegulationToList(table)
                    for _ in range(len(list(tdData.values())[0])):
                        # 宗地编号
                        ZDBH_6 = tdData.get('编号')[_] if tdData.get('编号') else ''
                        # 土地位置
                        TDWZ_7 = tdData.get('地块位置')[_] if tdData.get('地块位置') else ''
                        # 出让面积(m2)
                        CRMJ_8 = tdData.get('土地面积(亩)')[_] if tdData.get('土地面积(亩)') else ''
                        # 土地用途
                        TDYT_11 = tdData.get('土地用途')[_] if tdData.get('土地用途') else ''
                        # 是否需要判断重复 请求
                        if DUPLICATE_SWITCH:
                            if self.redisClient.isExist(md5Mark):  # 存在, 去重计数
                                self.duplicateUrl += 1

                        if self.duplicateUrl < 50:
                            if ZDBH_6 and TDYT_11:
                                # 重复效验通过, 存储数据
                                csvFile = [
                                    GGLX_1,
                                    WJBT_2,
                                    SJ_3,
                                    LY_4,
                                    ZWBT_5,
                                    ZDBH_6,
                                    TDWZ_7,
                                    CRMJ_8,
                                    LHYD_9,
                                    DLYD_10,
                                    TDYT_11,
                                    CRNX_12,
                                    RJL_13,
                                    JZMD_14,
                                    LDL_15,
                                    JZKJ_16,
                                    QSJ_17,
                                    BZJ_18,
                                    JJFD_19,
                                    BMRQ_20,
                                    GPRQ_21,
                                    GPJZSJ_22,
                                    BZJDZSJ_23,
                                    LXDZ_24,
                                    LXR_25,
                                    LXDH_26,
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
            print(response.url)
            self.log(f'详情页数据解析失败, 请求:{response.url}, 错误: {e}\n{traceback.format_exc()}', level=logging.ERROR)


