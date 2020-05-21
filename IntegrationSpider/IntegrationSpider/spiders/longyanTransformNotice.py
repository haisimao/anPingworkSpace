# longyanTransformNotice
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


class longyanTransformNoticeSpider(CrawlSpider):
    # TODO
    name = 'longyanTransformNotice'
    # 配置更换cookies时间
    COOKIES_SWITCH_TIME = datetime.datetime.now()

    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super(longyanTransformNoticeSpider, cls).__new__(cls)
            # TODO
            pathPage = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),'Logs/longyanTransformNoticePage.txt')
            # TODO
            cls.pathDetail = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))), 'data/龙岩市公共资源交易中心_出让公告_龙岩.csv')
            cls.filePage = open(pathPage, 'w+')
            if os.path.exists(cls.pathDetail):
                cls.fileDetail = open(cls.pathDetail, 'a+')
            else:
                cls.fileDetail = open(cls.pathDetail, 'a+')
                with open(cls.pathDetail, 'a+') as fp:
                    # TODO
                    fp.write("""文件标题,信息时间,正文标题,公告编号,出让时间,公告类型,宗地编号,地块编号,宗地位置,宗地坐落,土地用途,规划土地用途,出让年限,使用年限,批准机关及文号,规划用地面积〔m2),规划面积(m2),出让面积(m2),出让用地面积(m2),宗地出让面积,建筑密度,容积率,绿地率,绿地率(%),建筑控制高度(m),建筑控制高度(米),建筑系数(%),投资强度(万元/公顷),土地估价备案号,是否省重点,现状土地条件,竞买保证金,竟买保证金(万元),起叫价,出让起始价(万元),加价幅度,是否设置保留价,挂牌开始时间,挂牌截止时间,获取出让文件时间,提交竞买申请时间,保证金截止时间,确认竞买资格时间,联系地址,联系电话,联系人,保证金账户开户单位/户名,保证金账户账号,保证金账户开户行,出让金账户开户单位/户名,出让金账户开户行,出让金账户账号,爬取地址url,唯一标识,\n""")
        return cls.instance

    def __init__(self):
        super(longyanTransformNoticeSpider, self).__init__()
        dispatcher.connect(self.CloseSpider, signals.spider_closed)
        # TODO
        self.redisClient = RedisClient('longyan', 'longyanTransformNotice')
        self.duplicateUrl = 0
        # TODO
        self.targetUrl = 'https://www.lyggzy.com.cn/lyztb/tdky/084002/?pageing={}'
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
            for page in range(1, 10):
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
            WJBT_1 = ''
            XXSJ_2 = ''
            WBT_3 = ''
            GGBH_4 = ''
            CRSJ_5 = ''
            GGNX_6 = ''
            ZDBH_7 = ''
            DKWZ_8 = ''
            ZDWZ_9 = ''
            ZDZL_10 = ''
            TDYT_11 = ''
            GHTDYT_12 = ''
            CRNX_13 = ''
            SYNX_14 = ''
            PZJGJWH_15 = ''
            GHYDMJ_16 = ''
            GHMJ_17 = ''
            CRMJ_18 = ''
            CRYDMJ_19 = ''
            ZDCRMJ_20 = ''
            JZMD_21 = ''
            RJL_22 = ''
            LDL_23 = ''
            LDL_24 = ''
            JZKZGD_25 = ''
            JZKZZGD_26 = ''
            JZXS_27 = ''
            TZQD_28 = ''
            TDGJBAH_29 = ''
            SFSZD_30 = ''
            TDXZTJ_31 = ''
            JMBZJ_32 = ''
            JMBZJ_72 = ''
            QJJ_33 = ''
            CRQSJ_34 = ''
            JJFD_35 = ''
            SFSZBLJ_36 = ''
            GPKSSJ_37 = ''
            GPJZSJ_38 = ''
            HQCRWJSJ_39 = ''
            TJJMSQSJ_40 = ''
            BZJJZSJ_41 = ''
            QRJMZGSJ_42 = ''
            LXDZ_43 = ''
            LXDH_44 = ''
            LXR_45 = ''
            BZJZH_86 = ''
            BZJZH_87 = ''
            BZJZH_88 = ''
            CRJZH_97 = ''
            CRJZH_98 = ''
            CRJZH_99 = ''

            # TODO 共有字段  reFunction(f'时间：\s*([{self.reStr}]*)\s', LY)
            # 文件标题
            WJBT_1 = response.meta.get('title').strip()
            # 信息时间
            XXSJ_2 = reFunction('[\d\-]*', data.xpath('//p[@class="sub-cp"]/text()').extract_first())
            # 正文标题
            WBT_3 = WJBT_1
            # 公告编号
            GGBH_4 = ''.join(data.xpath('//div[@class="substance"]/p[position() <5]/.//*[contains(text(),"号")]/ancestor::p/.//text()').extract())
            # 出让时间
            CRSJ_5 = reFunction('定于\s*([（）【】\w\.—\(\)〔〕㎡≤≥《》\-\/\%\.﹪]*)[,；，、在]', items)
            # 公告类型
            GGNX_6 = '出让公告'
            # 爬取时间
            crawlingTime = time.strftime("%Y-%m-%d", time.localtime())
            # 爬取地址url
            url = response.url
            # 唯一标识
            md5Mark = encrypt_md5(url + WJBT_1 + XXSJ_2)

            GPSJ_0 = reFunction('挂牌交易期限：*\s*([（）\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)[\s。]', items)
            GPSJ_1 = reFunction('申请人可于：*\s*([（）\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)到', items)
            GPSJ = GPSJ_0 if GPSJ_0 else GPSJ_1
            # 挂牌开始时间、
            GPKSSJ_37 = reFunction('挂牌开始时间：*\s*([（）\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)
            # 挂牌截止时间、
            GPJZSJ_38 = reFunction('挂牌截止时间：*\s*([（）\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)
            if GPSJ:
                try:
                    GPKSSJ_37 = GPSJ.split('至')[0]
                    GPJZSJ_38 = GPSJ.split('至')[1]
                except:
                    pass
            # 获取出让文件时间、
            HQCRWJSJ_39 = GPSJ_1
            # 提交竞买申请时间、
            TJJMSQSJ_40 = reFunction('(\d{4}年\d{1,3}月\d{1,3}日(?:[（）\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)至 \d{4}年\d{1,3}月\d{1,3}日)', items)
            # 保证金截止时间、
            BZJJZSJ_41 = reFunction('(\d{4}年\d{1,3}月\d{1,3}日(?:[（）\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)至 \d{4}年\d{1,3}月\d{1,3}日)', items)
            # 确认竞买资格时间
            QRJMZGSJ_42 = reFunction('(\d{4}年\d{1,3}月\d{1,3}日(?:[（）\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)至 \d{4}年\d{1,3}月\d{1,3}日)', items)
            # 联系地址、
            LXDZ_43 = reFunction('联系地址：([（）\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)
            # 联系电话、
            LXDH_44 = reFunction('联系电话：([（）\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)
            # 联系人、
            LXR_45 = reFunction('联\s*系\s*人：([（）\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)

            ZH_0 = reFunction('以下账户：*\s*([\w\.:： —\(\)\s〔〕㎡㎡≤≥《》\-\/\%,；；，、\.﹪\s]*)[一二三四五六七八九123456789]*', items)
            ZH_1 = reFunction('保证金帐户：*\s*([\w\.:： —\(\)\s〔〕㎡㎡≤≥《》\-\/\%,；；，、\.﹪]*)\s*', items)
            try:
                if ZH_0:
                    if ZH_0[:2] == '户名':
                        result = re.split('[①②③④]*', ZH_0)
                        # 保证金账户开户单位 / 户名
                        BZJZH_86 = result[0].replace('户名：','') if result[0] else ''
                        # 保证金账户账号
                        BZJZH_87 = '|'.join([re.split('，|,', _)[0] for _ in result[1:]])
                        # 保证金账户开户行
                        BZJZH_88 = '|'.join([re.split('，|,', _)[-1] for _ in result[1:]])
                    else:
                        result = re.split('[①②③④]*', ZH_0)
                        # 保证金账户开户单位 / 户名
                        BZJZH_86 = '|'.join([re.findall('开\s*户\s*行：*\s*([\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,；；，、\.﹪]*)\s', _)[0] if re.findall('开 户 行：*\s*([\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,；；，、\.﹪]*)\s', _) else '' for _ in result])
                        # 保证金账户账号
                        BZJZH_87 = '|'.join([re.findall('户\s*名：*\s*([\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,；；，、\.﹪]*)\s', _)[0] if re.findall('开 户 行：*\s*([\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,；；，、\.﹪]*)\s', _) else '' for _ in result])
                        # 保证金账户开户行
                        BZJZH_88 = '|'.join([re.findall('账\s*号：*\s*([\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,；；，、\.﹪]*)\s', _)[0] if re.findall('账\s*号：*\s*([\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,；；，、\.﹪]*)\s', _) else '' for _ in result])
                elif ZH_1:
                    # 保证金账户开户单位 / 户名
                    BZJZH_86 = '|'.join(re.findall('开户[单位名称]*：*\s*([\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,，；；、\.﹪]*)\s', ZH_1)).replace('；')
                    # 保证金账户账号
                    BZJZH_87 = '|'.join(re.findall('开\s*户\s*行：*\s*([\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,，；；、\.﹪]*)\s', ZH_1)).replace('；')
                    # 保证金账户开户行
                    BZJZH_88 = '|'.join(re.findall('帐\s*号：*\s*([\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,，；；、\.﹪]*)\s', ZH_1)).replace('；')
            except:
                pass
            CR = reFunction('出让金帐户：*\s*([\w\.:： —\(\)\s〔〕㎡㎡≤≥《》\-\/\%,；；，、\.﹪]*)\s*', items)
            try:
                # 出让金账户开户单位 / 户名
                CRJZH_97 = '|'.join(re.findall('开户[单位名称]*：*\s*([\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,，；；、\.﹪]*)\s', CR)).replace('；')
                # 出让金账户开户行
                CRJZH_98 = '|'.join(re.findall('开\s*户\s*行：*\s*([\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,，；；、\.﹪]*)\s', CR)).replace('；')
                # 出让金账户账号
                CRJZH_99 = '|'.join(re.findall('帐\s*号：*\s*([\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,，；；、\.﹪]*)\s', CR)).replace('；')
            except:
                pass

            if '拍卖出让地块的基本情况和规划指标要求' not in items and '备注' not in items and '挂牌出让地块的基本情况和规划指标要求' not in items:
                try:
                    soup = BeautifulSoup(response.body.decode('utf-8'))
                    tables = soup.find_all('table')
                    if '规划用途及主要指标' in items:  # 处理费标准的表格
                        soup = BeautifulSoup(response.body.decode('utf-8'))
                        table = soup.find('table')
                        tdReplace = table.tbody.find_all('tr')[0].find('td', colspan='4')
                        number = table.tbody.find_all('tr')[0].index(tdReplace)
                        tdList = table.tbody.find_all('tr')[1].find_all('td')
                        for _ in range(1, len(tdList) + 1):
                            table.tbody.find_all('tr')[0].insert(number + _, tdList[_ - 1])
                        tdReplace.extract()
                        [_.extract() for _ in table.tbody.find_all('tr')[1].find_all('td')]
                        table.tbody.find_all('tr')[1].extract()

                        tdData = htmlTable.tableTrTdChangeToList(table)
                        for _ in range(len(list(tdData.values())[0])):
                            # 宗地编号
                            ZDBH_7 = tdData.get('宗地编号')[_] if tdData.get('宗地编号') else ''
                            # 出让面积(m2)
                            CRMJ_18 = tdData.get('土地面积（㎡）')[_] if tdData.get('土地面积（㎡）') else ''
                            # 容积率
                            RJL_22 = tdData.get('容积率')[_] if tdData.get('容积率') else ''
                            # 绿地率( %)
                            LDL_24 = tdData.get('绿地率（%）')[_] if tdData.get('绿地率（%）') else ''
                            # 建筑系数( %)
                            JZXS_27 = tdData.get('建筑系数（%）')[_] if tdData.get('建筑系数（%）') else ''
                            # 竟买保证金(万元)
                            JMBZJ_72 = tdData.get('竞买保证金（万元）')[_] if tdData.get('竞买保证金（万元）') else ''
                            # 出让起始价(万元)
                            CRQSJ_34 = tdData.get('挂牌出让起始价（万元）')[_] if tdData.get('挂牌出让起始价（万元）') else ''
                            # 加价幅度、
                            JJFD_35 = tdData.get('加价幅度')[_] if tdData.get('加价幅度') else ''

                            # 写入数据
                            if self.name in DUPLICATE_SWITCH_LIST:
                                if self.redisClient.isExist(md5Mark):  # 存在, 去重计数
                                    self.duplicateUrl += 1

                            if self.duplicateUrl < 50:
                                if ZDBH_7:
                                    # 重复效验通过, 存储数据
                                    csvFile = [
                                        WJBT_1,
                                        XXSJ_2,
                                        WBT_3,
                                        GGBH_4,
                                        CRSJ_5,
                                        GGNX_6,
                                        ZDBH_7,
                                        DKWZ_8,
                                        ZDWZ_9,
                                        ZDZL_10,
                                        TDYT_11,
                                        GHTDYT_12,
                                        CRNX_13,
                                        SYNX_14,
                                        PZJGJWH_15,
                                        GHYDMJ_16,
                                        GHMJ_17,
                                        CRMJ_18,
                                        CRYDMJ_19,
                                        ZDCRMJ_20,
                                        JZMD_21,
                                        RJL_22,
                                        LDL_23,
                                        LDL_24,
                                        JZKZGD_25,
                                        JZKZZGD_26,
                                        JZXS_27,
                                        TZQD_28,
                                        TDGJBAH_29,
                                        SFSZD_30,
                                        TDXZTJ_31,
                                        JMBZJ_32,
                                        JMBZJ_72,
                                        QJJ_33,
                                        CRQSJ_34,
                                        JJFD_35,
                                        SFSZBLJ_36,
                                        GPKSSJ_37,
                                        GPJZSJ_38,
                                        HQCRWJSJ_39,
                                        TJJMSQSJ_40,
                                        BZJJZSJ_41,
                                        QRJMZGSJ_42,
                                        LXDZ_43,
                                        LXDH_44,
                                        LXR_45,
                                        BZJZH_86,
                                        BZJZH_87,
                                        BZJZH_88,
                                        CRJZH_97,
                                        CRJZH_98,
                                        CRJZH_99,
                                        crawlingTime,
                                        url,
                                        md5Mark,
                                    ]
                                    results = ''
                                    for _ in csvFile:
                                        try:
                                            if _ and _ != '|' * len(_):
                                                results += _.replace(',', ' ').replace('\n', '').replace('\t', '').replace(
                                                    '\r', '').replace(r'\xa0', '').replace('\xa0', '') + ','
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
                                self.crawler.engine.close_spider(self,
                                                                 'response msg info %s, job duplicated!' % response.url)
                    elif len(tables) <= 3:
                        tdsList = {}
                        for table in tables:
                            td = htmlTable.tableTrTdRegulationToList(table)
                            tdsList.update(td)
                        for _ in range(len(list(tdsList.values())[0])):
                            # 宗地编号
                            ZDBH_7 = tdsList.get('宗地编号')[_] if tdsList.get('宗地编号') else ''
                            # 地块编号  地块名称
                            DKWZ_8 = tdsList.get('地块编号')[_] if tdsList.get('地块编号') else tdsList.get('地块编号')[_] if tdsList.get('地块编号') else ''
                            # 宗地位置
                            ZDWZ_9 = tdsList.get('宗地位置')[_] if tdsList.get('宗地位置') else ''
                            # 宗地坐落
                            ZDZL_10 = tdsList.get('宗地坐落')[_] if tdsList.get('宗地坐落') else ''
                            # 土地用途
                            TDYT_11 = tdsList.get('土地用途')[_] if tdsList.get('土地用途') else ''
                            # 规划土地用途
                            GHTDYT_12 = tdsList.get('规划土地用途')[_] if tdsList.get('规划土地用途') else ''
                            # 出让年限
                            CRNX_13 = tdsList.get('出让年限')[_] if tdsList.get('出让年限') else ''
                            # 使用年限
                            SYNX_14 = tdsList.get('使用年限')[_] if tdsList.get('使用年限') else ''
                            # 批准机关及文号
                            PZJGJWH_15 = tdsList.get('批准机关及文号')[_] if tdsList.get('批准机关及文号') else tdsList.get('批准文号')[_] if tdsList.get('批准文号') else ''
                            # 规划用地面积〔m2)
                            GHYDMJ_16 = tdsList.get('规划用地面积（m2）')[_] if tdsList.get('规划用地面积（m2）') else tdsList.get('用地面积(㎡)')[_] if tdsList.get('用地面积(㎡)') else tdsList.get('规划用地面积（㎡）')[_] if tdsList.get('规划用地面积（㎡）') else ''
                            # 出让面积(m2)
                            CRMJ_18 = tdsList.get('出让面积（㎡）')[_] if tdsList.get('出让面积（㎡）') else ''
                            # 规划面积(m2)
                            GHMJ_17 = tdsList.get('规划面积（㎡）')[_] if tdsList.get('规划面积（㎡）') else ''
                            # 出让用地面积(m2)
                            CRYDMJ_19 = tdsList.get('出让用地面积（m2）')[_] if tdsList.get('出让用地面积（m2）') else ''
                            # 宗地出让面积
                            ZDCRMJ_20 = tdsList.get('宗地出让面积')[_] if tdsList.get('宗地出让面积') else ''
                            # 建筑密度
                            JZMD_21 = tdsList.get('建筑密度(%)')[_] if tdsList.get('建筑密度(%)') else tdsList.get('建筑密度（%）')[_] if tdsList.get('建筑密度（%）') else ''
                            # 容积率
                            RJL_22 = tdsList.get('容积率')[_] if tdsList.get('容积率') else ''
                            # 绿地率
                            LDL_23 = tdsList.get('宗地坐落')[_] if tdsList.get('宗地坐落') else ''
                            # 绿地率( %)
                            LDL_24 = tdsList.get('绿地率')[_] if tdsList.get('绿地率') else tdsList.get('绿地率（%）')[_] if tdsList.get('绿地率（%）') else tdsList.get('绿地率(%)')[_] if tdsList.get('绿地率(%)') else ''
                            # 建筑控制高度(m)
                            JZKZGD_25 = tdsList.get('建筑控制高度（m）')[_] if tdsList.get('建筑控制高度（m）') else ''
                            # 建筑控制高度(米)
                            JZKZZGD_26 = tdsList.get('建筑控制高度(米)')[_] if tdsList.get('建筑控制高度(米)') else ''
                            # 投资强度(万元 / 公顷)
                            TZQD_28 = tdsList.get('投资强度（万元/公顷）')[_] if tdsList.get('投资强度（万元/公顷）') else ''
                            # 竞买保证金
                            JMBZJ_32 = tdsList.get('竞买保证金')[_] if tdsList.get('竞买保证金') else ''
                            # 出让起始价(万元)
                            CRQSJ_34 = tdsList.get('出让起始价')[_] if tdsList.get('出让起始价') else ''
                            # 竟买保证金(万元)
                            JMBZJ_72 = tdsList.get('竞买保证金(万元)')[_] if tdsList.get('竞买保证金(万元)') else ''
                            # 起叫价
                            QJJ_33 = tdsList.get('起始价')[_] if tdsList.get('起始价') else tdsList.get('出让起始价')[_] if tdsList.get('出让起始价') else ''
                            # 加价幅度
                            JJFD_35 = tdsList.get('加价幅度')[_] if tdsList.get('加价幅度') else ''

                            if self.name in DUPLICATE_SWITCH_LIST:
                                if self.redisClient.isExist(md5Mark):  # 存在, 去重计数
                                    self.duplicateUrl += 1

                            if self.duplicateUrl < 50:
                                if ZDBH_7:
                                    # 重复效验通过, 存储数据
                                    csvFile = [
                                        WJBT_1,
                                        XXSJ_2,
                                        WBT_3,
                                        GGBH_4,
                                        CRSJ_5,
                                        GGNX_6,
                                        ZDBH_7,
                                        DKWZ_8,
                                        ZDWZ_9,
                                        ZDZL_10,
                                        TDYT_11,
                                        GHTDYT_12,
                                        CRNX_13,
                                        SYNX_14,
                                        PZJGJWH_15,
                                        GHYDMJ_16,
                                        GHMJ_17,
                                        CRMJ_18,
                                        CRYDMJ_19,
                                        ZDCRMJ_20,
                                        JZMD_21,
                                        RJL_22,
                                        LDL_23,
                                        LDL_24,
                                        JZKZGD_25,
                                        JZKZZGD_26,
                                        JZXS_27,
                                        TZQD_28,
                                        TDGJBAH_29,
                                        SFSZD_30,
                                        TDXZTJ_31,
                                        JMBZJ_32,
                                        JMBZJ_72,
                                        QJJ_33,
                                        CRQSJ_34,
                                        JJFD_35,
                                        SFSZBLJ_36,
                                        GPKSSJ_37,
                                        GPJZSJ_38,
                                        HQCRWJSJ_39,
                                        TJJMSQSJ_40,
                                        BZJJZSJ_41,
                                        QRJMZGSJ_42,
                                        LXDZ_43,
                                        LXDH_44,
                                        LXR_45,
                                        BZJZH_86,
                                        BZJZH_87,
                                        BZJZH_88,
                                        CRJZH_97,
                                        CRJZH_98,
                                        CRJZH_99,
                                        crawlingTime,
                                        url,
                                        md5Mark,
                                    ]
                                    results = ''
                                    for _ in csvFile:
                                        try:
                                            if _ and _ != '|' * len(_):
                                                results += _.replace(',', ' ').replace('\n', '').replace('\t', '').replace(
                                                    '\r', '').replace(r'\xa0', '').replace('\xa0', '') + ','
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
                                self.crawler.engine.close_spider(self,
                                                                 'response msg info %s, job duplicated!' % response.url)

                    elif len(tables) == 6:
                        # TODO
                        pass
                except:
                    for item in ['宗地编号' + _ for _ in re.findall('一([\s\S]*)二、', items)[0].split('宗地编号')[1:]]:
                        # 宗地编号
                        ZDBH_7 = reFunction('宗地编号：*\s*([\w\-]*)\s', item).replace('宗地位置', '').replace('地块名称', '')
                        # 宗地坐落
                        ZDZL_10 = reFunction('宗地坐落：*\s*([（）\w\.:：—\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', item)
                        # 土地用途
                        TDYT_11 = reFunction('土地用途：*\s*([（）\w\.:：—\(\)〔〕㎡㎡≤≥＜《》\-\/\%,；，、\.﹪]*)\s', item)
                        # 出让年限
                        CRNX_13 = reFunction('出让年限：*\s*([（）\w\.:：—\(\)〔〕㎡㎡≤≥＜《》\-\/\%,；，、\.﹪]*)\s', item)
                        # 宗地出让面积
                        ZDCRMJ_20 = reFunction('宗地\s*面积：*\s*([（）\w\.:：—\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', item)
                        # 建筑密度
                        JZMD_21 = reFunction('建筑密度\(%\)：*\s*([（）\w\.:：—\(\)〔〕㎡㎡≤≥＜《》\-\/\%,；，、\.﹪]*)\s', item)
                        # 容积率
                        RJL_22 = reFunction('容积率：*\s*([（）\w\.:：—\(\)〔〕㎡㎡≤≥＜《》\-\/\%,；，、\.﹪]*)\s', item)
                        # 绿地率( %)
                        LDL_24 = reFunction('绿化率\(%\)：*\s*([（）\w\.:：—\(\)〔〕㎡㎡≤≥＜《》\-\/\%,；，、\.﹪]*)\s', item)
                        # 建筑控制高度(米)
                        JZKZZGD_26 = reFunction('建筑限高\(米\)：*\s*([（）\w\.:：—\(\)〔〕㎡㎡≤≥＜《》\-\/\%,；，、\.﹪]*)\s', item)
                        # 投资强度(万元 / 公顷)
                        TZQD_28 = reFunction('投资强度：*\s*([（）\w\.:：—\(\)〔〕㎡㎡≤≥＜《》\-\/\%,；，、\.﹪]*)\s', item)
                        # 土地估价备案号
                        TDGJBAH_29 = reFunction('土地估价备案号：*\s*([（）\w\.:：—\(\)〔〕㎡㎡≤≥＜《》\-\/\%,；，、\.﹪]*)\s', item)
                        # 现状土地条件
                        TDXZTJ_31 = reFunction('土地现状：*\s*([（）\w\.:：—\(\)〔〕㎡㎡≤≥＜《》\-\/\%,；，、\.﹪]*)\s', item)
                        # 竞买保证金
                        JMBZJ_32 = reFunction('保证金：*\s*([（）\w\.:：—\(\)〔〕㎡㎡≤≥＜《》\-\/\%,；，、\.﹪]*)\s', item)
                        # 起叫价
                        QJJ_33 = reFunction('起始价：*\s*([（）\w\.:：—\(\)〔〕㎡㎡≤≥＜《》\-\/\%,；，、\.﹪]*)\s', item)
                        # 加价幅度
                        JJFD_35 = reFunction('加价幅度：*\s*([（）\w\.:：—\(\)〔〕㎡㎡≤≥＜《》\-\/\%,；，、\.﹪]*)\s', item)
                        # 挂牌开始时间、
                        GPKSSJ_37 = reFunction('挂牌开始时间：*\s*([（）\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)
                        # 挂牌截止时间、
                        GPJZSJ_38 = reFunction('挂牌截止时间：*\s*([（）\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)

                        if self.name in DUPLICATE_SWITCH_LIST:
                            if self.redisClient.isExist(md5Mark):  # 存在, 去重计数
                                self.duplicateUrl += 1

                        if self.duplicateUrl < 50:
                            if ZDBH_7:
                                # 重复效验通过, 存储数据
                                csvFile = [
                                    WJBT_1,
                                    XXSJ_2,
                                    WBT_3,
                                    GGBH_4,
                                    CRSJ_5,
                                    GGNX_6,
                                    ZDBH_7,
                                    DKWZ_8,
                                    ZDWZ_9,
                                    ZDZL_10,
                                    TDYT_11,
                                    GHTDYT_12,
                                    CRNX_13,
                                    SYNX_14,
                                    PZJGJWH_15,
                                    GHYDMJ_16,
                                    GHMJ_17,
                                    CRMJ_18,
                                    CRYDMJ_19,
                                    ZDCRMJ_20,
                                    JZMD_21,
                                    RJL_22,
                                    LDL_23,
                                    LDL_24,
                                    JZKZGD_25,
                                    JZKZZGD_26,
                                    JZXS_27,
                                    TZQD_28,
                                    TDGJBAH_29,
                                    SFSZD_30,
                                    TDXZTJ_31,
                                    JMBZJ_32,
                                    JMBZJ_72,
                                    QJJ_33,
                                    CRQSJ_34,
                                    JJFD_35,
                                    SFSZBLJ_36,
                                    GPKSSJ_37,
                                    GPJZSJ_38,
                                    HQCRWJSJ_39,
                                    TJJMSQSJ_40,
                                    BZJJZSJ_41,
                                    QRJMZGSJ_42,
                                    LXDZ_43,
                                    LXDH_44,
                                    LXR_45,
                                    BZJZH_86,
                                    BZJZH_87,
                                    BZJZH_88,
                                    CRJZH_97,
                                    CRJZH_98,
                                    CRJZH_99,
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
                for item in ['宗地编号' + _ for _ in re.findall('一([\s\S]*)二、', items)[0].split('宗地编号')[1:]]:
                    # 宗地编号
                    ZDBH_7 = reFunction('宗地编号：*\s*([\w\-]*)\s', item).replace('宗地位置', '').replace('地块名称', '')
                    # 宗地坐落
                    ZDZL_10 = reFunction('宗地坐落：*\s*([（）\w\.:：—\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', item)
                    # 土地用途
                    TDYT_11 = reFunction('土地用途：*\s*([（）\w\.:：—\(\)〔〕㎡㎡≤≥＜《》\-\/\%,；，、\.﹪]*)\s', item)
                    # 出让年限
                    CRNX_13 = reFunction('出让年限：*\s*([（）\w\.:：—\(\)〔〕㎡㎡≤≥＜《》\-\/\%,；，、\.﹪]*)\s', item)
                    # 宗地出让面积
                    ZDCRMJ_20 = reFunction('宗地\s*面积：*\s*([（）\w\.:：—\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', item)
                    # 建筑密度
                    JZMD_21 = reFunction('建筑密度\(%\)：*\s*([（）\w\.:：—\(\)〔〕㎡㎡≤≥＜《》\-\/\%,；，、\.﹪]*)\s', item)
                    # 容积率
                    RJL_22 = reFunction('容积率：*\s*([（）\w\.:：—\(\)〔〕㎡㎡≤≥＜《》\-\/\%,；，、\.﹪]*)\s', item)
                    # 绿地率( %)
                    LDL_24 = reFunction('绿化率\(%\)：*\s*([（）\w\.:：—\(\)〔〕㎡㎡≤≥＜《》\-\/\%,；，、\.﹪]*)\s', item)
                    # 建筑控制高度(米)
                    JZKZZGD_26 = reFunction('建筑限高\(米\)：*\s*([（）\w\.:：—\(\)〔〕㎡㎡≤≥＜《》\-\/\%,；，、\.﹪]*)\s', item)
                    # 投资强度(万元 / 公顷)
                    TZQD_28 = reFunction('投资强度：*\s*([（）\w\.:：—\(\)〔〕㎡㎡≤≥＜《》\-\/\%,；，、\.﹪]*)\s', item)
                    # 土地估价备案号
                    TDGJBAH_29 = reFunction('土地估价备案号：*\s*([（）\w\.:：—\(\)〔〕㎡㎡≤≥＜《》\-\/\%,；，、\.﹪]*)\s', item)
                    # 现状土地条件
                    TDXZTJ_31 = reFunction('土地现状：*\s*([（）\w\.:：—\(\)〔〕㎡㎡≤≥＜《》\-\/\%,；，、\.﹪]*)\s', item)
                    # 竞买保证金
                    JMBZJ_32 = reFunction('保证金：*\s*([（）\w\.:：—\(\)〔〕㎡㎡≤≥＜《》\-\/\%,；，、\.﹪]*)\s', item)
                    # 起叫价
                    QJJ_33 = reFunction('起始价：*\s*([（）\w\.:：—\(\)〔〕㎡㎡≤≥＜《》\-\/\%,；，、\.﹪]*)\s', item)
                    # 加价幅度
                    JJFD_35 = reFunction('加价幅度：*\s*([（）\w\.:：—\(\)〔〕㎡㎡≤≥＜《》\-\/\%,；，、\.﹪]*)\s', item)
                    # 挂牌开始时间、
                    GPKSSJ_37 = reFunction('挂牌开始时间：*\s*([（）\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)
                    # 挂牌截止时间、
                    GPJZSJ_38 = reFunction('挂牌截止时间：*\s*([（）\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)

                    if self.name in DUPLICATE_SWITCH_LIST:
                        if self.redisClient.isExist(md5Mark):  # 存在, 去重计数
                            self.duplicateUrl += 1

                    if self.duplicateUrl < 50:
                        if ZDBH_7:
                            # 重复效验通过, 存储数据
                            csvFile = [
                                WJBT_1,
                                XXSJ_2,
                                WBT_3,
                                GGBH_4,
                                CRSJ_5,
                                GGNX_6,
                                ZDBH_7,
                                DKWZ_8,
                                ZDWZ_9,
                                ZDZL_10,
                                TDYT_11,
                                GHTDYT_12,
                                CRNX_13,
                                SYNX_14,
                                PZJGJWH_15,
                                GHYDMJ_16,
                                GHMJ_17,
                                CRMJ_18,
                                CRYDMJ_19,
                                ZDCRMJ_20,
                                JZMD_21,
                                RJL_22,
                                LDL_23,
                                LDL_24,
                                JZKZGD_25,
                                JZKZZGD_26,
                                JZXS_27,
                                TZQD_28,
                                TDGJBAH_29,
                                SFSZD_30,
                                TDXZTJ_31,
                                JMBZJ_32,
                                JMBZJ_72,
                                QJJ_33,
                                CRQSJ_34,
                                JJFD_35,
                                SFSZBLJ_36,
                                GPKSSJ_37,
                                GPJZSJ_38,
                                HQCRWJSJ_39,
                                TJJMSQSJ_40,
                                BZJJZSJ_41,
                                QRJMZGSJ_42,
                                LXDZ_43,
                                LXDH_44,
                                LXR_45,
                                BZJZH_86,
                                BZJZH_87,
                                BZJZH_88,
                                CRJZH_97,
                                CRJZH_98,
                                CRJZH_99,
                                crawlingTime,
                                url,
                                md5Mark,
                            ]
                            results = ''
                            for _ in csvFile:
                                try:
                                    if _ and _ != '|' * len(_):
                                        results += _.replace(',', ' ').replace('\n', '').replace('\t', '').replace('\r',
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





