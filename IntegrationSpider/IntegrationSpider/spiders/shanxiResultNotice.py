# shanxiResultNotice
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


class shanxiResultNoticeSpider(CrawlSpider):
    # TODO
    name = 'shanxiResultNotice'
    # 配置更换cookies时间
    COOKIES_SWITCH_TIME = datetime.datetime.now()

    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super(shanxiResultNoticeSpider, cls).__new__(cls)
            # TODO
            pathPage = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),'Logs/shanxiResultNoticePage.txt')
            # TODO
            cls.pathDetail = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))), 'data/山西省自然资源厅_结果公示_山西.csv')
            cls.filePage = open(pathPage, 'w+')
            if os.path.exists(cls.pathDetail):
                cls.fileDetail = open(cls.pathDetail, 'a+')
            else:
                cls.fileDetail = open(cls.pathDetail, 'a+')
                with open(cls.pathDetail, 'a+') as fp:
                    # TODO
                    fp.write("""文件标题,时间,来源,正文标题,地块编号,宗地编号,拍卖结果,公开转让方式,挂牌时间,转让人,转让方,受让人,受让方,受让单位,位置,地块位置,出让面积(平方米),用途,成交价(万元),不动产权登记证号,出让合同编号,出让合同,变更协议编号,土地用途,使用年限,面积,土地面积(公顷),转让价格(单价总价),出让年限,土地使用条件,备注,公示期,联系单位,单位地址,邮政编码,联系电话,联系人,电子邮件,爬取地址url,唯一标识,\n""")
        return cls.instance

    def __init__(self):
        super(shanxiResultNoticeSpider, self).__init__()
        dispatcher.connect(self.CloseSpider, signals.spider_closed)
        # TODO
        self.redisClient = RedisClient('shanxi', 'shanxiResultNotice')
        self.duplicateUrl = 0
        self.targetUrl = 'http://zrzyt.shanxi.gov.cn/zwgk/zwgkjbml/tdgl_836/jggs/index_{}.shtml'
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
            yield Request('http://zrzyt.shanxi.gov.cn/zwgk/zwgkjbml/tdgl_836/jggs/index.shtml', method='GET',
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
                url = 'http://zrzyt.shanxi.gov.cn/zwgk/zwgkjbml/tdgl_836/jggs' + dataItem.xpath('a/@href').extract_first()[1:]
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
            WJBT_45 = ''
            SJ_46 = ''
            LY_47 = ''
            ZWBT_48 = ''
            DKBH_49 = ''
            ZDBH_50 = ''
            PMJG_51 = ''
            GGZRFS_52 = ''
            GPSJ_53 = ''
            ZRR_54 = ''
            ZRF_55 = ''
            SRR_56 = ''
            SRF_57 = ''
            SRDW_58 = ''
            WZ_59 = ''
            DKWZ_60 = ''
            CRMJ_61 = ''
            YT_62 = ''
            CJJ_63 = ''
            BDCQDJH_64 = ''
            CRHTBH_65 = ''
            CRHT_66 = ''
            BGXYBH_67 = ''
            TDYT_68 = ''
            SYNX_69 = ''
            MJ_70 = ''
            TDMJ_71 = ''
            ZRJG_72 = ''
            CRNX_73 = ''
            TDSYNX_74 = ''
            BZ_75 = ''
            GSQ_76 = ''
            LXDW_77 = ''
            DWDZ_78 = ''
            YZBM_79 = ''
            LXDH_80 = ''
            LXR_81 = ''
            DZYJ_82 = ''

            # TODO 共有字段  reFunction(f'时间：\s*([{self.reStr}]*)\s', LY)
            # 文件标题
            WJBT_45 = response.meta.get('title')
            # 时间
            SJ_46 = data.xpath('//div[@class="ztzx_frame_subtitle_l"]/span[1]/text()').extract_first()
            # 来源
            LY_47 = data.xpath('//div[@class="ztzx_frame_subtitle_l"]/span[2]/text()').extract_first()
            # 正文标题
            ZWBT_48 = data.xpath('//div[@class="ztzx_frame_content"]/div[1]/text()').extract_first()
            # 公示期
            GSQ_76 = reFunction(f'公示期：([（）\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)[。\s]', items)
            # 联系单位
            LXDW_77 = reFunction('联系单位：([（）\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)
            # 单位地址
            DWDZ_78 = reFunction('单位地址：([（）\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)
            # 邮政编码
            YZBM_79 = reFunction('邮政编码：([（）\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)
            # 联系电话
            LXDH_80 = reFunction('联系电话：([（）\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)
            # 联系人
            LXR_81 = reFunction('联\s*系\s*人：([（）\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)
            # 电子邮件
            DZYJ_82 = reFunction('电子邮件：([（）\w\.:： —\(\)@〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)

            # 爬取时间
            crawlingTime = time.strftime("%Y-%m-%d", time.localtime())
            # 爬取地址url
            url = response.url
            # 唯一标识
            md5Mark = encrypt_md5(url + WJBT_45 + SJ_46)

            soup = BeautifulSoup(response.body.decode('utf-8').replace('thead', 'tbody'))
            table = soup.find('table')
            htmlTable = htmlTableTransformer()
            if '国有划拨土地使用权结果公示' in items:
                table.find_all('tr')[1].extract()
                tdData = htmlTable.tableTrTdRegulationToList(table)
                for _ in range(len(list(tdData.values())[0])):
                    # 地块编号
                    DKBH_49 =  tdData.get('地块编号')[_] if tdData.get('地块编号') else ''
                    # 公开转让方式
                    GGZRFS_52 = tdData.get('公开转让方式')[_] if tdData.get('公开转让方式') else ''
                    # 挂牌时间
                    GPSJ_53 = tdData.get('挂牌')[_] if tdData.get('挂牌') else ''
                    # 受让人
                    SRR_56 = tdData.get('受让人')[_] if tdData.get('受让人') else ''
                    # 位置
                    WZ_59 = tdData.get('位置')[_] if tdData.get('位置') else ''
                    # 出让面积(平方米)
                    CRMJ_61 = tdData.get('出让面积')[_] if tdData.get('出让面积') else ''
                    # 用途
                    YT_62 = tdData.get('用途')[_] if tdData.get('用途') else ''
                    # 成交价(万元)
                    CJJ_63 = tdData.get('成交价')[_] if tdData.get('成交价') else ''
                    # 写入数据
                    if self.name in DUPLICATE_SWITCH_LIST:
                        if self.redisClient.isExist(md5Mark):  # 存在, 去重计数
                            self.duplicateUrl += 1

                    if self.duplicateUrl < 50:
                        if True:
                            # 重复效验通过, 存储数据
                            csvFile = [
                                WJBT_45,
                                SJ_46,
                                LY_47,
                                ZWBT_48,
                                DKBH_49,
                                ZDBH_50,
                                PMJG_51,
                                GGZRFS_52,
                                GPSJ_53,
                                ZRR_54,
                                ZRF_55,
                                SRR_56,
                                SRF_57,
                                SRDW_58,
                                WZ_59,
                                DKWZ_60,
                                CRMJ_61,
                                YT_62,
                                CJJ_63,
                                BDCQDJH_64,
                                CRHTBH_65,
                                CRHT_66,
                                BGXYBH_67,
                                TDYT_68,
                                SYNX_69,
                                MJ_70,
                                TDMJ_71,
                                ZRJG_72,
                                CRNX_73,
                                TDSYNX_74,
                                BZ_75,
                                GSQ_76,
                                LXDW_77,
                                DWDZ_78,
                                YZBM_79,
                                LXDH_80,
                                LXR_81,
                                DZYJ_82,
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
                                    self.log(
                                        f'{getVariableName(_).pop()}字段解析出错, 错误: {e}\n{traceback.format_exc()}',
                                        level=logging.ERROR)
                            with open(self.pathDetail, 'a+') as fp:
                                fp.write(results)
                                fp.write('\n')
                            self.log(f'数据获取成功', level=logging.INFO)
                            yield
                    else:
                        self.crawler.engine.close_spider(self, 'response msg info %s, job duplicated!' % response.url)
            elif '不动产权登记证号' in items:
                # 转让方
                ZRF_55 = reFunction('转让方：\s*([（）【】\w\.:：—\(\)〔〕㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)
                # 受让方
                SRF_57 = reFunction('受让方：\s*([（）【】\w\.:：—\(\)〔〕㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)
                # 位置
                WZ_59 = reFunction('宗地位置：\s*([（）【】\w\.:：—\(\)〔〕㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)
                # 不动产权登记证号
                BDCQDJH_64 = reFunction('不动产权登记证号：\s*([（）【】\w\.:：—\(\)〔〕㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)
                # 出让合同编号
                CRHTBH_65 = reFunction('出让合同编号：\s*([（）【】\w\.:：—\(\)〔〕㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)
                # 变更协议编号
                BGXYBH_67 = reFunction('出让合同变更协议编号：\s*([（）【】\w\.:：—\(\)〔〕㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)
                # 土地用途
                TDYT_68 = reFunction('土地用途：\s*([（）【】\w\.:：—\(\)〔〕㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)
                # 使用年限
                SYNX_69 = reFunction('使用年限：\s*([（）【】\w\.:：—\(\)〔〕\s㎡≤≥《》\-\/\%,；，、\.﹪]*)面\s*积', items)
                # 面积
                MJ_70 = reFunction('面\s*积：\s*([（）【】\w\.:：—\(\)〔〕㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)
                # 转让价格(单价总价)
                ZRJG_72 = reFunction('转让价格：\s*([（）【】\w\.:：—\(\)〔〕㎡≤≥《》\-\/\%,；，、。\.﹪]*)\s', items)

                # 写入数据
                if self.name in DUPLICATE_SWITCH_LIST:
                    if self.redisClient.isExist(md5Mark):  # 存在, 去重计数
                        self.duplicateUrl += 1

                if self.duplicateUrl < 50:
                    if True:
                        # 重复效验通过, 存储数据
                        csvFile = [
                            WJBT_45,
                            SJ_46,
                            LY_47,
                            ZWBT_48,
                            DKBH_49,
                            ZDBH_50,
                            PMJG_51,
                            GGZRFS_52,
                            GPSJ_53,
                            ZRR_54,
                            ZRF_55,
                            SRR_56,
                            SRF_57,
                            SRDW_58,
                            WZ_59,
                            DKWZ_60,
                            CRMJ_61,
                            YT_62,
                            CJJ_63,
                            BDCQDJH_64,
                            CRHTBH_65,
                            CRHT_66,
                            BGXYBH_67,
                            TDYT_68,
                            SYNX_69,
                            MJ_70,
                            TDMJ_71,
                            ZRJG_72,
                            CRNX_73,
                            TDSYNX_74,
                            BZ_75,
                            GSQ_76,
                            LXDW_77,
                            DWDZ_78,
                            YZBM_79,
                            LXDH_80,
                            LXR_81,
                            DZYJ_82,
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
                                self.log(
                                    f'{getVariableName(_).pop()}字段解析出错, 错误: {e}\n{traceback.format_exc()}',
                                    level=logging.ERROR)
                        with open(self.pathDetail, 'a+') as fp:
                            fp.write(results)
                            fp.write('\n')
                        self.log(f'数据获取成功', level=logging.INFO)
                        yield
                else:
                    self.crawler.engine.close_spider(self, 'response msg info %s, job duplicated!' % response.url)
            elif '挂牌出让地块的基本情况和规划指标要求' in items:
                # 宗地编号
                ZDBH_50 = reFunction('宗地编号：*\s*([（）【】\w\.:：—\(\)〔〕㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)
                # 挂牌时间
                GPSJ_53 = reFunction('挂牌时间为:\s*([（）【】\w\.:：—\(\)〔〕㎡≤≥《》\-\/\%,；。，、\.﹪]*)\s', items).replace('。', '')
                # 转让人
                ZRR_54 = reFunction('转让人为：*\s*([（）【】\w\.:：—\(\)〔〕㎡≤≥《》\-\/\%\.﹪]*)，', items)
                # 位置
                WZ_59 = reFunction('宗地坐落：*\s*([（）【】\w\.:：—\(\)〔〕㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)
                # 土地用途
                TDYT_68 = reFunction('土地用途：*\s*([（）【】\w\.:：—\(\)〔〕㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)
                # 面积
                MJ_70 = reFunction('宗地面积：*\s*([（）【】\w\.:：—\(\)〔〕㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)
                # 出让年限
                CRNX_73 = reFunction('出让年限：*\s*([（）【】\w\.:：—\(\)〔〕㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', items)
                # 备注
                BZ_75 = reFunction('备注：*\s*([（）【】\w\.:：—\(\)〔〕㎡≤≥《》\-\/\%,；。，、\.﹪]*)\s*二', items)

                # 写入数据
                if self.name in DUPLICATE_SWITCH_LIST:
                    if self.redisClient.isExist(md5Mark):  # 存在, 去重计数
                        self.duplicateUrl += 1

                if self.duplicateUrl < 50:
                    if True:
                        # 重复效验通过, 存储数据
                        csvFile = [
                            WJBT_45,
                            SJ_46,
                            LY_47,
                            ZWBT_48,
                            DKBH_49,
                            ZDBH_50,
                            PMJG_51,
                            GGZRFS_52,
                            GPSJ_53,
                            ZRR_54,
                            ZRF_55,
                            SRR_56,
                            SRF_57,
                            SRDW_58,
                            WZ_59,
                            DKWZ_60,
                            CRMJ_61,
                            YT_62,
                            CJJ_63,
                            BDCQDJH_64,
                            CRHTBH_65,
                            CRHT_66,
                            BGXYBH_67,
                            TDYT_68,
                            SYNX_69,
                            MJ_70,
                            TDMJ_71,
                            ZRJG_72,
                            CRNX_73,
                            TDSYNX_74,
                            BZ_75,
                            GSQ_76,
                            LXDW_77,
                            DWDZ_78,
                            YZBM_79,
                            LXDH_80,
                            LXR_81,
                            DZYJ_82,
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
                                self.log(
                                    f'{getVariableName(_).pop()}字段解析出错, 错误: {e}\n{traceback.format_exc()}',
                                    level=logging.ERROR)
                        with open(self.pathDetail, 'a+') as fp:
                            fp.write(results)
                            fp.write('\n')
                        self.log(f'数据获取成功', level=logging.INFO)
                        yield
                else:
                    self.crawler.engine.close_spider(self, 'response msg info %s, job duplicated!' % response.url)
            elif '地块基本情况' in items:
                try:
                    if '备注' not in items:
                        tdData = htmlTable.tableTrTdRegulationToList(table)
                        for _ in range(len(list(tdData.values())[0])):
                            # 宗地编号
                            ZDBH_50 = tdData.get('宗地编号')[_] if tdData.get('宗地编号') else ''
                            # 受让单位
                            SRDW_58 = tdData.get('受让单位')[_] if tdData.get('受让单位') else ''
                            # 受让人
                            SRR_56 = tdData.get('竞得人')[_] if tdData.get('竞得人') else ''
                            # 地块位置
                            DKWZ_60 = tdData.get('地块位置')[_] if tdData.get('地块位置') else ''
                            # 土地用途
                            TDYT_68 = tdData.get('土地用途')[_] if tdData.get('土地用途') else ''
                            # 成交价(万元)
                            CJJ_63 = tdData.get('成交价(万元)')[_] if tdData.get('成交价(万元)') else ''
                            # 土地面积(公顷)
                            TDMJ_71 = tdData.get('土地面积(亩)')[_] if tdData.get('土地面积(亩)') else ''
                            # 出让年限
                            CRNX_73 = tdData.get('出让年限')[_] if tdData.get('出让年限') else ''

                            # 写入数据
                            if self.name in DUPLICATE_SWITCH_LIST:
                                if self.redisClient.isExist(md5Mark):  # 存在, 去重计数
                                    self.duplicateUrl += 1

                            if self.duplicateUrl < 50:
                                if True:
                                    # 重复效验通过, 存储数据
                                    csvFile = [
                                        WJBT_45,
                                        SJ_46,
                                        LY_47,
                                        ZWBT_48,
                                        DKBH_49,
                                        ZDBH_50,
                                        PMJG_51,
                                        GGZRFS_52,
                                        GPSJ_53,
                                        ZRR_54,
                                        ZRF_55,
                                        SRR_56,
                                        SRF_57,
                                        SRDW_58,
                                        WZ_59,
                                        DKWZ_60,
                                        CRMJ_61,
                                        YT_62,
                                        CJJ_63,
                                        BDCQDJH_64,
                                        CRHTBH_65,
                                        CRHT_66,
                                        BGXYBH_67,
                                        TDYT_68,
                                        SYNX_69,
                                        MJ_70,
                                        TDMJ_71,
                                        ZRJG_72,
                                        CRNX_73,
                                        TDSYNX_74,
                                        BZ_75,
                                        GSQ_76,
                                        LXDW_77,
                                        DWDZ_78,
                                        YZBM_79,
                                        LXDH_80,
                                        LXR_81,
                                        DZYJ_82,
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
                                            self.log(
                                                f'{getVariableName(_).pop()}字段解析出错, 错误: {e}\n{traceback.format_exc()}',
                                                level=logging.ERROR)
                                    with open(self.pathDetail, 'a+') as fp:
                                        fp.write(results)
                                        fp.write('\n')
                                    self.log(f'数据获取成功', level=logging.INFO)
                                    yield
                            else:
                                self.crawler.engine.close_spider(self, 'response msg info %s, job duplicated!' % response.url)
                    else:
                        if '竞得人' not in items:
                            for item in ['宗地编号' + _ for _ in re.findall('一([\s\S]*)二、', items)[0].split('宗地编号')[1:]]:
                                # 宗地编号
                                ZDBH_50 = reFunction('编号\s*([\w\-]*)\s', item)
                                # 受让单位
                                SRDW_58 = reFunction('受让单位\s*([（）\w\.:：—\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', item)
                                # 地块位置
                                DKWZ_60 = reFunction('地块位置\s*([（）\w\.:：—\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', item)
                                # 成交价(万元)
                                CJJ_63 = reFunction('成交价\(万元\)\s*([（）\w\.:：—\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s',
                                                    item) if reFunction(
                                    '成交价\(万元\)\s*([（）\w\.:：—\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', item) else reFunction(
                                    '成交价（万元）\s*([（）\w\.:：—\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', item)
                                # 土地用途
                                TDYT_68 = reFunction('土地用途\s*([（）\w\.:：—\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', item)
                                # 土地面积(公顷)
                                TDMJ_71 = reFunction('土地\s*面积\s*\(公顷\)\s*([（）\w\.:：—\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s',
                                                     item)
                                # 出让年限
                                CRNX_73 = reFunction('出让年限\s*([（）\w\.:：—\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', item)
                                # 备注
                                BZ_75 = reFunction('备注：\s*([（）\w\.:：—\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)', item)
                                if '二' in BZ_75:
                                    BZ_75 = ''
                                # 写入数据
                                if self.name in DUPLICATE_SWITCH_LIST:
                                    if self.redisClient.isExist(md5Mark):  # 存在, 去重计数
                                        self.duplicateUrl += 1

                                if self.duplicateUrl < 50:
                                    if True:
                                        # 重复效验通过, 存储数据
                                        csvFile = [
                                            WJBT_45,
                                            SJ_46,
                                            LY_47,
                                            ZWBT_48,
                                            DKBH_49,
                                            ZDBH_50,
                                            PMJG_51,
                                            GGZRFS_52,
                                            GPSJ_53,
                                            ZRR_54,
                                            ZRF_55,
                                            SRR_56,
                                            SRF_57,
                                            SRDW_58,
                                            WZ_59,
                                            DKWZ_60,
                                            CRMJ_61,
                                            YT_62,
                                            CJJ_63,
                                            BDCQDJH_64,
                                            CRHTBH_65,
                                            CRHT_66,
                                            BGXYBH_67,
                                            TDYT_68,
                                            SYNX_69,
                                            MJ_70,
                                            TDMJ_71,
                                            ZRJG_72,
                                            CRNX_73,
                                            TDSYNX_74,
                                            BZ_75,
                                            GSQ_76,
                                            LXDW_77,
                                            DWDZ_78,
                                            YZBM_79,
                                            LXDH_80,
                                            LXR_81,
                                            DZYJ_82,
                                            crawlingTime,
                                            url,
                                            md5Mark,
                                        ]
                                        results = ''
                                        for _ in csvFile:
                                            try:
                                                if _ and _ != '|' * len(_):
                                                    results += _.replace(',', ' ').replace('\n', '').replace('\t',
                                                                                                             '').replace(
                                                        '\r', '').replace(r'\xa0', '').replace('\xa0', '') + ','
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
                                else:
                                    self.crawler.engine.close_spider(self,
                                                                     'response msg info %s, job duplicated!' % response.url)
                except Exception as e:
                    if '竞得人' not in items:
                        for item in ['宗地编号' + _ for _ in re.findall('一([\s\S]*)二、', items)[0].split('宗地编号')[1:]]:
                            # 宗地编号
                            ZDBH_50 = reFunction('编号\s*([\w\-]*)\s', item)
                            # 受让单位
                            SRDW_58 = reFunction('受让单位\s*([（）\w\.:：—\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', item)
                            # 地块位置
                            DKWZ_60 = reFunction('地块位置\s*([（）\w\.:：—\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', item)
                            # 成交价(万元)
                            CJJ_63 = reFunction('成交价\(万元\)\s*([（）\w\.:：—\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', item) if reFunction( '成交价\(万元\)\s*([（）\w\.:：—\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', item) else reFunction('成交价（万元）\s*([（）\w\.:：—\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', item)
                            # 土地用途
                            TDYT_68 = reFunction('土地用途\s*([（）\w\.:：—\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', item)
                            # 土地面积(公顷)
                            TDMJ_71 = reFunction('土地\s*面积\s*\(公顷\)\s*([（）\w\.:：—\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', item)
                            # 出让年限
                            CRNX_73 = reFunction('出让年限\s*([（）\w\.:：—\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', item)
                            # 备注
                            BZ_75 = reFunction('备注：\s*([（）\w\.:：—\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*)', item)
                            if '二' in BZ_75:
                                BZ_75 = ''
                            # 写入数据
                            if self.name in DUPLICATE_SWITCH_LIST:
                                if self.redisClient.isExist(md5Mark):  # 存在, 去重计数
                                    self.duplicateUrl += 1

                            if self.duplicateUrl < 50:
                                if True:
                                    # 重复效验通过, 存储数据
                                    csvFile = [
                                        WJBT_45,
                                        SJ_46,
                                        LY_47,
                                        ZWBT_48,
                                        DKBH_49,
                                        ZDBH_50,
                                        PMJG_51,
                                        GGZRFS_52,
                                        GPSJ_53,
                                        ZRR_54,
                                        ZRF_55,
                                        SRR_56,
                                        SRF_57,
                                        SRDW_58,
                                        WZ_59,
                                        DKWZ_60,
                                        CRMJ_61,
                                        YT_62,
                                        CJJ_63,
                                        BDCQDJH_64,
                                        CRHTBH_65,
                                        CRHT_66,
                                        BGXYBH_67,
                                        TDYT_68,
                                        SYNX_69,
                                        MJ_70,
                                        TDMJ_71,
                                        ZRJG_72,
                                        CRNX_73,
                                        TDSYNX_74,
                                        BZ_75,
                                        GSQ_76,
                                        LXDW_77,
                                        DWDZ_78,
                                        YZBM_79,
                                        LXDH_80,
                                        LXR_81,
                                        DZYJ_82,
                                        crawlingTime,
                                        url,
                                        md5Mark,
                                    ]
                                    results = ''
                                    for _ in csvFile:
                                        try:
                                            if _ and _ != '|' * len(_):
                                                results += _.replace(',', ' ').replace('\n', '').replace('\t', '').replace('\r', '').replace(r'\xa0', '').replace('\xa0', '') + ','
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
                            else:
                                self.crawler.engine.close_spider(self, 'response msg info %s, job duplicated!' % response.url)

        except Exception as e:
            print(response.url)
            self.log(f'详情页数据解析失败, 请求:{response.url}, 错误: {e}\n{traceback.format_exc()}', level=logging.ERROR)






