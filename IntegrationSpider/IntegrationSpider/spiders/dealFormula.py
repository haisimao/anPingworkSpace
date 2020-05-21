# -*- coding: utf-8 -*-
import datetime
import os
import random
import re
import string
import time

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

from IntegrationSpider.items import IntegrationPageItem
from IntegrationSpider.useragent import agent_list
from IntegrationSpider.utils_ import IntegrationException, getSesion, encrypt_md5
from SpiderTools.Tool import reFunction


class dealFormulaSpider(CrawlSpider):
    name = 'dealFormula'
    # 配置更换cookies时间
    COOKIES_SWITCH_TIME = datetime.datetime.now()

    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super(dealFormulaSpider, cls).__new__(cls)
            pathPage = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
                                    'Logs/DataDealFormulaPage.txt')
            pathDetail = os.path.join(
                os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))),
                'data/广东省自然资源厅_成交公示_广东.csv')
            cls.filePage = open(pathPage, 'w+')
            if os.path.exists(pathDetail):
                cls.fileDetail = open(pathDetail, 'a+')
            else:
                cls.fileDetail = open(pathDetail, 'a+')
                cls.fileDetail.write("""行政区,供应公告标题,供应方式,发布时间,文件标题,正文标题,宗地编号/地块编号,地块位置,土地用途,土地面积(公顷),出让年限,成交价(万元),土地用途明细(用途名称、面积),受让单位,备注,公示期,联系单位,单位地址,邮政编码,联系电话,联系人,电子邮件,项目名称,土地使用条件,爬取时间,爬取地址url,唯一标识, \n""")
        return cls.instance

    def __init__(self):
        super(dealFormulaSpider, self).__init__()
        dispatcher.connect(self.CloseSpider, signals.spider_closed)
        self.targetUrl = 'http://jcjg.nr.gd.gov.cn:8088/GisqReport7.0/ReportServer?reportlet=other/mhgg/cjgs.cpt'
        self.header = {'User-Agent': random.choice(agent_list)}
        self.origin_url = 'http://jcjg.nr.gd.gov.cn:8088/GisqReport7.0/ReportServer?_={}&__boxModel__=true&op=page_content&sessionID={}&pn=1'

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
            sesionID = getSesion(self.targetUrl)
            now_time = int(time.time() * 1000)
            res = requests.get(self.origin_url.format(now_time, sesionID), headers=self.header, allow_redirects=False, timeout=300)
            pages = sorted(re.findall('reportTotalPage=(\d*);', str(res.content.decode('gbk'))), key=lambda x: len(x), reverse=True)[0]
            sumPage = 0
            for page in range(1, int(pages)+1):
                # 每十个请求换一个 sessionID
                if sumPage < 10:
                    self.origin_url = f'http://jcjg.nr.gd.gov.cn:8088/GisqReport7.0/ReportServer?_={now_time}&__boxModel__=true&op=page_content&sessionID={sesionID}&pn={page}'
                else:
                    sesionID = getSesion(self.targetUrl)
                    now_time = int(time.time() * 1000)
                    self.origin_url = f'http://jcjg.nr.gd.gov.cn:8088/GisqReport7.0/ReportServer?_={now_time}&__boxModel__=true&op=page_content&sessionID={sesionID}&pn={page}'
                    sumPage = 0
                self.log('当前爬取页数{}'.format(page), level=logging.INFO)
                self.filePage.write(str(page))
                priority = int(pages) + 1 - int(page)
                yield Request(self.origin_url, method='GET', priority=priority, callback=self.parse_index,
                              meta={'page': page, 'priority': priority},
                              # body=requests_data, headers={'Content-Type': 'application/json'}
                              dont_filter=True,
                              )
                sumPage += 1
        except Exception as e:
            self.log(f'当前爬取页数失败, {datetime.datetime.now()}, 错误: {e}', level=logging.ERROR)
            raise IntegrationException('爬取阻塞,请重启')

    def parse_index(self, response):
        '''
        拿到总页数,
        :param response:
        :return:
        '''
        try:
            resp = Selector(text=response.body.decode('gbk'))
            dataTrs = resp.xpath('//div[@class="frozen-center offset-c"]//table[@class="x-table"]//tr[@style="height:26px;" or @style="height:27px;" or @style="height:30px;"]')
            page = response.meta.get('page')
            for dataTr in dataTrs:
                url = 'http://jcjg.nr.gd.gov.cn:8088/GisqReport7.0/ReportServer?reportlet=other/mhgg/cjgsnr.cpt&CJGS_GUID=' + dataTr.xpath('td[1]//td/text()').extract_first()
                # 行政区
                administration = dataTr.xpath('td[3]//td/text()').extract_first()
                supplyNoticeTitle = dataTr.xpath('td[4]//td/text()').extract_first()
                supllyType = dataTr.xpath('td[5]//td/text()').extract_first()
                publishTime = dataTr.xpath('td[6]//td/text()').extract_first()
                yield Request(url, method='GET', callback=self.parse_index_purperse,
                                  meta={'page': page,
                                        'supllyType': supllyType,
                                        'administration': administration,
                                        'supplyNoticeTitle': supplyNoticeTitle,
                                        'publishTime': publishTime,
                                        },
                                  # body=requests_data, headers={'Content-Type': 'application/json'}
                                  dont_filter=True,
                                  )
        except Exception as e:
            self.log(f'列表页解析失败{page}, 错误: {e}', level=logging.ERROR)

    def parse_index_purperse(self, response):
        try:
            # 获取detailID
            purperseUrl = 'http://jcjg.nr.gd.gov.cn:8088/GisqReport7.0/ReportServer?_={}&__boxModel__=true&op=page_content&sessionID={}&pn=1'
            # TODO  detailID
            id = reFunction("FR\.SessionMgr\.register\('(\d*)', contentPane\);", str(Selector(text=response.body.decode('gbk')).xpath('string(.)').extract()[0]))

            yield Request(purperseUrl.format(int(time.time() * 1000), id), method='GET', callback=self.parse_detail,
                          meta={'page': response.meta.get('page'),
                                        'supllyType': response.meta.get('supllyType'),
                                        'administration': response.meta.get('administration'),
                                        'supplyNoticeTitle': response.meta.get('supplyNoticeTitle'),
                                        'publishTime': response.meta.get('publishTime'),},
                          # body=requests_data, headers={'Content-Type': 'application/json'}
                          dont_filter=True,
                          )
        except Exception as e:
            self.log(f'详情页获取失败, 错误: {e}', level=logging.ERROR)

    def parse_detail(self, response):
        try:
            # 数据获取不全
            data = Selector(text=response.body.decode('gbk'))
            items = str(data.xpath('string(.)').extract()[0]).replace('\xa0', '').replace('\u3000', '')
            # 共有字段
            fileTitle = data.xpath('//td[@class="fh tac bw fwb f18-0 pl2 b0"]/text()').extract_first()
            # 正文标题
            textTitle = data.xpath('//td[@class="fh vat bw f8-0 b1"]/table[1]//tr[1]/td[@align="center"]/text()').extract_first()
            supllyType = response.meta.get('supllyType').strip()
            administration = response.meta.get('administration').strip()
            supplyNoticeTitle = response.meta.get('supplyNoticeTitle').strip()
            publishTime = response.meta.get('publishTime').strip()
            projectName = ''
            parcelNumber = ''
            parcelLocation = ''
            landPurpose = ''
            landArea = ''
            transferTimeLimit = ''
            transferPrice = ''
            landPurposeDetail = ''
            transferUnit = ''
            remark = ''
            publicityPeriod = ''
            contactUnit = ''
            unitAddr = ''
            postalCode = ''
            contactTel = ''
            contacter = ''
            email = ''
            lanServiceCondition = ''

            # 公告类型
            # noticeType =
            # 公示期
            publicityPeriod = reFunction(u'公示期：([\s\S]*)三、', reFunction('四、[\s\S]*', items)).strip()
            # 联系单位
            contactUnit = reFunction(u'联系单位：([\s\S]*)单位地址', reFunction('四、[\s\S]*', items)).strip()
            # 单位地址
            unitAddr = reFunction(u'单位地址：([\s\S]*)邮政编码', reFunction('四、[\s\S]*', items)).strip()
            # 邮政编码
            postalCode = reFunction(u'邮政编码：([\s\S]*)联系电话', reFunction('四、[\s\S]*', items)).strip()
            # 联系电话
            contactTel = reFunction(u'联系电话：([\s\S]*)联 系 人', reFunction('四、[\s\S]*', items)).strip()
            # 联系人
            contacter = reFunction(u'联 系 人：([\s\S]*)电子邮件', reFunction('四、[\s\S]*', items)).strip()
            # 电子邮件
            email = reFunction(u'电子邮件：([\w\.\@]*)(?:[\S]*)', reFunction('四、[\s\S]*', items)).strip()
            if '宗地编号' in items:
                for item in ['宗地编号' + _ for _ in re.findall('([\s\S]*)二、', items)[0].split('宗地编号')[1:]]:
                    # 宗地编号
                    parcelNumber = reFunction('宗地编号：(?:\s*)([\s\S]*)地块位置', item).strip()
                    # 地块位置	parcelArea
                    parcelLocation = reFunction('地块位置：(?:\s*)([\s\S]*)土地用途：', item).strip()
                    # 土地用途
                    landPurpose = reFunction('土地用途：(?:\s*)([\s\S]*)土地面积\(公顷\)', item).strip()
                    # 土地面积(公顷)
                    landArea = reFunction('土地面积\(公顷\)：(?:\s*)([\w}/\.{]*)(?:\s*)', item).strip()
                    # 项目名称
                    projectName = reFunction('项目名称：(?:\s*)([\s\S]*)土地用途明细', item).strip()
                    # 出让年限
                    transferTimeLimit = reFunction('出让年限：(?:\s*)([\s\S]*)成交价\(万元\)', item).strip()
                    # 成交价(万元)
                    transferPrice = reFunction('成交价\(万元\)：(?:\s*)([\s\S]*)土地用途明细', item).strip()
                    # 土地用途明细(用途名称、面积)
                    landPurposeDetail = reFunction('(?:\s*)面积\(公顷\)(?:\s*)([\w}/\.{]*)受让单位', item).strip() if reFunction('(?:\s*)面积\(公顷\)(?:\s*)([\w}/\.{]*)受让单位', item).strip() else reFunction('(?:\s*)([\d\.]*)(?:[\s]*)受让单位', item).strip()
                    # 受让单位
                    transferUnit = reFunction('受让单位：(?:\s*)([\w}/{]*)(?:\s*)', item).strip()
                    # 土地使用条件
                    lanServiceCondition = reFunction('土地使用条件：(?:\s*)([\s\S]*)备注', item).strip()
                    # 备注
                    # remark = reFunction(u'备注：(?:\s*)([\w}/，、\u4e00-\uffe5（）《》：\-\.＜≤。{\u3002\uff1f\uff01\uff0c\u3001\uff1b\uff1a\u201c\u201d\u2018\u2019\uff08\uff09\u300a\u300b\u3008\u3009\u3010\u3011\u300e\u300f\u300c\u300d\ufe43\ufe44\u3014\u3015\u2026\u2014\uff5e\ufe4f\uffe5]*)(?:\s*)', item).strip()
                    remark = reFunction(u'备注：(?:\s*)([\s\S]*)(?:\s*)[二、]?', item).strip()
                    # 爬取时间
                    crawlingTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    # 爬取地址url
                    url = response.url
                    # 唯一标识
                    md5Mark = encrypt_md5(parcelNumber + publishTime + parcelLocation + url)

                    # 存储数据
                    csvFile = [administration,
supplyNoticeTitle,
supllyType,
publishTime,
fileTitle,
textTitle,
# noticeType,
parcelNumber,
parcelLocation,
landPurpose,
landArea,
transferTimeLimit,
transferPrice,
landPurposeDetail,
transferUnit,
remark,
publicityPeriod,
contactUnit,
unitAddr,
postalCode,
contactTel,
contacter,
email,
projectName,
lanServiceCondition,
crawlingTime,
url,
md5Mark,
]
                    self.fileDetail.write(
                        ','.join([_.replace(',', ' ').replace('\n', '').replace('\r', '') if _ else _ for _ in csvFile]))
                    self.fileDetail.write('\n')
                    yield
                    #TODO
            elif '地块编号' in items:
                for item in ['地块编号' + _ for _ in re.findall('([\s\S]*)二、', items)[0].split('地块编号')[1:]]:
                    # 地块编号
                    parcelNumber = reFunction('地块编号：(?:\s*)([\s\S]*)地块位置', item).strip()
                    # 地块位置	parcelArea
                    parcelLocation = reFunction('地块位置：(?:\s*)([\s\S]*)土地用途：', item).strip()
                    # 土地用途
                    landPurpose = reFunction('土地用途：(?:\s*)([\s\S]*)土地面积\(公顷\)', item).strip()
                    # 土地面积(公顷)
                    landArea = reFunction('土地面积\(公顷\)：(?:\s*)([\w}/\.{]*)(?:\s*)', item).strip()
                    # 项目名称
                    projectName = reFunction('项目名称：(?:\s*)([\s\S]*)土地用途明细', item).strip()
                    # 出让年限
                    transferTimeLimit = reFunction('出让年限：(?:\s*)([\s\S]*)成交价\(万元\)', item).strip()
                    # 成交价(万元)
                    transferPrice = reFunction('成交价\(万元\)：(?:\s*)([\s\S]*)土地用途明细', item).strip()
                    # 土地用途明细(用途名称、面积)
                    landPurposeDetail = reFunction('(?:\s*)面积\(公顷\)(?:\s*)([\w}/\.{]*)受让单位', item).strip() if reFunction(
                        '(?:\s*)面积\(公顷\)(?:\s*)([\w}/\.{]*)受让单位', item).strip() else reFunction(
                        '(?:\s*)([\d\.]*)(?:[\s]*)受让单位', item).strip()
                    # 受让单位
                    transferUnit = reFunction('受让单位：(?:\s*)([\w}/{]*)(?:\s*)', item).strip()
                    # 土地使用条件
                    lanServiceCondition = reFunction('土地使用条件：(?:\s*)([\s\S]*)备注', item).strip()
                    # 备注
                    remark = reFunction(u'备注：(?:\s*)([\s\S]*)(?:\s*)[二、]?', item).strip()
                    # 爬取时间
                    crawlingTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    # 爬取地址url
                    url = response.url
                    # 唯一标识
                    md5Mark = encrypt_md5(parcelNumber + publishTime + parcelLocation + url)

                    # 存储数据
                    csvFile = [administration,
                               supplyNoticeTitle,
                               supllyType,
                               publishTime,
                               fileTitle,
                               textTitle,
                               # noticeType,
                               parcelNumber,
                               parcelLocation,
                               landPurpose,
                               landArea,
                               transferTimeLimit,
                               transferPrice,
                               landPurposeDetail,
                               transferUnit,
                               remark,
                               publicityPeriod,
                               contactUnit,
                               unitAddr,
                               postalCode,
                               contactTel,
                               contacter,
                               email,
                               projectName,
                               lanServiceCondition,
                               crawlingTime,
                               url,
                               md5Mark,
                               ]
                    self.fileDetail.write(
                        ','.join([_.replace(',', ' ').replace('\n', '').replace('\r', '') if _ else _ for _ in csvFile]))
                    self.fileDetail.write('\n')
                    yield

            #TODO
        except Exception as e:
            self.log(f'详情页数据解析失败, 错误: {e}', level=logging.ERROR)














