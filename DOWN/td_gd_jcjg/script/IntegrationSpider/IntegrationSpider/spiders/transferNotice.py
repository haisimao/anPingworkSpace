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


class transferNoticeSpider(CrawlSpider):
    name = 'transferNotice'
    # 配置更换cookies时间
    COOKIES_SWITCH_TIME = datetime.datetime.now()

    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super(transferNoticeSpider, cls).__new__(cls)
            pathPage = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
                                    'Logs/DataTransferNoticePage.txt')
            pathDetail = os.path.join(
                os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))),
                'data/广东省自然资源厅_出让公告_广东.csv')

            cls.filePage = open(pathPage, 'w+')
            if os.path.exists(pathDetail):
                cls.fileDetail = open(pathDetail, 'a+')
            else:
                cls.fileDetail = open(pathDetail, 'a+')
                cls.fileDetail.write("""文件标题, 正文标题, 公告类型, 行政区, 供应公告标题, 发布时间, 出让文件获取时间,出让文件获取地址,保证金截止时间, 确认竞买资格时间, 联系地址, 联系电话, 联系人, 开户单位, 开户银行, 银行账号, 宗地编号,宗地面积, 宗地坐落, 岀让年限, 容积率, 建筑密度(%), 绿地率(%), 建筑限高(米),土地用途, 投资强度, 保证金, 估价报告备案号, 现状土地条件, 起始价, 加价幅度,挂牌截止时间, 挂牌开始时间, 基础设施配套,是否土地平整, 排污设施状况, 备注, 爬取时间, 爬取地址url, 唯一标识, \n""")
        return cls.instance

    def __init__(self):
        dispatcher.connect(self.CloseSpider, signals.spider_closed)
        self.targetUrl = 'http://jcjg.nr.gd.gov.cn:8088/GisqReport7.0/ReportServer?reportlet=other/mhgg/crgg.cpt'
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
                priority = int(pages) + 1 - int(page)
                self.filePage.write(str(page))
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
                url = 'http://jcjg.nr.gd.gov.cn:8088/GisqReport7.0/ReportServer?reportlet=other/mhgg/ggnr.cpt&GYGG_GUID=' + dataTr.xpath('td[@style="display:none;"]//td/text()').extract_first()
                # 行政区
                administration = dataTr.xpath('td[contains(@class,"fh") and contains(@class,"bw")and contains(@class,"b1")][position() >1][1]//td/text()').extract_first()
                supplyNoticeTitle = dataTr.xpath('td[contains(@class,"fh") and contains(@class,"bw")and contains(@class,"b1")][position() >1][2]//td/text()').extract_first()
                noticeType = dataTr.xpath('td[contains(@class,"fh") and contains(@class,"bw")and contains(@class,"b1")][position() >1][3]//td/text()').extract_first()
                publishTime = dataTr.xpath('td[contains(@class,"fh") and contains(@class,"bw")and contains(@class,"b1")][position() >1][4]//td/text()').extract_first()
                # self.filePage.write(REGIONALLISM + ',' + TITLE + ',' + TYPE + ',' + PUBLISHTIME)
                yield Request(url, method='GET', callback=self.parse_index_purperse,
                                  meta={'page': page,
                                        'noticeType': noticeType,
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
            # resp = Selector(text=response.body.decode('gbk'))
            purperseUrl = 'http://jcjg.nr.gd.gov.cn:8088/GisqReport7.0/ReportServer?_={}&__boxModel__=true&op=page_content&sessionID={}&pn=1'
            # TODO  detailID
            id = reFunction("FR\.SessionMgr\.register\('(\d*)', contentPane\);", str(Selector(text=response.body.decode('gbk')).xpath('string(.)').extract()[0]))

            yield Request(purperseUrl.format(int(time.time() * 1000), id), method='GET', callback=self.parse_detail,
                          meta={'page': response.meta.get('page'),
                                        'noticeType': response.meta.get('noticeType'),
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
            data = Selector(text=response.body.decode('gbk'))
            items = str(data.xpath('string(.)').extract()[0]).replace('\xa0', '').replace('\u3000', '')
            # 按照宗地编号来获取一页有几条数据
            # dataCount = len(list(filter(None, re.findall('宗地编号', items))))
            # 共有字段
            fileTitle = data.xpath('//td[@class="fh tac bw fwb f18-0 pl2 b0"]/text()').extract_first()
            textTitle = data.xpath('//td[@class="fh vat bw f8-0 b1"]/table[1]//tr[1]/td[@align="center"]/text()').extract_first()
            noticeType = response.meta.get('noticeType').strip()
            administration = response.meta.get('administration').strip()
            supplyNoticeTitle = response.meta.get('supplyNoticeTitle').strip()
            publishTime = response.meta.get('publishTime').strip()
            parcelNumber = ''
            parcelArea = ''
            parcelLocation = ''
            transferTimeLimit = ''
            plotRatio = ''
            buildingDensity = ''
            greenRatio = ''
            buldingHP = ''
            landPurpose = ''
            investmentIntensity = ''
            cashDeposit = ''
            evaluateNum = ''
            landCondition = ''
            startPrice = ''
            bidIncrenment = ''
            hangOutDeadTime = ''
            hangOutStartTime = ''
            supportingInfrastructure = ''
            landItact = ''
            sewageDisposalFacility = ''
            remark = ''
            transferTime = reFunction(u'申请人可于((?:[\w\s\u4e00-\u9fa5]*)至(?:[\s\w\u4e00-\u9fa5]*))到', reFunction('四、[\s\S]*五、', items)).strip()
            transferAddr = reFunction(u'申请人可于(?:[\w\s\u4e00-\u9fa5]*)至(?:[\s\w\u4e00-\u9fa5]*)到 ([\s\S\w\u4e00-\u9fa5.\n\r]*出让文件)', reFunction('四、[\s\S]*五、', items)).strip().replace('获取 挂牌 出让文件', '')

            try:
                time1 = reFunction(u'保证金的截止时间为([\w\s\u4e00-\u9fa5]*)。', reFunction('五、[\s\S]*六、', items)).strip()
                time2 = reFunction(u'将在([\w\s\u4e00-\u9fa5]*)前确认其竞买资格',reFunction('五、[\s\S]*六、', items)).strip()
                # 保证金截止时间
                # time.strftime("%Y-%m-%d %H:%M", time.strptime('2020年05月19日09时00分', u"%Y年%m月%d日%H时%M分"))
                depositTime = time.strftime("%Y-%m-%d %H:%M", time.strptime(time1, u"%Y年%m月%d日%H时%M分"))
                # 确认竞买资格时间
                affirmBuyTime = time.strftime("%Y-%m-%d %H:%M", time.strptime(time2, u"%Y年%m月%d日%H时%M分"))
            except:
                # 保证金截止时间
                depositTime = time1
                # 确认竞买资格时间
                affirmBuyTime = time2
            # 联系地址
            address = reFunction(u'联系地址：([\s\S]*)联 系 人', reFunction('八、[\s\S]*', items)).strip()
            # 电话
            tel = reFunction(u'联系电话：([\s\S]*)开户单位', reFunction('八、[\s\S]*', items)).strip()
            # 联系人
            linkman = reFunction(u'联系电话：([\s\S]*)开户单位', reFunction('八、[\s\S]*', items)).strip()
            # 开户单位
            accountOpener = reFunction(u'开户单位：([\s\S]*)开户银行', reFunction('八、[\s\S]*', items)).strip()
            # 开户银行
            depositBank = reFunction(u'开户银行：([\s\S]*)银行帐号', reFunction('八、[\s\S]*', items)).strip()
            # 银行帐号
            bankAccount = reFunction(u'银行帐号：([\w]*)(?:[\S]*)', reFunction('八、[\s\S]*', items)).strip()

            if '宗地编号' in items:
                for item in ['宗地编号' + _ for _ in re.findall('([\s\S]*)二、', items)[0].split('宗地编号')[1:]]:
                    # 宗地编号
                    parcelNumber = reFunction('宗地编号：(?:\s*)([\s\S]*)宗地总面积', item).strip()
                    # 宗地面积	parcelArea
                    parcelArea = reFunction('宗地总面积：(?:\s*)([\w}/{]*)(?:\s*)', item).strip()
                    # 宗地坐落	parcelLocation
                    parcelLocation = reFunction('宗地坐落：(?:\s*)([\s\S]*)出让年限', item).strip()
                    # 岀让年限 	transferTimeLimit
                    transferTimeLimit = reFunction('出让年限：(?:\s*)([\w}/{]*)(?:\s*)', item).strip()
                    # 容积率	plotRatio
                    plotRatio = reFunction('容积率：(?:\s*)([\w}/{]*)(?:\s*)', item).strip()
                    # 建筑密度(%)	buildingDensity
                    buildingDensity = reFunction('建筑密度\(%\)：([\s\S]*)绿化率', item).strip()
                    # 绿地率(%)	greenRatio
                    greenRatio = reFunction('绿[地化]率\(%\)：([\s\S]*)建筑限高', item).strip()
                    # 建筑限高(米)	buldingHP
                    buldingHP = reFunction('建筑限高\(米\)：(?:\s*)([\w}{/]*)主要用途', item).strip()
                    # 土地用途	landPurpose
                    landPurpose = reFunction('主要用途：(?:\s*)([\w}{/]*)(?:\s*)', item).strip()
                    # 投资强度 investmentIntensity
                    investmentIntensity = reFunction('投资强度：(?:\s*)([\w}{/]*)(?:\s*)保证金', item).strip()
                    # 保证金	cashDeposit
                    cashDeposit = reFunction('保证金：(?:\s*)([\w}{/]*)(?:\s*)', item).strip()
                    # 估价报告备案号	evaluateNum
                    evaluateNum = reFunction('估价报告备案号(?:\s*)([A-Za-z0-9_}{/]*)(?:\s*)', item).strip()
                    # 现状土地条件	landCondition
                    landCondition = reFunction('([：\u4e00-\u9fa5 ]*)', reFunction('估价报告备案号：([\s\S]*)起始价', item)).strip()
                    # TODO 起始价	startPrice
                    startPrice = reFunction('起始价：(?:\s*)([\w}/{]*)(?:\s*)', item).strip()
                    # 加价幅度	bidIncrenment
                    bidIncrenment = reFunction('加价幅度：(?:\s*)([\w}/{]*)(?:\s*)', item).strip()
                    try:
                        time3 = reFunction('挂牌[（竞价）]*截止时间：(?:\s*)([\w}/{]*)(?:\s*)', item).strip()
                        time4 = reFunction('挂牌[（竞价）]*开始时间：(?:\s*)([\w}/{]*)(?:\s*)', item).strip()
                        # 挂牌截止时间
                        hangOutDeadTime = time.strftime("%Y-%m-%d %H:%M", time.strptime(time3, u"%Y年%m月%d日%H时%M分"))
                        # 挂牌开始时间
                        hangOutStartTime = time.strftime("%Y-%m-%d %H:%M", time.strptime(time4, u"%Y年%m月%d日%H时%M分"))
                    except:
                        # 保证金截止时间
                        depositTime = time3
                        # 确认竞买资格时间
                        affirmBuyTime = time4
                    # 基础设施配套	supportingInfrastructure
                    supportingInfrastructure = reFunction('基础设施配套：(?:\s*)([\w}/{]*)(?:\s*)', item).strip()
                    # 是否土地平整	landItact
                    landItact = reFunction('是否土地平整[: ：](?:\s*)([\w}/{]*)(?:\s*)', item).strip()
                    # 排污设施状况	sewageDisposalFacility
                    sewageDisposalFacility = reFunction('排污设施状况：(?:\s*)([\w}/{]*)(?:\s*)', item).strip()
                    # 备注	remark
                    remark = reFunction('备注：([\s\S]*)(?:\s*)', item).strip()

                    # 爬取时间
                    crawlingTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    # 爬取地址url
                    url = response.url
                    # 唯一标识
                    md5Mark = encrypt_md5(fileTitle+publishTime+transferTime+url)

                    csvFile = [fileTitle, textTitle, noticeType, administration, supplyNoticeTitle, publishTime, transferTime, transferAddr,
                    depositTime, affirmBuyTime, address, tel, linkman, accountOpener, depositBank, bankAccount, parcelNumber,
                    parcelArea, parcelLocation, transferTimeLimit, plotRatio, buildingDensity, greenRatio, buldingHP,
                    landPurpose, investmentIntensity, cashDeposit, evaluateNum, landCondition, startPrice, bidIncrenment,
                    hangOutDeadTime, hangOutStartTime, supportingInfrastructure, landItact, sewageDisposalFacility, remark, crawlingTime, url, md5Mark, '\n']
                    # 存储数据
                    self.fileDetail.write(','.join([_.replace(',', ' ').replace('\n', '').replace('\r', '') if _ else _  for _ in csvFile]))
                    self.fileDetail.write('\n')

            yield
            # TODO
        except Exception as e:
            self.log(f'详情页数据解析失败, 错误: {e}', level=logging.ERROR)














