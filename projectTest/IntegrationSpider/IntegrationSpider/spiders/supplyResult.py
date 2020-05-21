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
from SpiderTools.Tool import reFunction, strfTime


class supplyResultSpider(CrawlSpider):
    name = 'supplyResult'
    # 配置更换cookies时间
    COOKIES_SWITCH_TIME = datetime.datetime.now()


    def __init__(self):
        super(supplyResultSpider, self).__init__()
        dispatcher.connect(self.CloseSpider, signals.spider_closed)
        self.targetUrl = 'http://jcjg.nr.gd.gov.cn:8088/GisqReport7.0/ReportServer?reportlet=other/mhgg/gyjg.cpt'
        self.header = {'User-Agent': random.choice(agent_list)}
        try:
            pathPage = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
                                    'Logs/DataSupplyResultPage.txt')
            pathDetail = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))),
                'data/DataSupplyResultDetail.csv')
            self.filePage = open(pathPage, 'w+')
            self.fileDetail = open(pathDetail, 'a+')
        except Exception as e:
            self.log('supplyResultSpider文件打开失败,At{}'.format(datetime.datetime.now()), level=logging.ERROR)
        fileds = ['administration', 'parcelLocation', 'totalArea', 'landPurpose', 'signTime', 'projectName', 'projectLocation',
         'area', 'landSource',
         'supplyType', 'landUsegeTerm', 'classification', 'landLevel', 'transferPrice', 'issue', 'paymentDate',
         'paymentAmount', 'remark', 'landHolder',
         'plotRatioUP', 'plotRatioLOWER', 'agreedDeliveryTime', 'agreedStartTime', 'agreedCompletionTime',
         'actualStartTime', 'actualCompletionTime', 'approvedUnit', 'contractTime', 'crawlingTime', 'url', 'md5Mark', '\n']
        self.fileDetail.write(','.join(fileds))
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
            res = requests.get(self.origin_url.format(now_time, sesionID), headers=self.header, allow_redirects=False, timeout=60)
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
            self.log(f'当前爬取失败页数{page}, {datetime.datetime.now()}, 错误: {e}', level=logging.ERROR)
            raise IntegrationException('爬取阻塞,请重启')

    def parse_index(self, response):
        '''
        拿到总页数,
        :param response:
        :return:
        '''
        try:
            resp = Selector(text=response.body.decode('gbk'))
            dataTrs = resp.xpath('//table[@class="x-table"]/tbody/tr[position()>3]')
            page = response.meta.get('page')
            for dataTr in dataTrs:
                url = 'http://jcjg.nr.gd.gov.cn:8088/GisqReport7.0/ReportServer?reportlet=other/mhgg/gyjgnr.cpt&GD_GUID=' + dataTr.xpath('td[@style="display:none;"]/text()').extract_first()
                # 行政区
                administration = dataTr.xpath('td[position()>2 and position()<last()][1]/text()').extract_first()
                # 宗地坐落
                parcelLocation = dataTr.xpath('td[position()>2 and position()<last()][2]/span/text()').extract_first()
                # 总面积
                totalArea = dataTr.xpath('td[position()>2 and position()<last()][3]/text()').extract_first()
                # 宗地坐落
                landPurpose = dataTr.xpath('td[position()>2 and position()<last()][4]/text()').extract_first()
                # 供应方式
                supplyType = dataTr.xpath('td[position()>2 and position()<last()][5]/text()').extract_first()
                # 签订日期
                signTime = dataTr.xpath('td[position()>2 and position()<last()][6]/text()').extract_first()
                yield Request(url, method='GET', callback=self.parse_index_purperse,
                                  meta={'page': page,
                                        'signTime': signTime,
                                        'administration': administration,
                                        'parcelLocation': parcelLocation,
                                        'totalArea': totalArea,
                                        'landPurpose': landPurpose,
                                        'supplyType': supplyType,
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
                                        'signTime': response.meta.get('signTime'),
                                        'administration': response.meta.get('administration'),
                                        'parcelLocation': response.meta.get('parcelLocation'),
                                        'totalArea': response.meta.get('totalArea'),
                                        'landPurpose': response.meta.get('landPurpose'),
                                        'supplyType': response.meta.get('supplyType'),},
                          # body=requests_data, headers={'Content-Type': 'application/json'}
                          dont_filter=True,
                          )
        except Exception as e:
            self.log(f'详情页获取失败, 错误: {e}', level=logging.ERROR)

    def parse_detail(self, response):
        try:
            data = Selector(text=response.body.decode('gbk'))
            items = str(data.xpath('string(.)').extract()[0]).replace('\xa0', '').replace('\u3000', '')
            # 共有字段
            signTime = response.meta.get('signTime')
            administration =  response.meta.get('administration')
            parcelLocation =  response.meta.get('parcelLocation')
            totalArea = response.meta.get('totalArea')
            # detailPage
            # 项目名称
            projectName = reFunction('项目名称：(?:\s*)([\s\S]*)项目位置', items).strip()
            # 项目位置
            projectLocation = reFunction('项目位置：(?:\s*)([\s\S]*)面积（公顷）', items).strip()
            # 面积(公顷)
            area = reFunction('面积（公顷）：(?:\s*)([\s\S]*)土地来源', items).strip()
            # 土地来源
            landSource = reFunction('土地来源：(?:\s*)([\s\S]*)土地用途', items).strip()
            # 土地用途
            landPurpose = reFunction('土地用途：(?:\s*)([\s\S]*)供地方式', items).strip()
            # 供地方式
            supplyType = reFunction('供地方式：(?:\s*)([\s\S]*)土地使用年限', items).strip()
            # landUsegeTerm
            landUsegeTerm = reFunction('土地使用年限：(?:\s*)([\s\S]*)行业分类', items).strip()
            # 行业分类
            classification = reFunction('行业分类：(?:\s*)([\s\S]*)土地级别', items).strip()
            # 土地级别
            landLevel = reFunction('行业分类：(?:\s*)([\s\S]*)成交价格', items).strip()
            # 成交价格（万元）
            transferPrice = reFunction('成交价格（万元）：(?:\s*)([\s\S]*)分期支付约定', items).strip()
            # TODO
            stagesData = reFunction('分期支付约定：(?:\s*)([\s\S]*)土地使用权人', items).strip()
            # 分期支付约定-支付期号
            issue = ''
            # 分期支付约定-约定支付日期
            paymentDate = '|'.join([strfTime(_) for _ in list(filter(None, re.findall('\d{4}年\d{2}月\d{2}日', stagesData)))])
            # 分期支付约定-约定支付金额(万元)
            paymentAmount = ''
            # 分期支付约定-备注
            remark = ''
            for _ in range(0, len(list(filter(None, re.findall('年', stagesData))))):
                # id 一定是从 9 开始 如果有多个项, 用Xpath一一匹配
                id = _ + 9
                issue += data.xpath(f'//*[@id="r-{id}-0"]/td[1]/text()').extract_first()+'|' if data.xpath(f'//*[@id="r-{id}-0"]/td[1]/text()').extract_first() else ' '
                paymentAmount += data.xpath(f'//*[@id="r-{id}-0"]/td[3]/text()').extract_first()+'|' if data.xpath(f'//*[@id="r-{id}-0"]/td[3]/text()').extract_first() else ' '
                remark += data.xpath(f'//*[@id="r-{id}-0"]/td[4]/text()').extract_first()+'|' if data.xpath(f'//*[@id="r-{id}-0"]/td[4]/text()').extract_first() else ' '

            # TODO
            # 土地使用权人
            landHolder = reFunction('土地使用权人：(?:\s*)([\s\S]*)约定容积率', items).strip()
            # 约定容积率上限
            plotRatioLOWER = reFunction('上限：(?:\s*)([\s\S]*)约定交地时间', items).strip()
            # 约定容积率下限
            plotRatioUP = reFunction('下限：(?:\s*)([\s\S]*)上限', items).strip()
            # 约定交地时间
            agreedDeliveryTime = strfTime(reFunction('约定交地时间：(?:\s*)([\s\S]*)约定开工时间', items).strip())
            # 约定开工时间
            agreedStartTime = strfTime(reFunction('约定开工时间：(?:\s*)([\s\S]*)约定竣工时间', items).strip())
            # 约定竣工时间
            agreedCompletionTime = strfTime(reFunction('约定竣工时间：(?:\s*)([\s\S]*)实际开工时间', items).strip())
            # 实际开工时间
            actualStartTime = strfTime(reFunction('实际开工时间：(?:\s*)([\s\S]*)实际竣工时间', items).strip())
            # 实际竣工时间
            actualCompletionTime = strfTime(reFunction('实际竣工时间：(?:\s*)([\s\S]*)批准单位', items).strip())
            # 批准单位
            approvedUnit = reFunction('批准单位：(?:\s*)([\s\S]*)合同签订日期', items).strip()
            # 合同签订日期
            contractTime = strfTime(reFunction('合同签订日期：(?:\s*)([\s\S]*)', items).strip())
            # 爬取时间
            crawlingTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            # 爬取地址url
            url = response.url
            # 唯一标识
            md5Mark = encrypt_md5(landSource+projectLocation+projectName+url)

            csvFile = [administration,parcelLocation,totalArea,landPurpose,signTime,projectName,projectLocation,area,landSource,
    supplyType,landUsegeTerm,classification,landLevel,transferPrice,issue,paymentDate,paymentAmount,remark,landHolder,
    plotRatioUP,plotRatioLOWER,agreedDeliveryTime,agreedStartTime,agreedCompletionTime,actualStartTime,actualCompletionTime,approvedUnit,contractTime,crawlingTime, url, md5Mark, '\n']
            # 存储数据
            self.fileDetail.write(','.join([_.replace(',', ' ').replace('\n', '').replace('\r', '') if _ else _ for _ in csvFile]))
            self.fileDetail.write('\n')
            yield
            #TODO
        except Exception as e:
            self.log(f'详情页数据解析失败, 错误: {e}', level=logging.ERROR)














