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


class supplyLandPlanSpider(CrawlSpider):
    name = 'supplyLandPlan'
    # 配置更换cookies时间
    COOKIES_SWITCH_TIME = datetime.datetime.now()

    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super(supplyLandPlanSpider, cls).__new__(cls)
            pathPage = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
                                    'Logs/DatasupplyLandPlanPage.txt')
            pathDetail = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))), 'data/广东省自然资源厅_供地计划_广东.csv')
            cls.filePage = open(pathPage, 'w+')
            if os.path.exists(pathDetail):
                cls.fileDetail = open(pathDetail, 'a+')
            else:
                cls.fileDetail = open(pathDetail, 'a+')
                cls.fileDetail.write("""行政区,供地计划标题,发布时间,文件标题,总供应面积合计,供应计划年度,工矿仓储用地,商服用地,住房供地总量,住宅用地-廉租房用地,住宅用地经济适用房用地,住宅用地-棚改用地总量,住宅用地棚改用地廉租房,住宅用地-棚改用地经济适用房用地,中小套型商品住房,住宅用地-商品房用地,住宅用地-其他用地,公共管理与公共服务用地,交通运输用地,水域及水利设施用地,特殊用地,公共租赁房,限价商品房用地面积,商品住房用地-中小套型商品住房用地 ,商品住房用地-总量,保障性安居工程和中小套型商品房用地占比(%), 爬取时间, 爬取地址url, 唯一标识, \n""")
        return cls.instance

    def __init__(self):
        super(supplyLandPlanSpider, self).__init__()
        dispatcher.connect(self.CloseSpider, signals.spider_closed)
        self.targetUrl = 'http://jcjg.nr.gd.gov.cn:8088/GisqReport7.0/ReportServer?reportlet=other/mhgg/gdjh.cpt'
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
        :param response:
        :return:
        '''
        try:
            resp = Selector(text=response.body.decode('gbk'))
            dataTrs = resp.xpath('//table[@class="x-table"]')[1].xpath('tbody/tr')
            page = response.meta.get('page')
            for dataTr in dataTrs:
                # 行政区
                url = 'http://jcjg.nr.gd.gov.cn:8088/GisqReport7.0/ReportServer?reportlet=other/mhgg/gdjhnr.cpt&GYJH_GUID=' + dataTr.xpath('td[1]//td/text()').extract_first()
                'http://jcjg.nr.gd.gov.cn:8088/GisqReport7.0/ReportServer?reportlet=other/mhgg/gdjhnr.cpt&GYJH_GUID='
                # 行政区
                administration = dataTr.xpath('td[3]//td/text()').extract_first()
                supplyLandTitle = dataTr.xpath('td[4]//td/text()').extract_first()
                publishTime = dataTr.xpath('td[5]//td/text()').extract_first()
                yield Request(url, method='GET', callback=self.parse_index_purperse,
                                  meta={'page': page,
                                        'supplyLandTitle': supplyLandTitle,
                                        'administration': administration,
                                        'publishTime': publishTime,
                                        },
                                  # body=requests_data, headers={'Content-Type': 'application/json'}
                                  dont_filter=True,
                                  )
        except Exception as e:
            self.log(f'列表页解析失败{page}, 错误: {e}', level=logging.ERROR)

    def parse_index_purperse(self, response):
        # 获取detailID
        # resp = Selector(text=response.body.decode('gbk'))
        try:
            purperseUrl = 'http://jcjg.nr.gd.gov.cn:8088/GisqReport7.0/ReportServer?_={}&__boxModel__=true&op=page_content&sessionID={}&pn=1'

            # TODO  detailID
            id = reFunction("FR\.SessionMgr\.register\('(\d*)', contentPane\);", str(Selector(text=response.body.decode('gbk')).xpath('string(.)').extract()[0]))

            yield Request(purperseUrl.format(int(time.time() * 1000), id), method='GET', callback=self.parse_detail,
                          meta={'page': response.meta.get('page'),
                                'supplyLandTitle': response.meta.get('supplyLandTitle'),
                                'administration': response.meta.get('administration'),
                                'publishTime': response.meta.get('publishTime'),
                                         },
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
            supplyLandTitle = response.meta.get('supplyLandTitle')
            administration =  response.meta.get('administration')
            publishTime =  response.meta.get('publishTime')
            # detailPage
            # 写入时, 没有的字段置为空
            totalSupplyLand = ''
            yearSupplyPlan = ''
            industrialLand = ''
            businessLand = ''
            totalHousionSupply = ''
            low_rentLand = ''
            affordableHousing = ''
            pengGaiLand = ''
            low_rentpengGaiLand = ''
            pengGaiAffordableHousing = ''
            pengGaiCommercialHousing = ''
            commercialHousing = ''
            ortherHousingLand = ''
            publicServiceLand = ''
            transportationLand = ''
            waterAreaLand = ''
            specialLand = ''
            publicRentalLand = ''
            limitCommercialLand = ''
            mediumCommercialLand = ''
            totalCommercialLand = ''
            commercialRatio = ''

            if '公共管理与公共服务用地' in items and '合计' in items and '特殊用地' in items and '水域及水利设施用地' in items and reFunction('经济适用房用[地](?:\s*)([\S\s]*)(?:\s*)棚改用地', items):
                # 文件标题
                fileTitle = data.xpath('//td[@class="fh tac bw fwb f18-0 pl2 b0"]/text()').extract_first()
                # 总供应面积合计
                totalSupplyLand = reFunction('合计(?:\s*)([\d\.]*)(?:\s*)', items)
                # 供应计划年度
                yearSupplyPlan = reFunction('(\d{4})年度国有建设用地供应计划', items)
                # 工矿仓储用地: 供应面积(公顷)、新增、存量、
                industrialLand = reFunction('工矿仓储用地(?:\s*)([\S\s]*)(?:\s*)商服用地', items)
                # 商服用地: 供应面积(公顷)、新增、存量
                businessLand = reFunction('商服用地(?:\s*)([\S\s]*)(?:\s*)住宅用地', items)
                # # 住房供地总量
                # totalHousionSupply = reFunction('小计(?:\s*)([\d\.]*)(?:\s*)([\d\.]*)(?:\s*)([\d\.]*)(?:\s*)', items)[0] if reFunction('小计(?:\s*)([\d\.]*)(?:\s*)([\d\.]*)(?:\s*)([\d\.]*)(?:\s*)', items) else ' '
                # 住宅用地 - 廉租房用地: 供应面积(公顷)、新增、存量、
                low_rentLand = '|'.join(reFunction('廉租房用地(?:\s*)([\S\s]*)(?:\s*)棚改用地', items))
                # 住宅用地经济适用房用地: 供应面积(公顷)、新增、存量
                affordableHousing = '|'.join(reFunction('经济适用房用[地](?:\s*)([\S\s]*)(?:\s*)棚改用地', items))
                # 住宅用地 - 棚改用地
                pengGaiLand = '|'.join(reFunction('棚改用地(?:\s*)([\S\s]*)(?:\s*)经济适用房用', items))
                # # 住宅用地棚改用地廉租房: 供应面积(公顷) ,新增、存量
                # low_rentpengGaiLand = '|'.join(reFunction('棚改用地(?:\s*)([\S\s]*)(?:\s*)经济适用房用', items))
                # 住宅用地 - 商品房用地: 供应面积(公顷)、新增、存量
                commercialHousing = '|'.join(reFunction('商品房用地(?:\s*)([\S\s]*)(?:\s*)其他用地', items))
                # 住宅用地 - 其他用地: 供应面积(公顷)、新增、存量
                ortherHousingLand = '|'.join(reFunction('其他用地(?:\s*)([\S\s]*)(?:\s*)小计', items))
                # 公共管理与公共服务用地: 供应面积(公顷)、新增、存量
                publicServiceLand = '|'.join(reFunction('公共管理与公共服务用地(?:\s*)([\S\s]*)(?:\s*)交通运输用地', items))
                # 交通运输用地: 供应面积(公顷)、新增、存量
                transportationLand = '|'.join(reFunction('交通运输用地(?:\s*)([\S\s]*)(?:\s*)水域及水利设施用地', items))
                # 水域及水利设施用地: 供应面积(公顷)、新增、存量
                waterAreaLand = '|'.join(reFunction('水域及水利设施用地(?:\s*)([\S\s]*)(?:\s*)特殊用地', items))
                # 特殊用地: 供应面积(公顷)、新增、存量
                specialLand = '|'.join(reFunction('特殊用地(?:\s*)([\S\s]*)(?:\s*)合计', items))
                # 唯一标识
                # 爬取时间
                crawlingTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                # 爬取地址url
                url = response.url
                md5Mark = encrypt_md5(fileTitle + totalSupplyLand + yearSupplyPlan + url)
                csvFile = [administration, supplyLandTitle, publishTime, fileTitle, totalSupplyLand, yearSupplyPlan,
                           industrialLand, businessLand,
                           totalHousionSupply, low_rentLand, affordableHousing, pengGaiLand, low_rentpengGaiLand,
                           pengGaiAffordableHousing, pengGaiCommercialHousing, commercialHousing, ortherHousingLand,
                           publicServiceLand,
                           transportationLand, waterAreaLand, specialLand, publicRentalLand, limitCommercialLand,
                           mediumCommercialLand,
                           totalCommercialLand, commercialRatio, crawlingTime, url, md5Mark]
                self.fileDetail.write(','.join([_.replace(',', ' ').replace('\n', '').replace('\r', '') if _ else _ for _ in csvFile]))
                self.fileDetail.write('\n')
                yield

            elif '总供应面积合计：' in items and '各类棚户区改造用地' in items \
                    and reFunction('总供应面积合计：(?:\s*)([\S\s]*)(?:\s*)供应计划年度', items) \
                    and len(re.split('\s*', reFunction('商品住房(?:\s*)([\s\S]*)', reFunction('总 量(?:\s*)([\s\S]*)%', items)))) > 3:
                # 文件标题
                fileTitle = data.xpath('//td[@class="fh tac bw fwb f18-0 pl2 b0"]/text()').extract_first()
                for item in [reFunction('([\S\s]*)(?:[\d\.]*%)', '总供应面积合计：' + _) for _ in re.findall('([\s\S]*)', items)[0].split('总供应面积合计：')[1:]]:
                    # 总供应面积合计
                    totalSupplyLand = reFunction('总供应面积合计：(?:\s*)([\d\w\.]*)(?:\s*)', item)
                    # 供应计划年度
                    yearSupplyPlan = reFunction('供应计划年度：(?:\s*)([\d\w\.]*)(?:\s*)', item)
                    # 商服用地: 供应面积(公顷)、新增、存量
                    businessLand = reFunction('商服用地：(?:\s*)([\d\w\.]*)(?:\s*)', item)
                    # 工矿仓储用地: 供应面积(公顷)、新增、存量、
                    industrialLand = reFunction('工矿仓储用地：(?:\s*)([\d\w\.]*)(?:\s*)', item)
                    # businessLand
                    # 住房供地总量
                    totalHousionSupply = reFunction('住房供地总量：(?:\s*)([\S\s]*)(?:\s*)其中存量', item)
                    # 先获取数字在一一对应
                    dataList = re.split('\s*', reFunction('商品住房(?:\s*)([\s\S]*)', reFunction('总 量(?:\s*)([\s\S]*)[%]?', item)))
                    # 住宅用地 - 廉租房用地: 供应面积(公顷)、新增、存量、
                    low_rentLand = dataList[0]
                    # 住宅用地经济适用房用地: 供应面积(公顷)、新增、存量
                    affordableHousing = dataList[1]
                    # 住宅用地 - 棚改用地  - 总量
                    pengGaiLand = dataList[2]
                    # 住宅用地棚改用地廉租房: 供应面积(公顷) 新增、存量
                    low_rentpengGaiLand = dataList[3]
                    # 住宅用地 - 棚改用地经济适用房用地: 供应面积公顷)、新增、存量
                    pengGaiAffordableHousing = dataList[4]
                    # 住宅用地 - 棚改用地 - 中小套型商品住房: 供应面积(公顷)、新增、存量
                    pengGaiCommercialHousing = dataList[5]
                    # # 住宅用地 - 商品房用地: 供应面积(公顷)、新增、存量
                    # commercialHousing = dataList[9]
                    # 公共租赁房: 划拨用地面积、出让用地面积
                    publicRentalLand = dataList[6] + '|' + dataList[7]
                    # 限价商品房用地面积
                    limitCommercialLand = dataList[8]
                    # 商品住房用地 - 中小套型商品住房用地
                    mediumCommercialLand = dataList[10]
                    # 商品住房用地 - 总量
                    totalCommercialLand = dataList[9]
                    # 保障性安居工程和中小套型商品房用地占比( %)
                    commercialRatio = dataList[11] + '%' if '%' not in dataList[11] else dataList[11]
                    # 公共管理与公共服务用地: 供应面积(公顷)、新增、存量
                    publicServiceLand = reFunction('公共管理与服务用地：(?:\s*)([\d\w\.]*)(?:\s*)', item)
                    # 交通运输用地: 供应面积(公顷)、新增、存量
                    transportationLand = reFunction('交通运输用地：(?:\s*)([\d\w\.]*)(?:\s*)', item)
                    # 水域及水利设施用地: 供应面积(公顷)、新增、存量
                    waterAreaLand = reFunction('水域及水利设施用地：(?:\s*)([\d\w\.]*)(?:\s*)', item)
                    # 特殊用地: 供应面积(公顷)、新增、存量
                    specialLand = reFunction('特殊用地：(?:\s*)([\d\w\.]*)(?:\s*)', item)
                    # 爬取时间
                    crawlingTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    # 爬取地址url
                    url = response.url
                    md5Mark = encrypt_md5(fileTitle + totalSupplyLand + yearSupplyPlan + url)
                    csvFile = [administration, supplyLandTitle, publishTime, fileTitle, totalSupplyLand, yearSupplyPlan,
                               industrialLand, businessLand,
                               totalHousionSupply, low_rentLand, affordableHousing, pengGaiLand, low_rentpengGaiLand,
                               pengGaiAffordableHousing, pengGaiCommercialHousing, commercialHousing, ortherHousingLand,
                               publicServiceLand,
                               transportationLand, waterAreaLand, specialLand, publicRentalLand, limitCommercialLand,
                               mediumCommercialLand,
                               totalCommercialLand, commercialRatio, crawlingTime, url, md5Mark]
                    self.fileDetail.write(','.join([_.replace(',', ' ').replace('\n', '').replace('\r', '') if _ else _ for _ in csvFile]))
                    self.fileDetail.write('\n')
                    yield
            else:
                # 文件标题
                fileTitle = data.xpath('//td[@class="fh tac bw fwb f18-0 pl2 b0"]/text()').extract_first()
                # 商服用地:供应面积(公顷)、新增、存量
                businessLand = reFunction('商服用地[占]?(?:\s*)([[\d}/\.{]*)(?:\s*)公顷', items)
                # 工矿仓储用地:供应面积(公顷)、新增、存量、
                industrialLand = reFunction('工矿仓储用地[占]?(?:\s*)([[\d}/\.{]*)(?:\s*)公顷', items)
                # 住房供地总量
                totalHousionSupply = reFunction('住宅用地[占]?(?:\s*)([[\d}/\.{]*)(?:\s*)公顷', items)
                # 公共管理与公共服务用地
                publicServiceLand = reFunction('公共管理与公共服务用地[占]?(?:\s*)([[\d}/\.{]*)(?:\s*)公顷', items)
                # 交通运输用地:供应面积(公顷)、新增、存量
                transportationLand = reFunction('交通运输用地[占]?(?:\s*)([[\d}/\.{]*)(?:\s*)公顷', items)
                # 水域及水利设施用地:供应面积(公顷)、新增、存量
                waterAreaLand = reFunction('水域及水利设施用地[占]?(?:\s*)([[\d}/\.{]*)(?:\s*)公顷', items)
                # 特殊用地: 供应面积(公顷)、新增、存量
                specialLand = reFunction('特殊用地(?:\s*)([[\d}/\.{]*)(?:\s*)公顷', items)

                # 爬取时间
                crawlingTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                # 爬取地址url
                url = response.url
                md5Mark = encrypt_md5(fileTitle + totalSupplyLand + yearSupplyPlan + url)
                csvFile = [administration,supplyLandTitle,publishTime,fileTitle,totalSupplyLand,yearSupplyPlan,industrialLand,businessLand,
                            totalHousionSupply,low_rentLand,affordableHousing,pengGaiLand,low_rentpengGaiLand,
                            pengGaiAffordableHousing,pengGaiCommercialHousing,commercialHousing,ortherHousingLand,publicServiceLand,
                            transportationLand,waterAreaLand,specialLand,publicRentalLand,limitCommercialLand,mediumCommercialLand,
                            totalCommercialLand,commercialRatio, crawlingTime, url, md5Mark]
                self.fileDetail.write(','.join([_.replace(',', ' ').replace('\n', '').replace('\r', '') if _ else _ for _ in csvFile]))
                self.fileDetail.write('\n')
                yield

        except Exception as e:
            self.log(f'详情页数据解析失败, 错误: {e}', level=logging.ERROR)

# TODO  规范部分时间输出 , 拟引入Log4j收集日志,












