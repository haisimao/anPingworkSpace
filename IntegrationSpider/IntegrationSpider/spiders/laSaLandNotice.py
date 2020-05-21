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


class laSaLandNoticeSpider(CrawlSpider):
    name = 'laSaLandNotice'
    # 配置更换cookies时间
    COOKIES_SWITCH_TIME = datetime.datetime.now()

    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super(laSaLandNoticeSpider, cls).__new__(cls)
            pathPage = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),'Logs/laSaLandNoticePage.txt')
            cls.pathDetail = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))), 'data/拉萨公共资源交易网_土地矿产公告_拉萨.csv')
            cls.filePage = open(pathPage, 'w+')
            if os.path.exists(cls.pathDetail):
                cls.fileDetail = open(cls.pathDetail, 'a+')
            else:
                cls.fileDetail = open(cls.pathDetail, 'a+')
                with open(cls.pathDetail, 'a+') as fp:
                    fp.write("""文件标题,文章来源,更新时间,宗地编号,宗地坐落,面积,土地用途,出让年限,容积率,绿地率,建筑密度,建筑限高,竞买保证金,起始价,增价幅度,其他说明,爬取地址url,唯一标识,\n""")
        return cls.instance

    def __init__(self):
        super(laSaLandNoticeSpider, self).__init__()
        dispatcher.connect(self.CloseSpider, signals.spider_closed)
        self.targetUrl = 'http://ggzy.lasa.gov.cn/Article/SearchArticle'
        self.header = {'User-Agent': random.choice(agent_list)}
        self.reStr = '（）\w\.:： 。 \(\)〔〕㎡≤；，≥《》\-\/\%,、\.﹪㎡'

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
            for page in range(1, 17):
                requests_data = {
'categoryId': '729',
'typeId': '0',
'pageNum': str(page),
'pageSize': '10',
'search': 'false',
'Title': '',
'StartTime': '',
'EndTime': '',
'area': '%E8%AF%B7%E9%80%89%E6%8B%A9',
}
                priority = 17 - int(page)
                yield FormRequest(self.targetUrl, method='POST', headers=self.header, priority=priority, callback=self.parse_index, meta={'page': page, 'priority': priority},
                              formdata=requests_data,
                              # headers={'Content-Type': 'application/json'},
                              # dont_filter=True
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
            dataItems = datas.xpath('//div[@id="listCon"]/ul/li')
            for dataItem in dataItems:
                title = dataItem.xpath('a/text()').extract_first()
                url = 'http://ggzy.lasa.gov.cn' + dataItem.xpath('a/@href').extract_first()
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
        try:
            data = Selector(text=response.body.decode('utf-8'))
            items = str(data.xpath('string(.)').extract()[0]).replace('\xa0', '').replace('\u3000', '')
            WJBT_1 = ''
            WZLY_2 = ''
            GXSJ_3 = ''
            ZDBH_4 = ''
            ZDZL_5 = ''
            MJ_6 = ''
            TDYT_7 = ''
            CRNX_8 = ''
            RJL_9 = ''
            LDL_10 = ''
            JZMD_11 = ''
            JZXG_12 = ''
            JMBZJ_13 = ''
            QSJ_14 = ''
            ZJFD_15 = ''
            CRR_16 = ''
            QTSM_17 = ''

            # TODO 共有字段
            # 文件标题
            WJBT_1 = response.meta.get('title')
            # 文章来源
            WZLY_2 = data.xpath('//div[@class="news_time"]/span[1]/text()').extract_first().replace('文章来自：', '')
            # 更新时间
            GXSJ_3 = data.xpath('//div[@class="news_time"]/span[2]/text()').extract_first().replace('更新时间：', '')
            # 备注
            QTSM_17 = reFunction(f'备注(?:[\s]*)([{self.reStr}]*)\s', items)

            # TODO //table[@border="1"]   //table[@border="0"]
            # table 解析
            if '宗地编号' not in items and '配套建筑规划用地' not in items:
                if data.xpath('//table[@border="0"]') and '主要规划指标' not in items:
                    soup = BeautifulSoup(response.body.decode('utf-8'))
                    table = soup.find('table')
                    htmlTable = htmlTableTransformer()
                    tdData = htmlTable.tableTrTdRegulation(table)
                    # 宗地编号
                    ZDBH_4 = tdData.get('地块编号')
                    # 宗地坐落
                    ZDZL_5 = tdData.get('土地位置')
                    # 面积
                    MJ_6 = tdData.get('土地面积(平方米)')
                    # 土地用途
                    TDYT_7 = tdData.get('土地用途')
                    # 出让年限
                    CRNX_8 = tdData.get('出让年限（年）') if tdData.get('出让年限（年）') else tdData.get('出让年限')
                    # 容积率
                    RJL_9 = tdData.get('容积率') if tdData.get('容积率') else tdData.get('容积率（不大于）')
                    # 绿地率
                    LDL_10 = tdData.get('绿地率') if tdData.get('绿地率') else tdData.get('绿地率（不小于）')
                    # 建筑密度
                    JZMD_11 = tdData.get('建筑密度')
                    # 建筑限高
                    JZXG_12 = tdData.get('建筑高度')
                    # 竞买保证金
                    JMBZJ_13 = tdData.get('竞买保证金（万元）') if tdData.get('竞买保证金（万元）') else tdData.get('竞买保证金（元）')
                    # 起始价
                    QSJ_14 = tdData.get('起始价（万元）')
                    # 增价幅度
                    ZJFD_15 = tdData.get('增价幅度(万元)') if tdData.get('增价幅度(万元)') else tdData.get('加价幅度')
                if '规划指标要求' in items:
                    soup = BeautifulSoup(response.body.decode('utf-8'))
                    table = soup.find('table')
                    tdReplace_ = table.tbody.find_all('tr')[0].find('td', colspan='4')
                    tdReplace = tdReplace_ if tdReplace_ else table.tbody.find_all('tr')[0].find('td', colspan='3')
                    try:
                        number = table.tbody.find_all('tr')[0].index(tdReplace)
                        tdList = table.tbody.find_all('tr')[1].find_all('td')
                        for _ in range(1, len(tdList) + 1):
                            table.tbody.find_all('tr')[0].insert(number + _, tdList[_ - 1])
                        tdReplace.extract()
                        table.tbody.find_all('tr')[1].extract()
                    except:
                        pass
                    htmlTable = htmlTableTransformer()
                    tdData = htmlTable.tableTrTdRegulation(table)
                    # 宗地编号
                    ZDBH_4 = tdData.get('地块编号')
                    # 宗地坐落
                    ZDZL_5_ = tdData.get('土地位置') if tdData.get('土地位置') else tdData.get('地块位置/名称')
                    ZDZL_5 = ZDZL_5_.replace(reFunction(f'备注(?:[\s]*)([{self.reStr}]*)\s', reFunction('一([\s\S]*)二', items)), '')
                    # 面积
                    MJ_6 = tdData.get('土地面积(m2)') if tdData.get('土地面积(m2)') else tdData.get('土地面积(平方米)')
                    # 土地用途
                    TDYT_7 = tdData.get('土地用途') if tdData.get('土地用途') else tdData.get('规划地性质')
                    # 出让年限
                    CRNX_8_ = tdData.get(r'出让\u3000年限') if tdData.get(r'出让\u3000年限') else tdData.get('出让年限')
                    CRNX_8 = CRNX_8_ if CRNX_8_ else tdData.get('出让年限（年）')
                    # 容积率
                    RJL_9 = tdData.get('容积率') if tdData.get('容积率') else tdData.get('容积率（不大于）')
                    # 绿地率
                    LDL_10_ = tdData.get('绿地率') if tdData.get('绿地率') else tdData.get('绿地率（%）')
                    LDL_10 = LDL_10_ if LDL_10_ else tdData.get('绿地率（不小于）')
                    # 建筑密度
                    JZMD_11_ = tdData.get('建筑\u3000密度') if tdData.get('建筑\u3000密度') else tdData.get('建筑密度')
                    JZMD_11__ = JZMD_11_ if JZMD_11_ else tdData.get('建筑密度（%）')
                    JZMD_11 = JZMD_11__ if JZMD_11__ else tdData.get('建筑\u3000密度（不大于）')
                    # 建筑限高
                    JZXG_12_ = tdData.get('建筑限高') if tdData.get('建筑限高') else tdData.get('建筑高度（m）')
                    JZXG_12__ = JZXG_12_ if JZXG_12_ else tdData.get('建筑高度')
                    JZXG_12 = JZXG_12__ if JZXG_12__ else tdData.get('建筑限高（不高于）')
                    # 竞买保证金
                    JMBZJ_13 = tdData.get('竞买保证金(元)') if tdData.get('竞买保证金(元)') else tdData.get('竞买保证金（万元）')
                    # 起始价
                    QSJ_14_ = tdData.get('起始价(元)') if tdData.get('起始价(元)') else tdData.get('挂牌出让起始价（元）')
                    QSJ_14 = QSJ_14_ if QSJ_14_ else tdData.get('起始价（万元）')
                    # 增价幅度
                    ZJFD_15 = tdData.get('增价幅度(万元)') if tdData.get('增价幅度(万元)') else tdData.get('加价幅度')
                    if ZJFD_15 == '' and QSJ_14 == '' and JMBZJ_13 == '':
                        soup = BeautifulSoup(response.body.decode('utf-8'))
                        table = soup.find('table')
                        tdReplace0 = table.tbody.find_all('tr')[0].find_all('td')[-1]  # 第一个
                        tdReplace1 = table.tbody.find_all('tr')[1].find_all('td')[-1]  # 第二个
                        number0 = table.tbody.find_all('tr')[0].index(tdReplace0)  # 第一个index
                        number1 = table.tbody.find_all('tr')[1].index(tdReplace1)  # 第二个index
                        tdList2 = table.tbody.find_all('tr')[2].find_all('td')  # 第二个
                        tdList3 = table.tbody.find_all('tr')[3].find_all('td')  # 第四个
                        for _ in range(1, len(tdList2) + 1):
                            table.tbody.find_all('tr')[0].insert(number0 + _, tdList2[_ - 1])
                        for _ in range(1, len(tdList3) + 1):
                            table.tbody.find_all('tr')[1].insert(number1 + _, tdList3[_ - 1])
                        table.tbody.find_all('tr')[2].extract()

                        htmlTable = htmlTableTransformer()
                        tdDataCopy = htmlTable.tableTrTdRegulation(table)
                        # 竞买保证金
                        JMBZJ_13 = tdDataCopy.get('竞买保证金(元)') if tdDataCopy.get('竞买保证金(元)') else tdDataCopy.get('竞买保证金（万元）')
                        # 起始价
                        QSJ_14_ = tdDataCopy.get('起始价(元)') if tdDataCopy.get('起始价(元)') else tdDataCopy.get('挂牌出让起始价（元）')
                        QSJ_14 = QSJ_14_ if QSJ_14_ else tdDataCopy.get('起始价（万元）')
                        # 增价幅度
                        ZJFD_15 = tdDataCopy.get('增价幅度(万元)') if tdDataCopy.get('增价幅度(万元)') else tdDataCopy.get('加价幅度')
                    # 出让人
                if '标的序号' in items:
                    soup = BeautifulSoup(response.body.decode('utf-8'))
                    table = soup.find('table', border='0')
                    htmlTable = htmlTableTransformer()
                    tdData = htmlTable.table_tr_td(table)
                    # 宗地坐落
                    ZDZL_5 = tdData.get('标的位置')
                    # 面积
                    MJ_6 = tdData.get('土地面积') if tdData.get('土地面积') else tdData.get('土地面积(平方米)')
                    # 起始价
                    QSJ_14_ = tdData.get('起始价(元)') if tdData.get('起始价(元)') else tdData.get('拍卖参考价（万元）')
                    QSJ_14 = QSJ_14_ if QSJ_14_ else tdData.get('起始价（万元）')
                    # 出让年限
                    CRNX_8 = tdData.get('土地性质（年限）') if tdData.get('土地性质（年限）') else tdData.get('出让年限（年）')
            else:
                if '宗地编号' in items:
                    for item in ['宗地编号' + _ for _ in re.findall('一([\s\S]*)二', items)[0].split('宗地编号')[1:]]:
                        # 宗地编号
                        ZDBH_4 += '|' +  reFunction(f'宗地编号：(?:[\s]*)([{self.reStr}]*)\s', item)
                        # 宗地坐落
                        ZDZL_5 += '|' +  reFunction(f'宗地坐落：(?:[\s]*)([{self.reStr}]*)\s', item)
                        # 面积
                        MJ_6 += '|' +  reFunction(f'宗地面积：(?:[\s]*)([{self.reStr}]*)\s', item)

                        # 出让年限
                        CRNX_8 += '|' +  reFunction(f'出让年限：(?:[\s]*)([{self.reStr}]*)\s', item)
                        # 容积率
                        RJL_9 += '|' +  reFunction(f'容积率：(?:[\s]*)([{self.reStr}]*)\s', item)
                        # 绿地率
                        LDL_10 += '|' +  reFunction(f'绿地率\(%\)：(?:[\s]*)([{self.reStr}]*)\s', item)
                        # 建筑密度
                        JZMD_11 += '|' +  reFunction(f'建筑密度\(%\)：(?:[\s]*)([{self.reStr}]*)\s', item)
                        # 建筑限高
                        JZXG_12 += '|' +  reFunction(f'建筑限高\(米\)：(?:[\s]*)([{self.reStr}]*)\s', item)
                        # 竞买保证金
                        JMBZJ_13 += '|' +  reFunction(f'保证金：(?:[\s]*)([{self.reStr}]*)\s', item)
                        # 起始价
                        QSJ_14 += '|' +  reFunction(f'起始价：(?:[\s]*)([{self.reStr}]*)\s', item)
                        # 增价幅度
                        ZJFD_15 += '|' +  reFunction(f'加价幅度：(?:[\s]*)([{self.reStr}]*)\s', item)
                        # 出让人
                        # CRR_16 += '|' +  reFunction(f'宗地编号：(?:[\s]*)([{self.reStr}]*)\s', item)
                        # 其他说明
                        QTSM_17 += '|' + reFunction(f'备注：(?:[\s]*)([{self.reStr}]*)\s', item)
                if '配套建筑规划用地' in items:
                    soup = BeautifulSoup(response.body.decode('utf-8'))
                    table = soup.find('table')
                    tdReplace0 = table.tbody.find_all('tr')[0].find_all('td')[-1]  # 第一个
                    tdReplace1 = table.tbody.find_all('tr')[1].find_all('td')[-1]  # 第二个
                    number0 = table.tbody.find_all('tr')[0].index(tdReplace0)  # 第一个index
                    number1 = table.tbody.find_all('tr')[1].index(tdReplace1)  # 第二个index
                    tdList2 = table.tbody.find_all('tr')[2].find_all('td')  # 第二个
                    tdList3 = table.tbody.find_all('tr')[3].find_all('td')  # 第四个
                    for _ in range(1, len(tdList2) + 1):
                        table.tbody.find_all('tr')[0].insert(number0 + _, tdList2[_ - 1])
                    for _ in range(1, len(tdList3) + 1):
                        table.tbody.find_all('tr')[1].insert(number1 + _, tdList3[_ - 1])
                    table.tbody.find_all('tr')[2].extract()
                    htmlTable = htmlTableTransformer()
                    tdData = htmlTable.tableTrTdRegulation(table)
                    # 宗地编号
                    ZDBH_4 = tdData.get('地块编号')
                    # 宗地坐落
                    ZDZL_5 = tdData.get('地块位置/名称')
                    # 面积
                    MJ_6 = tdData.get('配套设施出让面积（m2）') if tdData.get('配套设施出让面积（m2）') else tdData.get('土地面积(平方米)')
                    # 土地用途
                    TDYT_7 = tdData.get('配套建筑规划用地性质')
                    # 出让年限
                    CRNX_8 = tdData.get('出让年限') if tdData.get('出让年限') else tdData.get('出让年限（年）')
                    # 容积率
                    RJL_9 = tdData.get('容积率') if tdData.get('容积率') else tdData.get('容积率（不大于）')
                    # 绿地率
                    LDL_10 = tdData.get('公园整体绿地率（%）') if tdData.get('公园整体绿地率（%）') else tdData.get('绿地率（不小于）')
                    # 建筑密度
                    JZMD_11 = tdData.get('公园整体建筑密度（%）')
                    # 建筑限高
                    JZXG_12_ = tdData.get('建筑限高') if tdData.get('建筑限高') else tdData.get('建筑高度（m）')
                    JZXG_12 = JZXG_12_ if JZXG_12_ else tdData.get('建筑高度')
                    # 竞买保证金
                    JMBZJ_13 = tdData.get('竞买保证金（元）') if tdData.get('竞买保证金（元）') else tdData.get('竞买保证金（万元）')
                    # 起始价
                    QSJ_14_ = tdData.get('起始价(元)') if tdData.get('起始价(元)') else tdData.get('配套设施用地挂牌出让起始价（元）')
                    QSJ_14 = QSJ_14_ if QSJ_14_ else tdData.get('起始价（万元）')
                    # 增价幅度
                    ZJFD_15 = tdData.get('增价幅度(万元)') if tdData.get('增价幅度(万元)') else tdData.get('加价幅度')

            # 爬取时间
            crawlingTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            # 爬取地址url
            url = response.url
            # 唯一标识
            md5Mark = encrypt_md5(url)

            # 存储数据
            csvFile = [
                WJBT_1,
                WZLY_2,
                GXSJ_3,
                ZDBH_4,
                ZDZL_5,
                MJ_6,
                TDYT_7,
                CRNX_8,
                RJL_9,
                LDL_10,
                JZMD_11,
                JZXG_12,
                JMBZJ_13,
                QSJ_14,
                ZJFD_15, QTSM_17,
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
        except Exception as e:
            print(response.url)
            self.log(f'详情页数据解析失败, 错误: {e}\n{traceback.format_exc()}', level=logging.ERROR)
