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


class chongQingDealAnnouncementSpider(CrawlSpider):
    name = 'chongQingDealAnnouncement'
    # 配置更换cookies时间
    COOKIES_SWITCH_TIME = datetime.datetime.now()

    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super(chongQingDealAnnouncementSpider, cls).__new__(cls)
            cls.pathPage = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),'Logs/chongQingDealAnnouncementPage.txt')
            cls.pathDetail = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))), 'data/重庆公共资源交易_交易公告_重庆.csv')
            cls.filePage = open(cls.pathPage, 'w+')
            if os.path.exists(cls.pathDetail):
                cls.fileDetail = open(cls.pathDetail, 'a+')
            else:
                cls.fileDetail = open(cls.pathDetail, 'a+')
                with open(cls.pathDetail, 'a+') as fp:
                    fp.write("""文件标题,信息时间,土地位置,用途,土地面积(m),总计容建筑面积(m2),最大建筑密度,绿地率,出让价款起始价(万元),投标竞买保证金(万元),编号,产业类别,可建面积(m2)或容积率,投资强度(万元/公顷),产出要求(万元/公顷),备注,获取出让文件时间,获取出让文件地点,报名时间,报名地点,保证金截止时间,确认竞买资格时间,联系地址,联系电话,联系人,爬取时间,爬取地址url,唯一标识,\n""")
        return cls.instance

    def __init__(self):
        super(chongQingDealAnnouncementSpider, self).__init__()
        dispatcher.connect(self.CloseSpider, signals.spider_closed)
        self.targetUrl = 'https://www.cqggzy.com/interface/rest/inteligentSearch/getFullTextData'
        self.header = {'User-Agent': random.choice(agent_list)}
        self.session = requests.session()        # pn  是页数 n * 18
        self.data = {
    "token":"",
    "pn":0,
    "rn":18,
    "sdt":"",
    "edt":"",
    "wd":" ",
    "inc_wd":"",
    "exc_wd":"",
    "fields":"title",
    "cnum":"001",
    "sort":'{"istop":0,"ordernum":0,"webdate":0}',
    "ssort":"title",
    "cl":200,
    "terminal":"",
    "condition":[
        {
            "fieldName":"categorynum",
            "equal":"014004001",
            "notEqual": None,
            "equalList": None,
            "notEqualList": None,
            "isLike": True,
            "likeType":2
        }
    ],
    "time": None,
    "highlights":"title",
    "statistics": None,
    "unionCondition": None,
    "accuracy":"",
    "noParticiple":"0",
    "searchRange": None,
    "isBusiness":"1"
}

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

        :return:
        '''
        try:
            try:
                pageStart = int(self.filePage.read()) if self.filePage.read() else 0
            except:
                pageStart = 0
                self.log(f'获取历史页错误: {traceback.format_exc()}', level=logging.ERROR)
            if pageStart != 90:
                for page in range(0, 89):
                    self.data['pn'] = page * 18
                    requests_data = json.dumps(self.data)
                    priority = 89 - int(page)
                    with open(self.pathPage, 'w+') as fp:
                        fp.write(str(page))
                    yield Request(self.targetUrl, method='POST', headers=self.header, priority=priority, callback=self.parse_index,
                                  meta={'page': page, 'priority': priority},
                                  body=requests_data,
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
            datas = json.loads(response.body)
            dataItems = datas.get('result').get('records')
            page = response.meta.get('page')
            for dataItem in dataItems:
                url = 'https://www.baidu.com'
                yield Request(url, method='GET', callback=self.parse_detail,
                                  meta={
                                      'url': url,
                                      'page': page,
                                      'categorynum': dataItem.get('categorynum'),
                                      'infoid': dataItem.get('infoid'),
                                      # 'userDefind': True
                                  },
                                  # body=requests_data, headers={'Content-Type': 'application/json'}
                                  dont_filter=True,
                                  )
        except Exception as e:
            self.log(f'列表页解析失败{page}, 错误: {e}\n{traceback.format_exc()}', level=logging.ERROR)

    def parse_detail(self, response):
        try:
            # 数据获取不全
            categorynum = response.meta.get('categorynum')
            infoid = response.meta.get('infoid')
            targetUrl = "https://www.cqggzy.com/tiaozhuan.html?infoid=" + infoid + "&categorynum=" + categorynum
            results = ''
            for _ in range(5):
                try:
                    self.session.get(targetUrl, headers=self.header, allow_redirects=False, timeout=60)
                    redirectUrl = 'https://www.cqggzy.com/EpointWebBuilderService/getInfoListAndCategoryList.action?cmd=pageRedirect'
                    data = {'categorynum': categorynum, 'infoid': infoid}
                    response_ = self.session.post(redirectUrl, headers=self.header, data=data, allow_redirects=False, timeout=60)
                    url = 'https://www.cqggzy.com' + response_.json().get('custom') if 'http' not in response_.json().get('custom') else response_.json().get('custom')
                    results = self.session.get(url, headers=self.header, allow_redirects=False, timeout=60)
                    break
                except Exception as e:
                    pass

            data = Selector(text=results.content.decode('utf-8'))
            items = str(data.xpath('string(.)').extract()[0]).replace('\xa0', '').replace('\u3000', '')
            WJBT_1 = ''
            XXSJ_2 = ''
            TDWZ_3 = ''
            YT_4 = ''
            TDMJ_5 = ''
            ZJRJZMJ_6 = ''
            ZDJZMD_7 = ''
            LDL_9 = ''
            CRJKQSJ_8 = ''
            JMBZJ_11 = ''
            BH_12 = ''
            CYNB_13 = ''
            KJMJ_14 = ''
            TZQD_15 = ''
            CCYQ_16 = ''
            BZ_17 = ''
            HQCRWJSJ_18 = ''
            HQCRWJDD_19 = ''
            BMSJ_20 = ''
            BMDD_21 = ''
            BZJJZSJ_22 = ''
            QRJMZGSJ_23 = ''
            LXDZ_24 = ''
            LXDH_25 = ''
            LXR_26 = ''

            # 共有字段
            # 文件标题
            WJBT_1 = data.xpath('//*[@class="article-title"]/text()').extract_first()
            # 信息时间
            XXSJ_2 = reFunction('(\d{4}-\d{1,2}-\d{1,2})',data.xpath('//*[@class="info-source"]/text()[1]').extract_first())
            if (('总计容建筑面' in items and '序号' in items) or data.xpath('//table')) and '宗地编号' not in items:
                # TODO
                soup = BeautifulSoup(results.content.decode('utf-8'))
                tableMso = soup.find('table', 'MsoTableGrid')
                table = soup.find('table')
                htmlTable = htmlTableTransformer()
                try:
                    if tableMso:
                        tdData = htmlTable.table_tr_td(table)
                    else:
                        tdData = htmlTable.tableTrTdRegulation(table)
                    sourceTdData = tdData
                    for key, value in tdData.items():
                        tdData[key] = value.replace(str(key), '') if value else value
                    # 土地位置   //table[@class="MsoNormalTable"]
                    TDWZ_3 = tdData.get('土地位置')
                    # 用途
                    YT_4 = tdData.get('土地用途') if tdData.get('土地用途') else tdData.get('用途')
                    # 土地面积(m)
                    TDMJ_5 = tdData.get('土地面积(m)') if tdData.get('土地面积 (m)') else tdData.get('土地面积 (㎡)')
                    # 总计容建筑面积(m2)
                    ZJRJZMJ_6 = tdData.get('总计容建筑面积（㎡）')
                    # 最大建筑密度
                    ZDJZMD_7 = tdData.get('最大建筑密度')
                    # 绿地率
                    LDL_9 = tdData.get('绿地率')
                    # TODO 正则匹配
                    if not ZDJZMD_7 and not LDL_9:
                        # sourceTdData
                        for value in sourceTdData.values():
                            if '最大建筑密度' in value:
                                ZDJZMD_7 = value.replace('最大建筑密度', '')
                            if '绿地率' in value:
                                LDL_9_  = value.replace('绿地率', '')
                                LDL_9 = LDL_9_ if len(LDL_9_)<10 else reFunction(f'绿地率[：]*\s*([（）\w\.:： \(\)〔〕≤≥\-\/\%,、\.﹪]*)[；。，]?',value)
                            if '总计容建筑面积' in value:
                                LDL_9 = value.replace('总计容建筑面积（㎡）', '')
                    # 出让价款起始价(万元)
                    CRJKQSJ_8 = tdData.get('出让价款起始价（万元）')
                    # 投标竞买保证金(万元)  保证金（万元）
                    JMBZJ_11 = tdData.get('保证金（万元）') if tdData.get('保证金（万元）') else tdData.get('投标、竞买保证金（万元）')
                    # 编号
                    BH_12 = tdData.get('编号')
                    # 产业类别
                    CYNB_13 = tdData.get('产业类别')
                    # 可建面积(m2)或容积率
                    KJMJ_14 = tdData.get('可建面积(㎡）或容积率')
                    # 投资强度(万元 / 公顷)
                    TZQD_15 = tdData.get('投资强度（万元/公顷）')
                    # 产出要求(万元 / 公顷)
                    CCYQ_16 = tdData.get('产出要求（万元/公顷）')
                    # 备注  其他需要说明的宗地情况：
                    BZ_17_ = tdData.get('序号').split('备注：')[-1] if '备注' in tdData.get('序号') else tdData.get('备注：')
                    other = tdData.get('序号').split('其他需要说明的宗地情况：')[-1] if '其他需要说明的宗地情况：' in tdData.get(
                        '序号') else tdData.get('其他需要说明的宗地情况：')
                    BZ_17 = other if not BZ_17_ else BZ_17_
                    # 获取出让文件时间
                    HQCRWJSJ_18 = reFunction('竞买申请人可在([\w ：\.\-\s\/\%,、]*)。', reFunction('二、([\s\S]*)三、', items))
                    # 获取出让文件地点
                    HQCRWJDD_19 = reFunction('网址：([\w :\.\-\s\/\%,、]*)(?:[\）\s]*)', reFunction('二、([\s\S]*)三、', items))
                    # 报名时间
                    BMSJ_20 = reFunction('竞买申请人可在([\w \.：\-\s\/\%,、]*)\(报名时间\)', reFunction('三、([\s\S]*)四、', items))
                    # 保证金截止时间
                    BZJJZSJ_22 = reFunction('竞买保证金到账截止时间为([\w \.：\-\s\/\%,、]*)。', reFunction('三、([\s\S]*)四、', items))
                    # 确认竞买资格时间
                    QRJMZGSJ_23 = BZJJZSJ_22
                    # 联系地址
                    LXDZ_24 = '|'.join(
                        re.findall('联系地址：([\w 、\.：\-\/\%,、（)]*)(?:[，\n])', reFunction('七、([\s\S]*)', items)))
                    # 联系电话
                    LXDH_25 = '|'.join(
                        re.findall('[联系]*电话[：:]([\w 、\.：\-\/\%,、（)]*)(?:[\n。])', reFunction('七、([\s\S]*)', items)))
                    # 联系人
                    LXR_26 = '|'.join(re.findall('联系人[：:]([\w 、\.：\-\/\%,、（)]*)(?:[ ，]*)', reFunction('七、([\s\S]*)', items)))
                except:
                    for item in ['宗地编号' + _ for _ in re.findall('一、([\s\S]*)二、', items)[0].split('宗地编号')[1:]]:
                        # 土地位置
                        TDWZ_3 += '|' + reFunction('宗地坐落：([\w :\.\-\s\/\%,、]*)(?:\s)', item)
                        # 用途
                        YT_4_1 = reFunction('主要用途：(?:[\s]*)([\w :\.\- \/\%,、]*)(?:\s)', item)
                        YT_4_2 = reFunction('土地用途[：](?:[\s]*)([\w :：\.\- \/\%,、]*)(?:\s)', item)
                        YT_4 += '|' + YT_4_1 + YT_4_2
                        # 土地面积(m)
                        TDMJ_5 += '|' + reFunction('宗地总面积：(?:[\s]*)([\w :\.\- \/\%,、㎡]*)(?:\s)', item) if reFunction(
                            '宗地总面积：(?:[\s]*)([\w :\.\- \/\%,、㎡]*)(?:\s)', item) else '|' + reFunction(
                            '宗地面积：(?:[\s]*)([\w :\.\- \/\%,、㎡]*)(?:\s)', item)
                        # 最大建筑密度
                        ZDJZMD_7 += '|' + reFunction('建筑密度\(%\)：([\w :\.\-\s\/\%,、]*)(?:\s)', item)
                        # 绿地率
                        LDL_9 += '|' + reFunction('绿地率\(%\)[：]([\w :\.\-\s\/\%,、≤；≥]*)(?:\s)', item)
                        # 编号
                        BH_12 += '|' + reFunction('宗地编号[：]([\w :\.\-\s\/\%,、]*)(?:\s)', item)
                        # 投资强度(万元 / 公顷)
                        TZQD_15 += '|' + reFunction('投资强度[：]([\w :\.\-\s\/\%,、]*)(?:\s)', item)
                        # 备注
                        BZ_17 += '|' + reFunction('备注：([\s\S]*)', item)
                    # TODO 获取出让文件时间
                    HQCRWJSJ_18 = reFunction('申请人可于([\w ：\.\-\s\/\%,、]*)到', reFunction('四、([\s\S]*)五、', items))
                    # 获取出让文件地点
                    HQCRWJDD_19 = reFunction('申请人可于(?:[\w ：\.\-\s\/\%,、]*)到([\w ：\.\-\s\/\%,、]*)获取',
                                             reFunction('四、([\s\S]*)五、', items))
                    # 报名时间
                    BMSJ_20 = reFunction('申请人可于([\w \.：\-\s\/\%,、]*)到', reFunction('五、([\s\S]*)六、', items))
                    # 保证金截止时间
                    BZJJZSJ_22 = reFunction('竞买保证金的截止时间为([\w \d\.：\-\s\/\%,、 ]*)。', reFunction('五、([\s\S]*)六、', items))
                    # 确认竞买资格时间
                    QRJMZGSJ_23 = BZJJZSJ_22
                    # 联系地址
                    LXDZ_24 = '|'.join(re.findall('联系地址：([\w 、\.：\-\/\%,、（)]*)(?:[，\n])', reFunction('八|七、([\s\S]*)', items)))
                    # 联系电话
                    LXDH_25 = '|'.join(re.findall('[联系]*电话[：:]([\w 、\.：\-\/\%,、（)]*)(?:[\n。])', reFunction('八|七、([\s\S]*)', items)))
                    # 联系人
                    LXR_26 = '|'.join(re.findall('联 系 人[：:]([ \w]*)(?:[\n]*)', reFunction('八|七、([\s\S]*)', items)))
            else:
                for item in ['宗地编号' + _ for _ in re.findall('一、([\s\S]*)二、', items)[0].split('宗地编号')[1:]]:
                    # 土地位置
                    TDWZ_3 += '|' + reFunction('宗地坐落：([\w :\.\-\s\/\%,、]*)(?:\s)', item)
                    # 用途
                    YT_4_1 = reFunction('主要用途：(?:[\s]*)([\w :\.\- \/\%,、]*)(?:\s)', item)
                    YT_4_2 = reFunction('土地用途[：](?:[\s]*)([\w :：\.\- \/\%,、]*)(?:\s)', item)
                    YT_4 += '|' + YT_4_1 + YT_4_2
                    # 土地面积(m)
                    TDMJ_5 += '|' + reFunction('宗地总面积：(?:[\s]*)([\w :\.\- \/\%,、㎡]*)(?:\s)', item) if reFunction('宗地总面积：(?:[\s]*)([\w :\.\- \/\%,、㎡]*)(?:\s)', item) else '|' + reFunction('宗地面积：(?:[\s]*)([\w :\.\- \/\%,、㎡]*)(?:\s)', item)
                    # 最大建筑密度
                    ZDJZMD_7 += '|' + reFunction('建筑密度:([\w :\.\-\s\/\%,、≦；≥]*)(?:\s)', item)
                    # 绿地率
                    LDL_9 += '|' + reFunction('绿地率\(%\)[：]([\w :\.\-\s\/\%,、≤；≥]*)(?:\s)', item)
                    # 编号
                    BH_12 += '|' + reFunction('宗地编号[：]([\w :\.\-\s\/\%,、]*)(?:\s)', item)
                    # 投资强度(万元 / 公顷)
                    TZQD_15 += '|' + reFunction('投资强度[：]([\w :\.\-\s\/\%,、]*)(?:\s)', item)
                    # 备注
                    BZ_17 += '|' + reFunction('备注：([\s\S]*)', item)
                # TODO 获取出让文件时间
                HQCRWJSJ_18 = reFunction('申请人可于([\w ：\.\-\s\/\%,、]*)到', reFunction('四、([\s\S]*)五、', items))
                # 获取出让文件地点
                HQCRWJDD_19 = reFunction('申请人可于(?:[\w ：\.\-\s\/\%,、]*)到([\w ：\.\-\s\/\%,、]*)获取', reFunction('四、([\s\S]*)五、', items))
                # 报名时间
                BMSJ_20 = reFunction('申请人可于([\w \.：\-\s\/\%,、]*)到', reFunction('五、([\s\S]*)六、', items))
                # 保证金截止时间
                BZJJZSJ_22 = reFunction('竞买保证金的截止时间为([\w \d\.：\-\s\/\%,、 ]*)。', reFunction('五、([\s\S]*)六、', items))
                # 确认竞买资格时间
                QRJMZGSJ_23 = BZJJZSJ_22
                # 联系地址
                LXDZ_24 = '|'.join(re.findall('联系地址：([\w 、\.：\-\/\%,、（)]*)(?:[，\n])', reFunction('八|七、([\s\S]*)', items)))
                # 联系电话
                LXDH_25 = '|'.join(re.findall('[联系]*电话[：:]([\w 、\.：\-\/\%,、（)]*)(?:[\n。])', reFunction('八|七、([\s\S]*)', items)))
                # 联系人
                LXR_26 = '|'.join(re.findall('联 系 人[：:]([ \w]*)(?:[\n]*)', reFunction('八|七、([\s\S]*)', items)))
            # 爬取时间
            crawlingTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            # 爬取地址url
            url = url if url else response.url
            # 唯一标识
            md5Mark = encrypt_md5(url)

            # 存储数据
            csvFile = [WJBT_1,XXSJ_2,TDWZ_3,YT_4,TDMJ_5,ZJRJZMJ_6,ZDJZMD_7,LDL_9,CRJKQSJ_8,JMBZJ_11,BH_12,CYNB_13,KJMJ_14,TZQD_15,CCYQ_16,BZ_17,HQCRWJSJ_18,HQCRWJDD_19,BMSJ_20,BMDD_21,BZJJZSJ_22,QRJMZGSJ_23,LXDZ_24,LXDH_25,LXR_26,
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
                    self.log(f'{getVariableName(_).pop()}字段解析出错, 错误: {e}\n{traceback.format_exc()}', level=logging.ERROR)
            with open(self.pathDetail, 'a+') as fp:
                fp.write(results)
                fp.write('\n')
            yield
        except Exception as e:
            self.log(f'详情页数据解析失败, 错误: {e}\n{traceback.format_exc()}', level=logging.ERROR)
