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
from SpiderTools.Tool import reFunction
from SpiderTools.tableAnalysis import htmlTableTransformer


class chongQingdealFormulaSpider(CrawlSpider):
    name = 'chongQingdealFormula'
    # 配置更换cookies时间
    COOKIES_SWITCH_TIME = datetime.datetime.now()

    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super(chongQingdealFormulaSpider, cls).__new__(cls)
            cls.pathPage = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),'Logs/chongQingdealFormulaPage.txt')
            pathDetail = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))), 'data/重庆公共资源交易_成交公告_重庆.csv')
            cls.filePage = open(cls.pathPage, 'w+')
            if os.path.exists(pathDetail):
                cls.fileDetail = open(pathDetail, 'a+')
            else:
                cls.fileDetail = open(pathDetail, 'a+')
                cls.fileDetail.write("""文件标题,信息时间,正文标题,公告序号,宗地编号/编号,地块位置,土地用途/用途,土地面积(平方米)/土地面积(m2)/出让面积(m),容积率,建筑密度(%),绿地率(%),保证金(万元),底价(万元),计容建筑面积(m2),出让方式,出让年限,成交价(万元)/成交价,受让单位,土地使用条件,交易时间,成交人,备注,联系单位,联系地址,联系电话,公示期,爬取时间,爬取地址url,唯一标识,\n""")
        return cls.instance

    def __init__(self):
        super(chongQingdealFormulaSpider, self).__init__()
        dispatcher.connect(self.CloseSpider, signals.spider_closed)
        self.targetUrl = 'https://www.cqggzy.com/interface/rest/inteligentSearch/getFullTextData'
        self.header = {'User-Agent': random.choice(agent_list)}
        self.session = requests.session()
        self.reStr = '[（）\w\.:： 。\(\)〔〕≤；≥《》\-\/\%,、\.]*'
        # self.session = HTMLSession()
        # pn  是页数 n * 18
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
            "equal":"014004004",
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
        先拿到总页数, 按照优先级爬取, 并每十个请求换一个sessionID
        '''
        try:
            try:
                pageStart = int(self.filePage.read()) if self.filePage.read() else 0
            except:
                pageStart = 0
                self.log(f'获取历史页错误: {traceback.format_exc()}', level=logging.ERROR)
            if pageStart != 105:
                for page in range(pageStart, 105):
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
            GGXH_31 = ''
            ZDBH_32 = ''
            DKWZ_33 = ''
            TDYT_34 = ''
            TDMJ_35 = ''
            RJL_36 = ''
            JZMD_37 = ''
            LDL_38 = ''
            BZJ_39 = ''
            DJ_40 = ''
            JRZJMJ_41 = ''
            CRFS_42 = ''
            CRNX_43 = ''
            CJJ_44 = ''
            SRDW_45 = ''
            TDSYTJ_46 = ''
            JYSJ_47 = ''
            CJR_48 = ''
            BZ_49 = ''
            LXDW_50 = ''
            LXDZ_51 = ''
            LXDH_52 = ''
            GSQ_53 = ''

            # 共有字段
            # 文件标题
            WJBT_27 = data.xpath('//*[@class="article-title"]/text()').extract_first()
            # 信息时间
            XXSJ_28 = reFunction('(\d{4}-\d{1,2}-\d{1,2})',data.xpath('//*[@class="info-source"]/text()[1]').extract_first())
            # TODO
            # 正文标题
            ZWBT_29 = WJBT_27
            soup = BeautifulSoup(results.content.decode('utf-8'))
            table = soup.find('table')

            if '土地使用条件' in items or '宗地编号' in items or '公告序号' in items:
                # TODO 正则匹配的页面
                # 公告序号
                GGXH_31 = '|'.join(re.findall('公告序号(?:[\s]*)([（）\w\.：\-\/\%,、]*)(?:\n)',  items))
                # 宗地编号 / 编号
                ZDBH_32_ = '|'.join(re.findall(f'[宗地](?:[\s]*)编号(?:[\s]*)([{self.reStr}]*)(?:\s)',  items))
                ZDBH_32 = ZDBH_32_.replace('：', '') if ZDBH_32_ else ZDBH_32_
                # 地块位置
                DKWZ_33_ = '|'.join(re.findall(f'地块位置(?:[\s]*)({self.reStr})(?:\n)',  items))
                DKWZ_33 = DKWZ_33_ if DKWZ_33_ else '|'.join(re.findall('土地(?:[\s]*)坐落(?:[\s]*)([（）\w\.:： ≤；≥\-\/\%,、\.]*)(?:\n)',  items))
                # 土地用途 / 用途
                TDYT_34 = '|'.join(re.findall('[土地]?用途(?:[\s]*)([（）\w\.：\-\/\%,、]*)(?:\n)',  items))
                # 土地面积(平方米) / 土地面积(m2) / 出让面积(m)
                TDMJ_35_ = '|'.join(re.findall('土地面积\(m2\)(?:[\s]*)([（）\w\.：\-\/\%,、\.]*)(?:\n)',  items))
                TDMJ_35 = TDMJ_35_ if TDMJ_35_ else '|'.join(re.findall(f'土地面积(?:\s*)[\(（]*平方米[\)）]*(?:[\s]*)({self.reStr})(?:\n)',  items))
                # 容积率
                RJL_36 = '|'.join(re.findall('容积率(?:[\s]*)([（）\w\.：≤≥\-\/\%,、\.]*)(?:\n)',  items))
                # 计容建筑面积(m2)
                JRZJMJ_41 = '|'.join(re.findall(f'计容建筑面积\(m2\)(?:[\s]*)({self.reStr})(?:\n)',  items))
                # 出让方式
                CRFS_42 = '|'.join(re.findall('出让方式[：]*(?:[\s]*)([（）\w\.：≤≥\-\/\%,、\.]*)(?:\n)',  items))
                # 出让年限
                CRNX_43 = '|'.join(re.findall(f'出让年限(?:[\s]*)({self.reStr})(?:\n)',  items))
                # 成交价(万元) / 成交价
                CJJ_44_ = '|'.join(re.findall(f'成交价(?:[\s]*)[\(（]*万元[）\)]*(?:[\s]*)({self.reStr})(?:\n)',  items))
                CJJ_44 = CJJ_44_ if CJJ_44_ else '|'.join(re.findall(f'成交价：(?:[\s]*)({self.reStr})(?:三)', items))
                # 受让单位
                SRDW_45 = '|'.join(re.findall('受让单位(?:[\s]*)([（）\w\.：≤≥\-\/\%,、\.]*)(?:\n)',  items))
                # 土地使用条件
                TDSYTJ_46 = '|'.join(re.findall('土地(?:[\s]*)使用(?:[\s]*)条件(?:[\s]*)([（）\w\.:： ≤；≥\-\/\%,、\.]*)(?:\n)',  items))
                # 交易时间
                JYSJ_47 = '|'.join(re.findall('交易时间(?:[\s]*)([（）\w\.：≤≥\-\/\%,、\.]*)(?:\n)',  items))
                # 成交人
                CJR_48 = '|'.join(re.findall(f'成交人：(?:[\s]*)({self.reStr})(?:二)',  items))
                # 备注
                BZ_49 = '|'.join(re.findall(f'备注：(?:[\s]*)({self.reStr})(?:\n)',  items))
                # 联系地址
                LXDZ_51 = '|'.join(re.findall(f'联系地址:(?:[\s]*)([{self.reStr}]*)(?:\s)',  items))
                # 联系电话
                LXDH_52 = '|'.join(re.findall('联系电话：(?:[\s]*)([（）\w\.：≤≥\-\/\%,、\.]*)(?:\n)',  items))
                # 公示期
                GSQ_53 = '|'.join(re.findall(f'公示时间：(?:[\s]*)([{self.reStr}]*)(?:\n)',  items))
            else:
                if not table:
                    # TODO 正则匹配的页面
                    # 公告序号
                    GGXH_31 = '|'.join(re.findall('公告序号(?:[\s]*)([（）\w\.：\-\/\%,、]*)(?:\n)', items))
                    # 宗地编号 / 编号
                    ZDBH_32_ = '|'.join(re.findall(f'[宗地](?:[\s]*)编号(?:[\s]*)([{self.reStr}]*)(?:\s)', items))
                    ZDBH_32 = ZDBH_32_.replace('：', '') if ZDBH_32_ else ZDBH_32_
                    # 地块位置
                    DKWZ_33_ = '|'.join(re.findall(f'地块位置(?:[\s]*)({self.reStr})(?:\n)', items))
                    DKWZ_33 = DKWZ_33_ if DKWZ_33_ else '|'.join(
                        re.findall('土地(?:[\s]*)坐落(?:[\s]*)([（）\w\.:： ≤；≥\-\/\%,、\.]*)(?:\n)', items))
                    # 土地用途 / 用途
                    TDYT_34 = '|'.join(re.findall('[土地]?用途(?:[\s]*)([（）\w\.：\-\/\%,、]*)(?:\n)', items))
                    # 土地面积(平方米) / 土地面积(m2) / 出让面积(m)
                    TDMJ_35_ = '|'.join(re.findall('土地面积\(m2\)(?:[\s]*)([（）\w\.：\-\/\%,、\.]*)(?:\n)', items))
                    TDMJ_35 = TDMJ_35_ if TDMJ_35_ else '|'.join(
                        re.findall(f'土地面积(?:\s*)[\(（]*平方米[\)）]*(?:[\s]*)({self.reStr})(?:\n)', items))
                    # 容积率
                    RJL_36 = '|'.join(re.findall('容积率(?:[\s]*)([（）\w\.：≤≥\-\/\%,、\.]*)(?:\n)', items))
                    # 计容建筑面积(m2)
                    JRZJMJ_41 = '|'.join(re.findall(f'计容建筑面积\(m2\)(?:[\s]*)({self.reStr})(?:\n)', items))
                    # 出让方式
                    CRFS_42 = '|'.join(re.findall('出让方式[：]*(?:[\s]*)([（）\w\.：≤≥\-\/\%,、\.]*)(?:\n)', items))
                    # 出让年限
                    CRNX_43 = '|'.join(re.findall(f'出让年限(?:[\s]*)({self.reStr})(?:\n)', items))
                    # 成交价(万元) / 成交价
                    CJJ_44_ = '|'.join(re.findall(f'成交价(?:[\s]*)[\(（]*万元[）\)]*(?:[\s]*)({self.reStr})(?:\n)', items))
                    CJJ_44 = CJJ_44_ if CJJ_44_ else '|'.join(re.findall(f'成交价：(?:[\s]*)({self.reStr})(?:三)', items))
                    # 受让单位
                    SRDW_45 = '|'.join(re.findall('受让单位(?:[\s]*)([（）\w\.：≤≥\-\/\%,、\.]*)(?:\n)', items))
                    # 土地使用条件
                    TDSYTJ_46 = '|'.join(
                        re.findall('土地(?:[\s]*)使用(?:[\s]*)条件(?:[\s]*)([（）\w\.:： ≤；≥\-\/\%,、\.]*)(?:\n)', items))
                    # 交易时间
                    JYSJ_47 = '|'.join(re.findall('交易时间(?:[\s]*)([（）\w\.：≤≥\-\/\%,、\.]*)(?:\n)', items))
                    # 成交人
                    CJR_48 = '|'.join(re.findall(f'成交人：(?:[\s]*)({self.reStr})(?:二)', items))
                    # 备注
                    BZ_49 = '|'.join(re.findall(f'备注：(?:[\s]*)({self.reStr})(?:\n)', items))
                    # 联系地址
                    LXDZ_51 = '|'.join(re.findall(f'联系地址:(?:[\s]*)([{self.reStr}]*)(?:\s)', items))
                    # 联系电话
                    LXDH_52 = '|'.join(re.findall('联系电话：(?:[\s]*)([（）\w\.：≤≥\-\/\%,、\.]*)(?:\n)', items))
                    # 公示期
                    GSQ_53 = '|'.join(re.findall(f'公示时间：(?:[\s]*)([{self.reStr}]*)(?:\n)', items))
                else:
                    htmlTable = htmlTableTransformer()
                    tdData = htmlTable.tableTrTdRegulation(table)
                    # 宗地编号 / 编号
                    ZDBH_32 = tdData.get('编号') if tdData.get('编号') else tdData.get('地块编号')
                    # 地块位置
                    DKWZ_33 = tdData.get('地块位置')
                    # 土地用途 / 用途
                    TDYT_34 = tdData.get('用途') if tdData.get('用途') else tdData.get('土地用途')
                    # 土地面积(平方米) / 土地面积(m2) / 出让面积(m)
                    TDMJ_35_ = tdData.get('地块面积（平方米）') if tdData.get('地块面积（平方米）') else tdData.get('地块面积（㎡）')
                    TDMJ_35 = TDMJ_35_ if TDMJ_35_ else tdData.get('宗地面积（平方米）')
                    # 出让方式
                    CRFS_42 = tdData.get('出让方式')
                    # 容积率
                    RJL_36 = tdData.get('容积率')
                    # 建筑密度( %)
                    JZMD_37 = tdData.get('建筑密度（%）')
                    # 绿地率( %)
                    LDL_38 = tdData.get('绿地率（%）')
                    # 底价(万元)
                    DJ_40 = tdData.get('底价(万元)')
                    # 保证金(万元)
                    BZJ_39 = tdData.get('保证金(万元)')
                    # 出让年限
                    CRNX_43 = tdData.get('出让年限')
                    # 成交价(万元) / 成交价
                    CJJ_44 = tdData.get('成交价（万元）') if tdData.get('成交价（万元）') else tdData.get('成交价格（万元）')
                    # 成交人
                    CJR_48 = tdData.get('成交人')
                    # 备注
                    BZ_49 =  tdData.get('备注')
                    # 公示期
                    GSQ_53 = reFunction(f'公示期:(?:[\s]*)([{self.reStr}]*)(?:\s)',  items)
                    # 联系单位
                    LXDW_50 = reFunction(f'联 系 人：(?:[\s]*)([{self.reStr}]*)(?:\s)',  items)
                    # 联系地址
                    LXDZ_51 = reFunction(f'联系地址:(?:[\s]*)([{self.reStr}]*)(?:\s)',  items)
                    # 联系电话
                    LXDH_52 = reFunction(f'联系电话：(?:[\s]*)([{self.reStr}]*)(?:\s)',  items)

            # 爬取时间
            crawlingTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            # 爬取地址url
            url = url if url else response.url
            # 唯一标识
            md5Mark = encrypt_md5(url + ZDBH_32 + DKWZ_33)

            # 存储数据
            csvFile = [WJBT_27,XXSJ_28,ZWBT_29,GGXH_31, ZDBH_32, DKWZ_33, TDYT_34, TDMJ_35, RJL_36, JZMD_37, LDL_38, BZJ_39, DJ_40, JRZJMJ_41, CRFS_42,CRNX_43, CJJ_44, SRDW_45, TDSYTJ_46, JYSJ_47, CJR_48, BZ_49, LXDW_50, LXDZ_51, LXDH_52, GSQ_53,
                       crawlingTime,url,md5Mark,
                       ]
            self.fileDetail.write(','.join([_.replace(',', ' ').replace('\n', '').replace('\r', '') if _ else _ for _ in csvFile]))
            self.fileDetail.write('\n')
            self.log(f'数据获取成功', level=logging.INFO)
            yield
            #TODO
        except Exception as e:
            self.log(f'详情页数据解析失败, 错误: {e}\n{traceback.format_exc()}', level=logging.ERROR)














