# -*- coding: utf-8 -*-
import datetime
import os
import random
import re
import string
import sys
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
from SpiderTools.tableAnalysis import htmlTableTransformer

curPath = os.path.abspath(os.path.dirname(__file__))
sys.path.append(curPath[:-9])
# src_dir = curPath + "/js/"


class shangHaiTransferResultSpider(CrawlSpider):
    name = 'shangHaiTransferResult'
    # 配置更换cookies时间
    COOKIES_SWITCH_TIME = datetime.datetime.now()

    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super(shangHaiTransferResultSpider, cls).__new__(cls)
            try:
                pathPage = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), 'Logs/DatashangHaiTransferResultPage.txt')
                pathDetail = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))), 'data/深圳市土地房产交易中心_土地_深圳.csv')
                cls.dirName = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))), 'data/深圳市土地房产交易中心/')
                cls.filePage = open(pathPage, 'w+')

                if os.path.exists(pathDetail):
                    cls.fileDetail = open(pathDetail, 'a+')
                else:
                    cls.fileDetail = open(pathDetail, 'a+')
                    cls.fileDetail.write("""交易时间,交易状态,宗地号,土地位置,起始价,土地用途,土地面积,交易方式,交易类型,宗地,发布时间,交易状态,中标人/竞得人,成交价(元),保证金 (元),起始价 (元),竞价阶梯 (元),封顶价 (元),竞买申请截止时间,竞买人数,正文标题,公告期,挂牌开始时间,挂牌结束时间,宗地代码/地块宗地编号,宗地号,土地位置,土地用途,准入行业类别,土地面积/土地面积(平方米),建筑面积(平方米)/总建筑面积,土地使用年期/土地使用年限,土地发展建设现状,容积率,挂牌起始价(人民币万元),竞买(投标)保证金(人民币万元),土地使用年限(年),保证金截止时间,地址,电话,宗地号,土地面积,建筑面积,容积率,建筑覆盖率,建筑高度,用途,使用年限,区域,位置,绿地率,建筑楼层,竞买人,竞买出价(元),竞价时间,状态,正文标题,发布日期,宗地号,竞得人,中标人,位置,土地用途,土地面积,建筑面积,起始价,成交价,溢价率,综合楼面单价,爬取时间,爬取地址url,唯一标识,附件,\n""")
            except Exception as e:
                cls.log('supplyResultSpider文件打开失败,{}'.format(datetime.datetime.now()), level=logging.ERROR)
        return cls.instance

    def __init__(self):
        super(shangHaiTransferResultSpider, self).__init__()
        dispatcher.connect(self.CloseSpider, signals.spider_closed)
        self.targetUrl = 'https://www.sz68.com/tiaim/web/getTargetJson'
        self.header = {'User-Agent': random.choice(agent_list)}

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
         按照优先级爬取
        '''
        try:
            pages = 110
            sumPage = 0
            for page in range(1, int(pages)+1):
                self.log('当前爬取页数{}'.format(page), level=logging.INFO)
                priority = int(pages) + 1 - int(page)
                self.filePage.write(str(page))
                data = {
                    'total_page': '110',
                    'tatol': '1312',
                    'currentPage': f'{page}',
                    'pageSize': '12',
                    'code': '0015-0001',
                    'type': '0,1,4,5,6,7,9,11,99',
                    'name': '',
                    'area': '',
                    'status': '',
                    'currentSelectTime': '',
                    'stopstatus': '',
                    'suspendstatus': '',
                }
                yield FormRequest(self.targetUrl, method='POST', formdata=data, priority=priority, callback=self.parse_index,
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
        解析 josn 文件
        :param response:
        :return:
        '''
        try:
            resp = json.loads(response.text)
            # 解析json数据 发起请求
            targetsData = resp.get('result').get('targets')
            page = response.meta.get('page')
            for targets in targetsData:
                # GOODID ID 拼接url
                id = targets.get('ID')
                goodid= targets.get('GOODID')
                url = f'https://www.sz68.com/tiaim/web/landDetail?id={id}&code=0015&goodId={goodid}'
                # 交易时间
                JYSJ = targets.get('AUCTION_BEGIN_TIME')
                # 交易状态 STATUS == 5 的 已成交
                if targets.get('STATUS') == 5:
                    JYZT = '已成交'
                elif targets.get('STATUS') == 3:
                    JYZT = '在交易'
                elif targets.get('STATUS') == 6:
                    JYZT = '未成交'
                # 宗地号
                ZDH = targets.get('NO')
                # 土地位置
                TDWZ = targets.get('ADDRESS')
                # 起始价
                QSJ = targets.get('BEGIN_PRICE')
                # 土地用途
                TDYT = targets.get('GOODS_USE')
                # 土地面积
                TDMJ = targets.get('GOODS_SIZE')
                # 交易方式
                JYFS = targets.get('TRANS_TYPE2')

                yield Request(url, method='GET', callback=self.parse_detail,
                                  meta={'page': page,
                                        'JYSJ': JYSJ,
                                        'JYZT': JYZT,
                                        'ZDH': ZDH,
                                        'TDWZ': TDWZ,
                                        'QSJ': QSJ,
                                        'TDYT': TDYT,
                                        'TDMJ': TDMJ,
                                        'JYFS': JYFS,
                                        'id': id,
                                        },
                                  # body=requests_data, headers={'Content-Type': 'application/json'}
                                  dont_filter=True,
                                  )
        except Exception as e:
            self.log(f'列表页解析失败{page}, 错误: {e}', level=logging.ERROR)

    def parse_detail(self, response):
        try:
            data = Selector(text=response.body.decode('utf-8'))
            noticeDetail = 'https://www.sz68.com' + data.xpath('//iframe[@id="externalframe1"]/@src').extract_first() if data.xpath('//iframe[@id="externalframe1"]/@src').extract_first() else 'https://www.sz68.com' + data.xpath('//iframe[@id="externalframe0"]/@src').extract_first()

            ZWBT = ''
            GGQ = ''
            GPKSSJ = ''
            GPJSSJ = ''
            ZDDM_DKZDBH = ''
            ZDH = ''
            DKWZ = ''
            DKYT = ''
            ZRHYLB = ''
            TDMJ = ''
            JZMJ = ''
            TDSYNX = ''
            TDFZXZ = ''
            RJL = ''
            GPQSJ = ''
            JMBZJ = ''
            TDSYNX = ''
            ZBJJZSJ = ''
            BMSJ = ''
            BMDD = ''
            DZ = ''
            DH = ''
            JYSJ = response.meta.get('JYSJ')
            JYZT = response.meta.get('JYZT')
            ZDH = response.meta.get('ZDH')
            TDWZ = response.meta.get('TDWZ')
            QSJ = response.meta.get('QSJ')
            TDYT = response.meta.get('TDYT')
            TDMJ = response.meta.get('TDMJ')
            JYFS = response.meta.get('JYFS')
            id = response.meta.get('id')
            # 公告详情
            detailData = requests.get(noticeDetail, headers=self.header, allow_redirects=False, timeout=60, verify=False)

            if detailData.status_code == 200:
                detail = Selector(text=detailData.content.decode('utf-8'))
                items = str(detail.xpath('string(.)').extract()[0]).replace('\xa0', '').replace('\u3000', '').replace('\n', '').replace(' ', '')
                # 正文标题
                ZWBT = ''.join(detail.xpath('/html/body/div/p[2]/span//text() | /html/body/p[2]/span//text()|/html/body/p[1]/span//text()').extract())
                # 公告期
                GGQ = reFunction('公告期自([\w \-\s]*)[止]?，', items)
                # 挂牌开始时间
                GPKSSJ = reFunction('挂牌期自(\d{4}年\d{1,2}月\d{1,2}日)[起]?至(?:\d{4}年\d{1,2}月\d{1,2}日\d{1,2}时)止', items)
                # 挂牌结束时间
                GPJSSJ = reFunction('挂牌期自(?:\d{4}年\d{1,2}月\d{1,2}日)[起]?至(\d{4}年\d{1,2}月\d{1,2}日\d{1,2}时)止', items)
                # TODO 解析页面表格
                soup = BeautifulSoup(detailData.text)
                table = soup.find('body').find('div').find('table') if soup.find('body').find('div').find('table') else soup.find('table')

                htmlTable = htmlTableTransformer()
                tdData = htmlTable.table_tr_td(table)
                # 宗地代码 / 地块宗地编号
                ZDDM_DKZDBH = tdData.get('宗地编号') if tdData.get('宗地编号') else tdData.get('地块宗地编号')
                # 宗地号
                ZDH_A = tdData.get('宗地号')
                # 土地位置
                DKWZ = tdData.get('土地位置')
                # 土地用途
                DKYT = tdData.get('土地用途')
                # 准入行业类别
                ZRHYLB = tdData.get('准入行业类别')
                # 土地面积 / 土地面积(平方米)
                TDMJ_A = tdData.get('土地面积（平方米）') if tdData.get('土地面积（平方米）') else tdData.get('土地面积')
                # 建筑面积(平方米) / 总建筑面积
                JZMJ = tdData.get('建筑面积（平方米）') if tdData.get('建筑面积（平方米）') else tdData.get('总建筑面积')
                # 挂牌起始价(人民币万元)
                GPQSJ = tdData.get('挂牌起始价（人民币、万元）')
                # 竞买(投标)保证金(人民币万元)
                JMBZJ = tdData.get('竞买（投标）保证金（人民币、万元）')
                # 土地使用年限(年)
                TDSYNX = tdData.get('土地使用年期')

                if not detail.xpath('//table').extract():
                    # 宗地代码 / 地块宗地编号
                    ZDDM_DKZDBH = reFunction('宗地编号([\w \-\s]*)，', items)
                    # 土地使用年期 / 土地使用年限  情况2 中的 土地使用年期
                    TDSYNX = reFunction('土地使用年[\s期限]*[为]?(\d*年)', items)
                    # 土地发展建设现状
                    TDFZXZ = reFunction('土地的发展建设现状：([\S\s]*。)', items)
                    # 容积率  容积率不大于1.518。
                    RJL = reFunction('容积率[\D]*([\.\d]*)。', items)
                    # 土地位置  宗地位于龙岗 中心城14号地，
                    DKWZ = reFunction('宗地位于([\w \s]*)，', items)
                    # 土地用途
                    DKYT = reFunction('土地用途为([\w \s]*)，', items)
                    # TODO 是否需要在解析一种页面  http://localhost:63342/IntegrationSpider/Logs/dwsw.html?_ijt=rfnsd28r0fb132e6i5qkd3db6f
                # 保证金截止时间
                ZBJJZSJ = reFunction('保证金的到账截止时间为(\d{4}年\d{1,2}月\d{1,2}日\d{1,2}时\d{1,2}分)', items)
                # 地址  //匹配这些中文标点符号 。 ？ ！ ， 、 ； ：

                DZ = '|'.join(re.findall('地址：([\w \.\-\s\/\%,\（\）。 \？ \！  、：]*)；咨询电话', items))
                # 电话
                DH = '|'.join(re.findall('咨询电话：([\w \.\-\s\/\%,\（\）。 \？ \！  、]*)[；。]', items))
            else:
                raise IntegrationException(f'获取公告详情失败, url: {noticeDetail}')

            # TODO 基本信息  完成
            itemsData = str(data.xpath('string(.)').extract()[0]).replace('\xa0', '').replace('\u3000', '')
            # 交易方式
            JYFS_A = data.xpath('//div[@class="content_case1"]/div[1]/ul/li[2]/span/text()').extract_first()
            # 交易类型
            JYLX = data.xpath('//div[@class="content_case1"]/div[1]/ul/li[1]/span/text()').extract_first()
            # 宗地
            ZD = data.xpath('//div[@class="content_case1"]/div[1]/div/text()').extract_first()
            # 发布时间
            FBSJ = data.xpath('//div[@class="content_case1"]/div[2]/span[2]/text()').extract_first()
            # 交易状态
            JYZT_A = data.xpath('//div[@class="content_case1"]/div[2]/span[3]/text()').extract_first()
            # 中标人 / 竞得人
            ZBR_24 = data.xpath('//div[@class="right_first"]/div[1]/div[2]/text()').extract_first()
            # 成交价(元)
            CJJ_25 = data.xpath('//div[@class="right_first"]/div[2]/div[2]/text()').extract_first()
            # 保证金(元)
            BZJ_26 = data.xpath('//div[@class="right_first twin"][1]/div[1]/div[2]/text()').extract_first()
            # 起始价(元)
            QSJ_A = data.xpath('//div[@class="right_first twin"][1]/div[2]/div[2]/text()').extract_first()
            # 竞价阶梯(元)
            JJJT_28 = data.xpath('//div[@class="right_first twin"][2]/div[1]/div[2]/text()').extract_first()
            # 封顶价(元)
            FDJ_29 = data.xpath('//div[@class="right_first twin"][2]/div[2]/div[2]/text()').extract_first()
            # 竞买申请截止时间
            JMSQJZSJ_30 = data.xpath('//div[@class="right_first twin"][3]/div[1]/div[2]/text()').extract_first()
            # 竞买人数
            JMRS_31 = data.xpath('//div[@class="right_first twin"][3]/div[2]/div[2]/text()').extract_first()

            # TODO 标的详情  完成
            BDdetail = data.xpath('//li[@class="weather_info_ul_item"]/div[2]/span')
            # 宗地号
            ZDH_B = BDdetail[0].xpath('text()').extract_first()
            # 土地面积
            TDMJ_B = BDdetail[1].xpath('text()').extract_first()
            # 建筑面积
            JZMJ_A = BDdetail[2].xpath('text()').extract_first()
            # 容积率
            RJL_A = BDdetail[3].xpath('text()').extract_first()
            # 建筑覆盖率
            JZFGL = BDdetail[4].xpath('text()').extract_first()
            # 建筑高度
            JZGD = BDdetail[5].xpath('text()').extract_first()
            # 用途
            YT = BDdetail[6].xpath('text()').extract_first()
            # 使用年限
            SYNX = BDdetail[7].xpath('text()').extract_first()
            # 区域
            QY = BDdetail[8].xpath('text()').extract_first()
            # 位置
            WZ = BDdetail[9].xpath('text()').extract_first()
            # 绿地率
            LDL = BDdetail[10].xpath('text()').extract_first()
            # 建筑楼层
            JZLC = BDdetail[11].xpath('text()').extract_first()

            # TODO 竞价记录 完成
            # 竞买人
            JMR = data.xpath('//div[@class="conomy"][1]/table/tr[2]/td[2]/text()').extract_first()
            # 竞买出价(元)
            JMSJ = data.xpath('//div[@class="conomy"][1]/table/tr[2]/td[3]/text()').extract_first()
            # 竞价时间
            CJSJ = data.xpath('//div[@class="conomy"][1]/table/tr[2]/td[4]/text()').extract_first()
            # 状态
            ZT = data.xpath('//div[@class="conomy"][1]/table/tr[2]/td[5]/text()').extract_first()

            # TODO 结果公示 完成
            results = requests.post('https://www.sz68.com/tiaim/web/resultdetailbytargetId', headers=self.header, data={'targetId': id}, allow_redirects=False, timeout=60, verify=False)
            if results.status_code == 200:
                resultsData = results.json()
                # 正文标题
                ZWBT_A = resultsData.get('notice').get('NAME')
                # 发布日期
                FBRQ = resultsData.get('notice').get('PUBLISH_TIME')
                # 宗地号
                ZDH_C = resultsData.get('notice').get('DTL_REF_NO')
                # 竞得人
                JDR = reFunction('竞得人：([\w \.\-\s\/\%,]*)<', resultsData.get('fileExtName'))
                # 中标人
                ZBR_A = reFunction('中标人：([\w \.\-\s\/\%,]*)<', resultsData.get('fileExtName'))
                # 位置
                WZ = reFunction('位置：([\w \.\-\s\/\%,、]*)<', resultsData.get('fileExtName'))
                # 土地用途
                TDYT_A = reFunction('土地用途：([\w \.\-\s\/\%,、]*)<', resultsData.get('fileExtName'))
                # 土地面积
                TDMJ_C = reFunction('土地面积：([\w \.\-\s\/\%,、]*)<', resultsData.get('fileExtName'))
                # 建筑面积
                JZMJ_B = reFunction('建筑面积：([\w \.\-\s\/\%,、]*)<', resultsData.get('fileExtName'))
                # 起始价
                QSJ_D = reFunction('起始价：([\w \.\-\s\/\%,、]*)<', resultsData.get('fileExtName'))
                # 成交价
                CJJ_A = reFunction('成交价：([\w \.\-\s\/\%,、]*)<', resultsData.get('fileExtName'))
                # 溢价率
                YJL = reFunction('溢价率：([\w \.\-\s\/\%,、]*)<', resultsData.get('fileExtName'))
                # 综合楼面单价
                ZHLMDJ = reFunction('综合楼面单价：([\w \.\-\s\/\%,、]*)<', resultsData.get('fileExtName'))

            # TODO  附件  解析出让合同  完成
            accessory = '土地模块|'
            links = data.xpath('//div[@class="accessory_link"]/a')
            for link in links:
                fileName = link.xpath('text()[position()=((position() mod 2)=0)]').extract_first().strip() if link.xpath('text()[position()=((position() mod 2)=0)]').extract_first() else '未知名称'
                try:
                    href = link.xpath('@href').extract_first()
                    linkPath = self.dirName + f'土地模块_{ZDH}' + fileName
                    response = requests.get(href, headers=self.header, timeout=200)

                    with open(linkPath, 'wb') as fp:
                        fp.write(response.content)
                except:
                    pass
                else:
                    accessory += fileName + '|'
            # 爬取时间
            crawlingTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            # 爬取地址url
            url = response.url
            md5Mark = encrypt_md5(ZDH + WZ + ZWBT + url)
            csvFile = [JYSJ,JYZT,ZDH,TDWZ,QSJ,TDYT,TDMJ,
JYFS_A,JYLX,ZD,FBSJ,JYZT_A,ZBR_24,CJJ_25,BZJ_26,QSJ_A,JJJT_28,FDJ_29,JMSQJZSJ_30,JMRS_31,ZWBT,GGQ,GPKSSJ,GPJSSJ,ZDDM_DKZDBH,ZDH_A,DKWZ,DKYT,ZRHYLB,TDMJ_A,JZMJ,TDSYNX,TDFZXZ,RJL,GPQSJ,JMBZJ,TDSYNX,ZBJJZSJ,DZ,DH,
ZDH_B,TDMJ_B,JZMJ_B,RJL_A,JZFGL,JZGD,YT,SYNX,QY,WZ,LDL,JZLC,JMR,JMSJ,CJSJ,ZT,
ZWBT_A,FBRQ,ZDH_C,JDR,ZBR_A,WZ,TDYT_A,TDMJ_C,JZMJ_B,QSJ_D,CJJ_A,YJL,ZHLMDJ,
crawlingTime,
url,
md5Mark,
accessory,]
            fileData = []
            for _ in csvFile:
                try:
                    fileData.append(_.replace(',', ' ').replace('\n', '').replace('\r', ''))
                except:
                    fileData.append(str(_))
            self.fileDetail.write(','.join(fileData))
            self.fileDetail.write('\n')
        except Exception as e:
            self.log(f'详情页数据解析失败, 错误: {e}', level=logging.ERROR)
        # isinstance
