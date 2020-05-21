import re

import requests
from bs4 import BeautifulSoup
from scrapy import Selector

from SpiderTools.Tool import reFunction
from SpiderTools.tableAnalysis import htmlTableTransformer

header = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:74.0) Gecko/20100101 Firefox/74.0',}
url = 'http://ggzy.lasa.gov.cn/Article/ArticleDetail?id=51800'
response = requests.get(url, headers=header, verify=False)

data = Selector(text=response.content.decode('utf-8'))
items = str(data.xpath('string(.)').extract()[0]).replace('\xa0', '').replace('\u3000', '')

reStr = '（）\w\.:： 。 \(\)〔〕㎡≤；，≥《》\-\/\%,、\.﹪㎡'
# TODO 共有字段


# TODO //table[@border="1"]   //table[@border="0"]
# table 解析
if '宗地编号' not in items and '配套建筑规划用地' not in items:
    if data.xpath('//table[@border="0"]') and '主要规划指标' not in items:
        soup = BeautifulSoup(response.content.decode('utf-8'))
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
        RJL_9 = tdData.get('容积率')
        # 绿地率
        LDL_10 = tdData.get('绿地率')
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
        soup = BeautifulSoup(response.content.decode('utf-8'))
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
        ZDZL_5 = ZDZL_5_.replace(reFunction(f'备注(?:[\s]*)([{reStr}]*)\s', reFunction('一([\s\S]*)二', items)), '')
        # 面积
        MJ_6 = tdData.get('土地面积(m2)') if tdData.get('土地面积(m2)') else tdData.get('土地面积(平方米)')
        # 土地用途
        TDYT_7 = tdData.get('土地用途') if tdData.get('土地用途') else tdData.get('规划地性质')
        # 出让年限
        CRNX_8_ = tdData.get(r'出让\u3000年限') if tdData.get(r'出让\u3000年限') else tdData.get('出让年限')
        CRNX_8 = CRNX_8_ if CRNX_8_ else tdData.get('出让年限（年）')
        # 容积率
        RJL_9 = tdData.get('容积率')
        # 绿地率
        LDL_10 = tdData.get('绿地率') if tdData.get('绿地率') else tdData.get('绿地率（%）')
        # 建筑密度
        JZMD_11_ = tdData.get(r'建筑\u3000密度') if tdData.get(r'建筑\u3000密度') else tdData.get('建筑密度')
        JZMD_11 = JZMD_11_ if JZMD_11_ else tdData.get('建筑密度（%）')
        # 建筑限高
        JZXG_12_ = tdData.get('建筑限高') if tdData.get('建筑限高') else tdData.get('建筑高度（m）')
        JZXG_12 = JZXG_12_ if JZXG_12_ else tdData.get('建筑高度')
        # 竞买保证金
        JMBZJ_13 = tdData.get('竞买保证金(元)') if tdData.get('竞买保证金(元)') else tdData.get('竞买保证金（万元）')
        # 起始价
        QSJ_14_ = tdData.get('起始价(元)') if tdData.get('起始价(元)') else tdData.get('挂牌出让起始价（元）')
        QSJ_14 = QSJ_14_ if QSJ_14_ else tdData.get('起始价（万元）')
        # 增价幅度
        ZJFD_15 = tdData.get('增价幅度(万元)') if tdData.get('增价幅度(万元)') else tdData.get('加价幅度')
        if ZJFD_15 == '' and QSJ_14 == '' and JMBZJ_13 == '':
            soup = BeautifulSoup(response.content.decode('utf-8'))
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
        soup = BeautifulSoup(response.content.decode('utf-8'))
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
