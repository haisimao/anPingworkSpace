# -*- coding:utf-8 -*-
import re
import sys
import bs4
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup

# sys.path.append(sys.prefix + "\\Lib\\MyWheels")
# reload(sys)
# sys.setdefaultencoding('utf8')
import requests
from demjson import nan
from scrapy import Selector

from SpiderTools.Tool import reFunction


class htmlTableTransformer(object):

    def __init__(self):
        pass

    def table_tr_td(self, e_table, fill_method=None):
        """
        :param e_table: bs4中的table元素
        :param fill_method : 参数与fillna()中的method相同，选择填充方式，否则用None
        :return:
        """
        if not (isinstance(e_table, bs4.element.Tag) or isinstance(e_table, bs4.BeautifulSoup)):
            e_table = bs4.BeautifulSoup(e_table, 'html.parser')

        # 搭建表格框架
        df0 = pd.DataFrame(e_table.find_all('tr'))
        df0[1] = df0[0].apply(lambda e: len(e.find_all('td')))
        col_count = max(df0[1])
        row_count = len(df0.index)
        df = pd.DataFrame(np.zeros([row_count, col_count]), dtype=int)

        # 根据网页中的表格，还原在dataframe中，有合并单元格现象的
        # 值填在第一个单元格中，其他的用None填充
        e_trs = df0[0].tolist()
        for r in range(row_count):
            row = e_trs[r]
            e_tds = row.find_all('td')
            i = 0
            has_colspan = False
            for c in range(col_count):
                if pd.isnull(df.iloc[r,c]):
                    continue
                e_td = e_tds[i]
                # 横向合并的单元格
                if e_td.has_attr('colspan'):
                    has_colspan = True
                    for j in range(1, int(e_td['colspan'])):
                        df.iloc[r, c + j] = None
                # 竖向合并的单元格
                if e_td.has_attr('rowspan'):
                    for j in range(1, int(e_td['rowspan'])):
                        df.iloc[r + j, c] = None
                df.iloc[r, c] = e_td.get_text(strip=True)
                i = i + 1
            if has_colspan and fill_method:
                df.iloc[r,:] = df.iloc[r,:].fillna(method = fill_method)
        df.fillna(method='ffill', inplace=True)  # 用前面的值来填充
        results = {}
        for index, row in df.iteritems():
            results[row[0]] = '|'.join(list(df[index])[1:]).replace(str(row[0]), '')

        return results

    def standardize(self, df, delimiter='/\n/', b0=True):
        """将数据的标题与数据分离，将有合并单元的行合并"""
        if b0 and df.iloc[0,:].hasnans and df.iloc[1,:].hasnans:  # 假设第一排数据行没有横向合并单元格
            df.iloc[0, :] = df.iloc[0, :].fillna(method='ffill') + (delimiter + df.iloc[1,:]).fillna('')
            df.columns = df.iloc[0]
            df.columns.name = None
            df = df.drop([0,1], axis=0)

        for r in range(df.shape[0]-1, 0, -1):
            if df.iloc[r,:].hasnans:
                df.iloc[r-1, :] = df.iloc[r-1, :] + (delimiter + df.iloc[r, :]).fillna('')
                df = df.drop(r,axis=0)

        df.index = range(len(df.index))  # 索引重新从0计算
        return df

    def tableTrTdUNregulationToList(self, e_table, fill_method=None, toList=True):
        """
        :param e_table: bs4中的table元素
        :param fill_method : 参数与fillna()中的method相同，选择填充方式，否则用None
        :return:
        """
        if not (isinstance(e_table, bs4.element.Tag) or isinstance(e_table, bs4.BeautifulSoup)):
            e_table = bs4.BeautifulSoup(e_table, 'html.parser')

        # 搭建表格框架
        df0 = pd.DataFrame(e_table.find_all('tr'))
        df0[1] = df0[0].apply(lambda e: len(e.find_all('td')))
        col_count = max(df0[1])
        row_count = len(df0.index)
        df = pd.DataFrame(np.zeros([row_count, col_count]), dtype=int)

        # 根据网页中的表格，还原在dataframe中，有合并单元格现象的
        # 值填在第一个单元格中，其他的用None填充
        e_trs = df0[0].tolist()
        for r in range(row_count):
            row = e_trs[r]  # 获取具体行
            e_tds = row.find_all('td')  # 获取具体行的 td
            i = 0
            has_colspan = False
            for c in range(col_count):
                if pd.isnull(df.iloc[r, c]):
                    continue
                e_td = e_tds[i]
                # 横向合并的单元格
                if e_td.has_attr('colspan'):
                    has_colspan = True
                    for j in range(1, int(e_td['colspan'])):
                        df.iloc[r, c + j] = None
                # 竖向合并的单元格
                if e_td.has_attr('rowspan'):
                    for j in range(1, int(e_td['rowspan'])):
                        df.iloc[r + j, c] = None
                df.iloc[r, c] = e_td.get_text(strip=True)
                i = i + 1
            if has_colspan and fill_method:
                df.iloc[r,:] = df.iloc[r,:].fillna(method=fill_method)
        df.fillna(method='ffill', inplace=True)  # 用前面的值来填充
        self.standardize(df)
        results = {}
        for index, row in df.iteritems():
            if toList:
                results[row[0]] = [str(_).replace(str(row[0]), '') for _ in list(df[index])[1:]]
            else:
                results[row[0]] = '|'.join(list(df[index])[1:]).replace(str(row[0]), '')

        return results

    # TODO 将两个 解析合并  try 的方式
    def tableTrTdRegulation(self, e_table, fill_method=None):
        """
        适用于规则但字段变化的表格
        :param e_table: bs4中的table元素
        :param fill_method : 参数与fillna()中的method相同，选择填充方式，否则用None
        :return:
        """
        if not e_table:
            return
        text = ''
        titleList = []
        results = {}
        RegulationTable = e_table.tbody if e_table.tbody else e_table
        td_1 = RegulationTable.findAll('tr')[0]
        for td in td_1.findAll('td'):
            titleList.append(td.getText())

        for tr in RegulationTable.findAll('tr')[1:]:
            tds = tr.findAll('td')
            for _ in range(len(tds)):
                strTd = results.get(titleList[_]) if results.get(titleList[_]) else ''  # 拿到已写入的数据
                results[titleList[_]] = strTd + '|' + tds[_].getText()

        newResults = {}
        for key in list(results.keys()):
            newKey = key.replace('\xa0', '').replace('\n', '').replace(' ', '')
            newResults[newKey] = results.get(key).replace('\n', '').replace(newKey, '')
        return newResults

    def tableTrTdRegulationToList(self, e_table, fill_method=None):
        """
        适用于规则但字段变化的表格
        :param e_table: bs4中的table元素
        :param fill_method : 参数与fillna()中的method相同，选择填充方式，否则用None
        :return:
        """
        if not e_table:
            return

        text = ''
        titleList = []
        results = {}
        RegulationTable = e_table.tbody if e_table.tbody else e_table
        maxCol = max([len(_.findAll('td')) for _ in RegulationTable.findAll('tr')])
        td_1 = RegulationTable.findAll('tr')[0]
        for td in td_1.findAll('td'):
            titleList.append(td.getText())

        for tr in RegulationTable.findAll('tr')[1:]:
            tds = tr.findAll('td')
            for _ in range(maxCol):  # 取到最大的列
                strTd = results.get(titleList[_]) if results.get(titleList[_]) else []  # 拿到已写入的数据
                try:
                    strTd.append(tds[_].getText())
                except:
                    try:
                        strTd.append(strTd[-1])
                    except:
                        strTd.append('')
                results[titleList[_]] = strTd

        newResults = {}
        for key in list(results.keys()):
            newKey = key.replace('\xa0', '').replace('\n', '').replace(' ', '').replace('\t', '')
            newResults[newKey] = [_.replace('\n', '').replace(newKey, '') for _ in results.get(key)]
        return newResults

    def tableTrTdChangeToList(self, e_table, fill_method=None):
            """
            适用于规则但字段变化的表格
            :param e_table: bs4中的table元素
            :param fill_method : 参数与fillna()中的method相同，选择填充方式，否则用None
            :return:
            """
            # 搭建表格框架
            df0 = pd.DataFrame(e_table.find_all('tr'))
            df0[1] = df0[0].apply(lambda e: len(e.find_all('td')))
            col_count = max(df0[1])
            row_count = len(df0.index)

            if not e_table:
                return
            text = ''
            titleList = []
            results = {}
            RegulationTable = e_table.tbody if e_table.tbody else e_table
            td_1 = RegulationTable.findAll('tr')[0]
            for td in td_1.findAll('td'):
                titleList.append(td.getText())

            RegulationTr = RegulationTable.findAll('tr')
            for index in range(1, len(RegulationTr)):
                # for tr in RegulationTable.findAll('tr')[1:]:
                tds = RegulationTr[index].findAll('td')
                for _ in range(col_count):  # 取到最大的列
                    strTd = results.get(titleList[_]) if results.get(titleList[_]) else []  # 拿到已写入的数据
                    try:
                        strTd.append(tds[_].getText())  # 按照 td 顺序获取数据 若出错则获取前一个数据, 若出错, 则插入空值
                    except:
                        try:
                            strTd.append(strTd[-1])
                        except:
                            strTd.append('')
                    results[titleList[_]] = strTd

            newResults = {}
            for key in list(results.keys()):
                newKey = key.replace('\xa0', '').replace('\n', '').replace(' ', '').replace('\t', '')
                newResults[newKey] = [_.replace('\n', '').replace(newKey, '') for _ in results.get(key)]
            return newResults


if __name__ == '__main__':
    '''
    移除: extract()
    插入: insert_after()
    my_index = self.parent.index(self)
    self.extract()
    old_parent.insert(my_index, replace_with)
    替换: replace_with()
    1. 找到 table.tbody.find_all('tr')[0].find_all('td', colspan='4')
    2. 找到 table.tbody.find_all('tr')[1].find_all('td')
    '''
    from bs4 import BeautifulSoup
    header = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:74.0) Gecko/20100101 Firefox/74.0',
               }
    url = 'https://www.lyggzy.com.cn/lyztb/InfoDetail/?InfoID=3c28d23d-096e-4abd-848a-c948a82477b2&CategoryNum=084001'

    resp = requests.get(url, headers=header, verify=False)
    # resp.content.decode('utf-8')

    soup = BeautifulSoup(resp.content.decode('utf-8'))
    table = soup.find('table')

    htmlTable = htmlTableTransformer()
    # table_tr_td, tableTrTdUNregulationToList (数据单元格有合并)     tableTrTdRegulationToList (标准单元格)
    results = htmlTable.tableTrTdRegulationToList(table)
    print(results)
    print(list(results.keys()))
