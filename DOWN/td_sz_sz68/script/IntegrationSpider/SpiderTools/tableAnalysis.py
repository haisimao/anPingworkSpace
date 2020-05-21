# -*- coding:utf-8 -*-
import sys
import bs4
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup

# sys.path.append(sys.prefix + "\\Lib\\MyWheels")
# reload(sys)
# sys.setdefaultencoding('utf8')
import requests


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
        df0[1] = df0[0].apply(lambda e:len(e.find_all('td')))
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
            results[row[0]] = '|'.join(list(df[index])[1:])
        return results

    def standardize(self, df, delimiter='/\n/', b0 = True):
        """将数据的标题与数据分离，将有合并单元的行合并"""
        if b0 and df.iloc[0,:].hasnans and df.iloc[1,:].hasnans:# 假设第一排数据行没有横向合并单元格
            df.iloc[0, :] = df.iloc[0, :].fillna(method='ffill') + (delimiter + df.iloc[1,:]).fillna('')
            df.columns = df.iloc[0]
            df.columns.name = None
            df = df.drop([0,1], axis=0)

        for r in range(df.shape[0]-1, 0, -1):
            if df.iloc[r,:].hasnans:
                df.iloc[r-1, :] = df.iloc[r-1, :] + (delimiter + df.iloc[r, :]).fillna('')
                df = df.drop(r,axis=0)

        df.index = range(len(df.index)) # 索引重新从0计算
        return df


if __name__ == '__main__':
    from bs4 import BeautifulSoup
    header = {
        'Host': 'www.sz68.com',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:74.0) Gecko/20100101 Firefox/74.0',

    }
    url = 'https://www.sz68.com/tiaim/web/noticehtml?id=20191101181149425482053200333552&iframeid=externalframe1'
    resp = requests.get(url, headers=header, verify=False)
    soup = BeautifulSoup(resp.text)
    table = soup.find('body').find('div').find('table')
    html_table = htmlTableTransformer()
    data = html_table.table_tr_td(table)

    print(data)