import requests
import re
import os
import time
import pymongo
from pyquery import PyQuery as pq
from urllib.parse import urljoin
from fontTools.ttLib import TTFont
from hashlib import md5
from requests.exceptions import ReadTimeout, ConnectionError
from fonts import font_md5_list_dict
from keyword_list import *
from urllib.parse import unquote

class LandChina:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36',
            'Cookie':f'Hm_lvt_83853859c7247c5b03b527894622d3fa=1587194605,1587302414,1587386859,1587430790; ASP.NET_SessionId=uamiaoeymyazhuxclijuqxpm; security_session_verify=b85755a9c9b76a4d6195095ceaf8620d; security_session_high_verify=f06c25fa45c4419e26e53b51dff6f097; security_session_mid_verify=53623062e0ea9dd09f49fe550795e88a; Hm_lpvt_83853859c7247c5b03b527894622d3fa={int(time.time())}'

        }

        # client = pymongo.MongoClient('localhost',27017)
        # db = client['landchina']
        # self.collection = db['供地计划']

    def parse_index(self, page,city_code,city_name):
        # print(f'正在爬取第{page}页')
        url = 'http://www.landchina.com/default.aspx?tabid=226'
        city_info = unquote(f'6506735a-523d-4bde-9674-121656045ed1:{city_code}' + u"▓~" + city_name)
        city_info = city_info.encode("gb18030")
        data = {
            '__VIEWSTATE': '/wEPDwUJNjkzNzgyNTU4D2QWAmYPZBYIZg9kFgICAQ9kFgJmDxYCHgdWaXNpYmxlaGQCAQ9kFgICAQ8WAh4Fc3R5bGUFIEJBQ0tHUk9VTkQtQ09MT1I6I2YzZjVmNztDT0xPUjo7ZAICD2QWAgIBD2QWAmYPZBYCZg9kFgJmD2QWBGYPZBYCZg9kFgJmD2QWAmYPZBYCZg9kFgJmDxYEHwEFIENPTE9SOiNEM0QzRDM7QkFDS0dST1VORC1DT0xPUjo7HwBoFgJmD2QWAgIBD2QWAmYPDxYCHgRUZXh0ZWRkAgEPZBYCZg9kFgJmD2QWAmYPZBYEZg9kFgJmDxYEHwEFhwFDT0xPUjojRDNEM0QzO0JBQ0tHUk9VTkQtQ09MT1I6O0JBQ0tHUk9VTkQtSU1BR0U6dXJsKGh0dHA6Ly93d3cubGFuZGNoaW5hLmNvbS9Vc2VyL2RlZmF1bHQvVXBsb2FkL3N5c0ZyYW1lSW1nL3hfdGRzY3dfc3lfamhnZ18wMDAuZ2lmKTseBmhlaWdodAUBMxYCZg9kFgICAQ9kFgJmDw8WAh8CZWRkAgIPZBYCZg9kFgJmD2QWAmYPZBYCZg9kFgJmD2QWAmYPZBYEZg9kFgJmDxYEHwEFIENPTE9SOiNEM0QzRDM7QkFDS0dST1VORC1DT0xPUjo7HwBoFgJmD2QWAgIBD2QWAmYPDxYCHwJlZGQCAg9kFgJmD2QWBGYPZBYCZg9kFgJmD2QWAmYPZBYCZg9kFgJmD2QWAmYPFgQfAQUgQ09MT1I6I0QzRDNEMztCQUNLR1JPVU5ELUNPTE9SOjsfAGgWAmYPZBYCAgEPZBYCZg8PFgIfAmVkZAICD2QWBGYPZBYCZg9kFgJmD2QWAmYPZBYCAgEPZBYCZg8WBB8BBYYBQ09MT1I6IzAwMDAwMDtCQUNLR1JPVU5ELUNPTE9SOjtCQUNLR1JPVU5ELUlNQUdFOnVybChodHRwOi8vd3d3LmxhbmRjaGluYS5jb20vVXNlci9kZWZhdWx0L1VwbG9hZC9zeXNGcmFtZUltZy94X3Rkc2N3X3p5X2dkamhfMDEuZ2lmKTsfAwUCNDYWAmYPZBYCAgEPZBYCZg8PFgIfAmVkZAIBD2QWAmYPZBYCZg9kFgJmD2QWAgIBD2QWAmYPFgQfAQUgQ09MT1I6I0QzRDNEMztCQUNLR1JPVU5ELUNPTE9SOjsfA2QWAmYPZBYCAgEPZBYCZg8PFgIfAmVkZAIDD2QWAgIDDxYEHglpbm5lcmh0bWwF/gY8cCBhbGlnbj0iY2VudGVyIj48c3BhbiBzdHlsZT0iZm9udC1zaXplOiB4LXNtYWxsIj4mbmJzcDs8YnIgLz4NCiZuYnNwOzxhIHRhcmdldD0iX3NlbGYiIGhyZWY9Imh0dHBzOi8vd3d3LmxhbmRjaGluYS5jb20vIj48aW1nIGJvcmRlcj0iMCIgYWx0PSIiIHdpZHRoPSIyNjAiIGhlaWdodD0iNjEiIHNyYz0iL1VzZXIvZGVmYXVsdC9VcGxvYWQvZmNrL2ltYWdlL3Rkc2N3X2xvZ2UucG5nIiAvPjwvYT4mbmJzcDs8YnIgLz4NCiZuYnNwOzxzcGFuIHN0eWxlPSJjb2xvcjogI2ZmZmZmZiI+Q29weXJpZ2h0IDIwMDgtMjAxOSBEUkNuZXQuIEFsbCBSaWdodHMgUmVzZXJ2ZWQmbmJzcDsmbmJzcDsmbmJzcDsgPHNjcmlwdCB0eXBlPSJ0ZXh0L2phdmFzY3JpcHQiPg0KdmFyIF9iZGhtUHJvdG9jb2wgPSAoKCJodHRwczoiID09IGRvY3VtZW50LmxvY2F0aW9uLnByb3RvY29sKSA/ICIgaHR0cHM6Ly8iIDogIiBodHRwczovLyIpOw0KZG9jdW1lbnQud3JpdGUodW5lc2NhcGUoIiUzQ3NjcmlwdCBzcmM9JyIgKyBfYmRobVByb3RvY29sICsgImhtLmJhaWR1LmNvbS9oLmpzJTNGODM4NTM4NTljNzI0N2M1YjAzYjUyNzg5NDYyMmQzZmEnIHR5cGU9J3RleHQvamF2YXNjcmlwdCclM0UlM0Mvc2NyaXB0JTNFIikpOw0KPC9zY3JpcHQ+Jm5ic3A7PGJyIC8+DQrniYjmnYPmiYDmnIkmbmJzcDsg5Lit5Zu95Zyf5Zyw5biC5Zy6572RJm5ic3A7Jm5ic3A75oqA5pyv5pSv5oyBOua1meaxn+iHu+WWhOenkeaKgOiCoeS7veaciemZkOWFrOWPuCZuYnNwOzxiciAvPg0K5aSH5qGI5Y+3OiDkuqxJQ1DlpIcxMjAzOTQxNOWPty00IOS6rOWFrOe9keWuieWkhzExMDEwMjAwMDY2NigyKSZuYnNwOzxiciAvPg0KPC9zcGFuPiZuYnNwOyZuYnNwOyZuYnNwOzxiciAvPg0KJm5ic3A7PC9zcGFuPjwvcD4fAQVkQkFDS0dST1VORC1JTUFHRTp1cmwoaHR0cDovL3d3dy5sYW5kY2hpbmEuY29tL1VzZXIvZGVmYXVsdC9VcGxvYWQvc3lzRnJhbWVJbWcveF90ZHNjdzIwMTNfeXdfMS5qcGcpO2Rkf0+QBjsHhcHKb5rQVhQblyjSnNEO2KMX34QFotC1QNk=',
            '__EVENTVALIDATION': '/wEdAAIWJqyqYCmxDee9SvFgILYOCeA4P5qp+tM6YGffBqgTjWJQHe1PSOrkpdHZ6Qek6PJhjBqA8LfYuYDMszYikPu9',
            'hidComName': 'default',
            'TAB_QuerySubmitConditionData': city_info,
            'TAB_QuerySubmitOrderData': '',
            'TAB_RowButtonActionControl': '',
            'TAB_QuerySubmitPagerData': page,
            'TAB_QuerySubmitSortData': ''
        }
        try:
            response = requests.post(url, headers=self.headers, data=data, timeout=15)
            if response.status_code == 200:
                doc = pq(response.text)
                gts = doc('#TAB_contentTable > tbody > tr:gt(0)').items()
                for gt in gts:
                    yield {
                        '行政区代码': gt('td:nth-child(2)').text().strip(),
                        'url': 'http://www.landchina.com/' + gt('td:nth-child(3) > a').attr('href')
                    }
        except (ReadTimeout, ConnectionError) as e:
            # print('连接异常，重试', e.args)
            self.parse_index(page)

    def parse_detail(self, index, failed_time=0):
        if index:
            url = index.get('url')
            # print('爬取详情页', url)
            try:
                response = requests.get(url, headers=self.headers, timeout=10)
                if response.status_code == 200:
                    response.encoding = 'gbk'
                    html = response.text
                    doc = pq(html)
                    font_filename = self.download_woff(url, html)

                    detail = {
                        '标题': doc('#lblTitle').text().strip(),
                        '发布时间': doc('#lblCreateDate').text().strip()[5:],
                        '行政区': doc('#lblXzq').text().strip()[4:],
                        '行政区代码': index.get('行政区代码'),
                        '城市': '',
                        '内容文本': '',
                        '内容链接': url
                    }

                    region = re.search(r'>(.*?)>', detail['行政区'])
                    detail['城市'] = region.group(1).strip() if region else ''

                    year = re.search(r'(\d+)年度', detail['标题'])
                    detail['供应计划年度'] = year.group(1).strip() if year else ''

                    content_except_table = self.parse_font(doc('#tdContent').remove('table').text(), font_filename).replace('\n', '')

                    detail['内容文本'] = content_except_table

                    supply_gross = self.get_supply_gross(content_except_table)
                    detail['供应总量'] = supply_gross if supply_gross else ''

                    new_construction = self.get_part_area(content_except_table, NEW_CONSTRUCTION_KEYWORD)
                    detail['供应总量_增量'] = new_construction if new_construction else ''

                    stock = self.get_part_area(content_except_table, STOCK_KEYWORD)
                    detail['供应总量_存量'] = stock if stock else ''

                    commercial_service = self.get_part_area(content_except_table, COMMERCIAL_SERVICE_KEYWORD)
                    detail['商服用地'] = commercial_service if commercial_service else ''

                    industrial_mining = self.get_part_area(content_except_table, INDUSTRIAL_MINING_KEYWORD)
                    detail['工矿仓储用地'] = industrial_mining if industrial_mining else ''

                    residence = self.get_part_area(content_except_table, RESIDENCE_KEYWORD)
                    detail['住宅用地'] = residence if residence else ''

                    indemnificatory_housing = self.get_part_area(content_except_table, INDEMNIFICATORY_HOUSING_KEYWORD)
                    detail['住宅用地_保障性住房用地'] = indemnificatory_housing if indemnificatory_housing else ''

                    small_medium_housing = self.get_part_area(content_except_table, SMALL_MEDIUM_HOUSING_KEYWORD)
                    detail['住宅用地_中小套型商品房用地'] = small_medium_housing if small_medium_housing else ''

                    other_housing = self.get_part_area(content_except_table, OTHER_HOUSING_KEYWORD)
                    detail['住宅用地_普通商品房用地'] = other_housing if other_housing else ''

                    shanty_town_housing = self.get_part_area(content_except_table, SHANTY_TOWN_HOUSING_KEYWORD)
                    detail['住宅用地_棚户区改造住房用地'] = shanty_town_housing if shanty_town_housing else ''

                    transportation = self.get_part_area(content_except_table, TRANSPORTATION_KEYWORD)
                    detail['交通运输用地'] = transportation if transportation else ''

                    public_administration_service = self.get_part_area(content_except_table, PUBLIC_ADMINISTRATION_SERVICE_KEYWORD)
                    detail['公共管理与公共服务用地'] = public_administration_service if public_administration_service else ''

                    water_area_conservancy = self.get_part_area(content_except_table, WATER_AREA_CONSERVANCY_KEYWORD)
                    detail['水域及水利设施用地'] = water_area_conservancy if water_area_conservancy else ''

                    eduction = self.get_part_area(content_except_table, EDUCATION_KEYWORD)
                    detail['教育用地'] = eduction if eduction else ''

                    industrial = self.get_part_area(content_except_table, INDUSTRIAL_KEYWORD)
                    detail['工业用地'] = industrial if industrial else ''

                    public_welfare = self.get_part_area(content_except_table, PUBLIC_WELFARE_KEYWORD)
                    detail['公益事业用地'] = public_welfare if public_welfare else ''

                    special = self.get_part_area(content_except_table, SPECIAL_KEYWORD)
                    detail['特殊用地'] = special if special else ''

                    other = self.get_part_area(content_except_table, OTHER_KEYWORD)
                    detail['其他用地'] = other if other else ''

                    detail['主键MD5'] = self.to_md5(url)
                    detail['爬取时间'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(time.time())))

                    # print(detail)
                    self.to_csv(detail)
                    # self.collection.insert_one(detail)

            except (ReadTimeout, ConnectionError) as e:
                failed_time += 1

                if failed_time <= 5:
                    # print(f'失败次数{failed_time}，重试', e.args)
                    self.parse_detail(index, failed_time)
                else:
                    # print(f'失败次数超过5次，放弃请求')
                    pass

    def download_woff(self, url, html):
        try:
            woff = re.search(r"url\('(.*?\.woff\?fdipzone)'\) format", html)
            if woff:
                font_url = urljoin(url, woff.group(1))
                font_filename = re.search(r'fonts/(.*?)\?fdipzone', font_url).group(1)

                if not os.path.exists(font_filename):
                    # print('下载字体文件', font_url)
                    response = requests.get(font_url, headers=self.headers, timeout=10)
                    if response.status_code == 200:
                        with open(font_filename, 'wb') as f:
                            f.write(response.content)
                        # time.sleep(2)
                        return font_filename
                else:
                    # print('字体文件已存在', font_filename)
                    pass
            else:
                # print('找不到字体文件url')
                pass
        except (ReadTimeout, ConnectionError) as e:
            # print('连接异常，重试', e.args)
            self.download_woff(url, html)

    @staticmethod
    def parse_font(text, font_filename):
        """字体替换逻辑"""
        new_text = ''
        font = TTFont(font_filename)
        for each in text:
            each_unicode = each.encode('unicode_escape').decode('utf8').upper().replace(r'\U', 'uni')
            glyf = font['glyf'].glyphs.get(each_unicode)
            if glyf:
                content = glyf.data
                each_md5 = md5(content).hexdigest()
                for key, value in font_md5_list_dict.items():
                    if each_md5 == value:
                        each = key
            new_text += each

        os.remove(font_filename)
        return new_text

    @staticmethod
    def extract_sentence(text, keyword):
        """
        提取关键词所在片段（，或。）
        """
        sen_list = re.findall(r'.*?[,，;；。]', text)
        if sen_list:
            for sen in sen_list:
                if keyword in sen:
                    yield sen

    def get_supply_gross(self, text):
        for keyword in SUPPLY_GROSS_KEYWORD:
            for sen in self.extract_sentence(text, keyword):
                pattern = keyword + r'.*?(\d+(\.\d+)?\s?(公顷|亩|平方)(以内|以上)?)'
                matching = re.search(pattern, sen)
                if matching:
                    return re.sub(r'\s', '', matching.group(1))

    def get_part_area(self, text, keyword_list):
        for keyword in keyword_list:
            for sen in self.extract_sentence(text, keyword):
                pattern = keyword + r'.*?(\d+(\.\d+)?\s?(公顷|亩|平方)(以内|以上)?)'
                matching = re.search(pattern, sen)
                if matching:
                    return re.sub(r'\s', '', matching.group(1))

    def to_csv(self,datas):
        """
        存储csv文件逻辑
        :param data:
        :return:
        """
        import csv
        if not os.path.exists('./中国土地市场-供地计划.csv'):
            names = [name for name in datas.keys()]
            with open(f'./中国土地市场-供地计划.csv', 'a', newline='') as f:
                writer = csv.writer(f)
                if isinstance(names, list):
                    # 单行存储
                    if names:
                        writer.writerow(names)
                        f.close()
        # 存数据
        data = [i for i in datas.values()]
        try:
            with open(f'./中国土地市场-供地计划.csv', 'a', newline='') as f:
                writer = csv.writer(f)
                if isinstance(data, list):
                    # 单行存储
                    if data:
                        writer.writerow(data)
                        f.close()
                        return True
                    else:
                        return False
                else:
                    # print(type(data))
                    return False
        except Exception as e:
            raise e.args

    def to_md5(self, txt):
        import hashlib
        m = hashlib.md5()
        m.update(txt.encode())
        return m.hexdigest()

    def run(self,city_code,city_name):
        for page in range(1, 5):
            for index in self.parse_index(page,city_code,city_name):
                self.parse_detail(index)
                time.sleep(1)

if __name__ == '__main__':
    landchina = LandChina()
    landchina.run(city_code='11',city_name='北京市')