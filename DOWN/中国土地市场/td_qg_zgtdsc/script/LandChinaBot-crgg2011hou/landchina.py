import binascii
import requests_html
from urllib.parse import unquote
from win32api import GetSystemMetrics
from requests_html import AsyncHTMLSession
import re
from parseFont import replace_content
import os,time
from scrapy.selector import Selector
import requests


class LandChinaBot:
    info_all = []
    url = 'https://www.landchina.com/'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36',
        # 'Cookie': 'security_session_mid_verify=b92679e3c892fc921cb78030f1e86157',
        'Cookie':f'Hm_lvt_83853859c7247c5b03b527894622d3fa=1587194605,1587302414,1587386859,1587430790; ASP.NET_SessionId=uamiaoeymyazhuxclijuqxpm; security_session_verify=b85755a9c9b76a4d6195095ceaf8620d; security_session_high_verify=f06c25fa45c4419e26e53b51dff6f097; security_session_mid_verify=53623062e0ea9dd09f49fe550795e88a; Hm_lpvt_83853859c7247c5b03b527894622d3fa={int(time.time())}'
    }
    data = None

    def __init__(self, city_code, city_name):
        self.city_name = city_name
        self.getCityInfo(city_code, city_name)
        self.async_session = AsyncHTMLSession()

    def getCityInfo(self, city_code, city_name):
        # 894e12d9-6b0f-46a2-b053-73c49d2f706d：出让公告2011后
        # 894e12d9-6b0f-46a2-b053-73c49d2f706d
        city_info = unquote(f'894e12d9-6b0f-46a2-b053-73c49d2f706d:{city_code}' + u"▓~" + city_name)
        city_info = city_info.encode("gb18030")
        self.data = {
            'TAB_QuerySubmitConditionData': city_info,
        }
    def to_csv(self,datas):
        """
        存储csv文件逻辑
        :param data:
        :return:
        """
        import csv
        if not os.path.exists('./中国土地市场-出让公告2011后.csv'):
            names = [name for name in datas.keys()]
            with open(f'./中国土地市场-出让公告2011后.csv', 'a', newline='') as f:
                writer = csv.writer(f)
                if isinstance(names, list):
                    # 单行存储
                    if names:
                        writer.writerow(names)
                        f.close()
        # 存数据
        data = [i for i in datas.values()]
        try:
            with open(f'./中国土地市场-出让公告2011后.csv', 'a', newline='') as f:
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

    @staticmethod
    def stringToHex():
        width = str(GetSystemMetrics(0))
        height = str(GetSystemMetrics(1))
        screendate = width + "," + height
        val = ""
        for i in range(len(screendate)):
            if val == "":
                val = binascii.b2a_hex(screendate[i].encode('utf-8'))
            else:
                val += binascii.b2a_hex(screendate[i].encode('utf-8'))
        return val.decode('utf-8')

    async def getCookie(self):
        response = await self.async_session.get(self.url, headers=self.headers)
        security_verify_data = self.stringToHex()
        link = f'{self.url}?security_verify_data={security_verify_data}'
        response = await self.async_session.get(link, headers=self.headers)

    # print(self.async_session.cookies)

    async def getInfo(self, session):
        # detail_link = []
        # link = f'{self.url}default.aspx?tabid=263'
        link = f'{self.url}default.aspx?tabid=261'
        for page in range(1, 201):
            self.data['TAB_QuerySubmitPagerData'] = str(page)
            # print(self.data)
            try:
                response = requests.post(link, data=self.data, headers=self.headers)
                # print(response.content.decode('gbk'))

                info = Selector(text=response.content.decode('gbk')).xpath('//*[@id="TAB_contentTable"]/tbody/tr')
                # info = response.html.xpath('//*[@id="TAB_contentTable"]/tbody/tr')
                for sub_raw in info[1:]:
                    info_basic = {}
                    basic_value = []
                    sub_list = sub_raw.xpath('td')
                    for i, info_sub in enumerate(sub_list):
                        if i == 0:
                            info_sub = info_sub.xpath('text()').extract()[0][:-1]
                            # print(info_sub, end=' ')
                            basic_value.append(info_sub)
                        elif i > 0 and i != 2:
                            info_sub = info_sub.xpath('text()').extract()[0]
                            # print(info_sub, end=' ')
                            basic_value.append(info_sub)
                        else:
                            link_sub = info_sub.xpath('a/@href').extract()[0]
                            # detail_link.append(link_sub)
                            try:
                                info_sub = info_sub.xpath('a/text()').extract()[0]
                            except IndexError:
                                info_sub = info_sub.xpath('a/span/@title').extract()[0]
                            # print(info_sub, end=' ')
                            basic_value.append(info_sub)
                    # print('\n')
                    details = await self.getDetail(link_sub, self.async_session)

                    info_basic['城市'] = self.city_name
                    # info_basic['序号'] = basic_value[0][:-1]
                    info_basic['行政区'] = basic_value[1]

                    info_basic['供应公告标题'] = basic_value[2]
                    info_basic['公告编号'] = ''.join(re.findall(r'[\s|\S]*(\([\s|\S]*\))',str(basic_value[2])))

                    info_basic['公告类型'] = basic_value[3]
                    info_basic['发布时间'] = basic_value[4]
                    info_basic['网上创建时间'] = basic_value[5]



                    # info_basic['地块公示信息'] = details
                    for det in details:
                        info_ba = {}
                        info_ba = {**info_basic, **det}
                        all_data = {'城市':'','行政区':'','供应公告标题':'','公告编号':'','公告类型':'','发布时间':'','网上创建时间':'',
                                    '宗地编号：': '', '宗地总面积：': '', '宗地坐落：': '', '出让年限：': '', '容积率：': '', '建筑密度(%)：': '',
                                    '绿化率(%)：': '', '建筑限高(米)：': '', '主要用途：': '', '明细用途': '', '用途名称': '', '面积': '',
                                    '投资强度：': '', '保证金：': '', '估价报告备案号': '', '起始价：': '', '加价幅度：': '', '挂牌开始时间：': '',
                                    '挂牌截止时间：': '', '受让单位': '', '地块位置': '', '土地用途': '', '成交价(万元)': '', '土地面积(公顷)': '',
                                    '提交书面申请地': '', '缴纳竞买保证金截止时间': '', '确认竞买资格时间': '', '拍卖开始时间': '', '拍卖挂牌进行地点': '',
                                    '联系地址': '', '联系人': '', '联系电话': '', '开户单位': '', '开户银行': '', '银行账号': ''}
                        info_all = {**all_data, **info_ba}
                        # print(info_all)
                        self.to_csv(info_all)
                        # self.info_all.append(info_all)
            except Exception as e:
                continue

        # return detail_link

    async def getDetail(self, link, session):
        link = f'{self.url}{link}'
        # print(link)
        # link = 'https://www.landchina.com//DesktopModule/BizframeExtendMdl/workList/bulWorkView.aspx?wmguid=20aae8dc-4a0c-4af5-aedf-cc153eb6efdf&recorderguid=4eff5dbf-6bce-4cef-a3a8-61b51cd4dc21&sitePath='

        # response = await session.get(link, headers=self.headers)
        response = requests.get(link, headers=self.headers)
        # print(response.content.decode('gb18030'))

        ttf_url = re.findall(r"truetype[\s|\S]*styles/fonts/([\s|\S]*?)'[\s|\S]*woff'\)",response.content.decode('gb18030'))[0]
        # print(ttf_url)

        # ttf_content = await session.get(f'{self.url}/styles/fonts/{ttf_url}', headers=self.headers)
        ttf_content = requests.get(f'{self.url}/styles/fonts/{ttf_url}', headers=self.headers)
        new_font_name = f"{link.split('recorderguid=')[1]}.ttf"
        with open(new_font_name, 'wb') as f:
            f.write(ttf_content.content)

        info_text_all = Selector(text=response.content.decode('gb18030')).xpath('//*[@id="tdContent"]//td/div')
        other_text = Selector(text=response.content.decode('gb18030')).xpath('//*[@id="tdContent"]//td/p//text()').extract()
        # info = ''.join([str(ir).replace('\r\n','').replace(' ','') for ir in info_text])
        bg_all = []
        for info_t in info_text_all:
            info_text = info_t.xpath('table//text()').extract()
            info_temp = '#'.join(list(filter(None, [str(ir).replace(' ', '').replace('\t', '') for ir in info_text])))
            bg_all.append(info_temp)
        info = '$$$$'.join(bg_all)
        other = list(filter(None, [str(ot).replace(' ', '').replace('\t', '') for ot in other_text]))
        # info = [str(ir).replace(' ','').replace('\t','') for ir in info_text]
        # info = '#'.join(info)
        other = '&'.join(other)
        # 替换繁体字
        info_all = replace_content(f'{info}****{other}',link.split('recorderguid=')[1])

        if not info_all:
            return False
        # print(info_all)
        names = ['宗地编号：', '宗地总面积：', '宗地坐落：', '出让年限：', '容积率：', '建筑密度(%)：',
                 '绿化率(%)：', '建筑限高(米)：', '主要用途：', '明细用途', '用途名称', '面积',
                 '投资强度：', '保证金：', '估价报告备案号', '起始价：', '加价幅度：', '挂牌开始时间：',
                 '挂牌截止时间：', '受让单位', '地块位置', '土地用途', '成交价(万元)', '土地面积(公顷)',
                 '提交书面申请地', '缴纳竞买保证金截止时间', '确认竞买资格时间', '拍卖开始时间', '拍卖挂牌进行地点',
                 '联系地址', '联系人', '联系电话', '开户单位', '开户银行', '银行账号']

        keys = ['宗地编号', '宗地总面积', '宗地坐落', '出让年限', '容积率',
                '建筑密度', '绿化率', '建筑限高', '主要用途', '明细用途', '面积',
                '投资强度', '保证金', '估价报告备案号', '起始价', '加价幅度',
                '挂牌开始时间', '挂牌截止时间', '受让单位', '地块位置', '土地用途',
                '成交价', '用途名称', '土地面积', '提交书面申请地', '缴纳竞买保证金截止时间',
                '确认竞买资格时间', '拍卖开始时间', '拍卖挂牌进行地点',
                '联系地址', '联系人', '联系电话', '开户单位', '开户银行', '银行账号']

        info = info_all.split('****')[0]  # 表格信息
        other = info_all.split('****')[1]  # 其他内容信息

        infos = list(filter(None, info.split('$$$$')))
        # infos = info.split('$$$$')
        # infos_all = []

        # 多表格解析逻辑
        content_all = []
        for info in infos:
            result_info = info.split('#')
            # print(result_info)
            # infos_all.append(result_info)
            # 添加一个空元素，防止后面解析报错
            result_info.append('')

            # 解析表格逻辑
            content_dict = dict()
            for i, inf in enumerate(result_info):
                if inf in names:
                    if result_info[i + 1] in names and inf not in ['用途名称', '面积']:
                        content_dict[inf] = ''
                    elif inf in ['用途名称', '面积']:
                        if inf == '用途名称':
                            yt = re.findall(r'用途名称#面积#([\u4E00-\u9FA5]+).*?#投资强度', info)
                            if yt:
                                content_dict[inf] = yt[0]
                            else:
                                content_dict[inf] = ''
                        else:
                            mj = re.findall(r'用途名称#面积#.*?([\d|\.|#]+?)#投资强度', info)
                            if mj:
                                content_dict[inf] = mj[0][1:]
                            else:
                                content_dict[inf] = ''
                    else:
                        content_dict[inf] = str(result_info[i + 1])
                else:
                    pass

            # 非表格信息
            content_dict['提交书面申请地'] = ''.join(re.findall(r'五、申请人可于[\s|\S]*&到&([\s|\S]*?)&向我局提交书面申请', other))
            content_dict['缴纳竞买保证金截止时间'] = ''.join(re.findall(r'竞买保证金的截止时间为&([\s|\S]*?)&', other))
            content_dict['确认竞买资格时间'] = ''.join(re.findall(r'具备申请条件的，我局将在&([\s|\S]*?)&前确认其竞买资格', other))
            try:
                content_dict['拍卖开始时间'] = ''.join(re.findall(r'拍卖活动定于&([\s|\S]*?)&在&', other))
                content_dict['拍卖挂牌进行地点'] = ''.join(re.findall(r'&在&([\s|\S]*?)&进行', other))
            except:
                # &号地块:&2020年05月18土09时30分&至&
                # content_dict['拍卖开始时间'] = ''
                # content_dict['拍卖进行地点'] = ''
                # content_dict['挂牌开始时间'] = ''.join(re.findall(r'&号地块:&([\s|\S]*?)&至&', other))
                # content_dict['挂牌进行地点'] = ''.join(re.findall(r'&在&([\s|\S]*?)&进行', other))
                pass

            content_dict['联系地址'] = ''.join(re.findall(r'联系地址：([\s|\S]*?)&', other))
            content_dict['联系人'] = ''.join(re.findall(r'&联系人：([\s|\S]*?)&', other))
            content_dict['联系电话'] = ''.join(re.findall(r'&联系电话：([\s|\S]*?)&', other))
            content_dict['开户单位'] = ''.join(re.findall(r'&开户单位：([\s|\S]*?)&', other))
            content_dict['开户银行'] = ''.join(re.findall(r'&开户银行：([\s|\S]*?)&', other))
            content_dict['银行账号'] = ''.join(re.findall(r'&银行帐号：([\s|\S]*)', other))

            content_dict['内容链接'] = link
            content_dict['主键MD5'] = self.to_md5(str(link))
            content_dict['爬取时间'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(time.time())))
            content_all.append(content_dict)

            # 每个表格返回一条数据

        return content_all

    async def run(self):
        await self.getCookie()
        await self.getInfo(self.async_session)
        # for info_sub in self.info_all:
        #     print(info_sub, '\n\n')
        #     pass

    def to_md5(self, txt):
        import hashlib
        m = hashlib.md5()
        m.update(txt.encode())
        return m.hexdigest()

    def main(self):
        self.async_session.run(self.run)


if __name__ == '__main__':
    bot = LandChinaBot('31', '上海市')
    bot.main()
