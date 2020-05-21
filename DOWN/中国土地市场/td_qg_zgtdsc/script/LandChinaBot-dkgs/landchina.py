import base64
import binascii
import time
from urllib.parse import unquote
from win32api import GetSystemMetrics

import requests
from requests_html import AsyncHTMLSession
import re
from parseFont import replace_content
import os


class LandChinaBot:
    info_all = []
    url = 'https://www.landchina.com/'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36',
        # 'Cookie': 'security_session_mid_verify=b92679e3c892fc921cb78030f1e86157',
    }
    #  lzl改成了空字典，避免获取城市编号时报错
    data = {}

    def __init__(self, city_code, city_name):
        self.city_name = city_name
        self.city_code = city_code
        self.async_session = AsyncHTMLSession()

    def check_verify_img(self, img):
        """
        ocr
        :param img:
        :return:
        """
        AK = "ME7Z5koZtVtZ5cnOqPQyB0Nu"
        SK = "PL2lG0eWSNdTpIOnM6lMHH7zcFBXoswd"
        # method = 'PUT'

        # request_url = "https://aip.baidubce.com/rest/2.0/ocr/v1/accurate_basic"
        request_url = "https://aip.baidubce.com/rest/2.0/ocr/v1/general_basic"
        headers = {
            'Content-Type	': "application/x-www-form-urlencoded"
        }

        def get_txt(img, request_url=request_url, times=1):

            img = base64.b64encode(img)
            params = {"image": img,
                      'detect_direction': "true"}

            access_token = "24.23e07be493a0ff5ce1239c63684e7f4c.2592000.1589013469.282335-19343178"
            request_url = request_url + "?access_token=" + access_token
            try:
                response = requests.post(request_url, data=params, headers=headers)
                if response:

                    return ('\n'.join([i['words'] for i in response.json()['words_result']]))

                else:
                    return False
            except Exception as e:

                if times > 3:
                    return False
                time.sleep(3 * times)
                get_txt(img, request_url, 1 + times)

        code = get_txt(img, )
        # print(code)
        return code

    def getCityInfo(self, city_code, city_name, id):
        # response =await self.async_session.get(self.url,headers=self.headers)
        # id = response.html.xpath('//input[@id="queryGroupEnumItem_80_v"]/@qitem')[0]
        # id = '4a611fc4-42b1-4861-ac43-8d25b002dc2b'
        city_info = unquote(f'{id}:{city_code}' + u"▓~" + city_name)
        city_info = city_info.encode("gb18030")
        self.data = {
            'TAB_QuerySubmitConditionData': city_info,
        }

    def to_csv(self, datas):
        """
        存储csv文件逻辑
        :param data:
        :return:
        """
        import csv
        if not os.path.exists('./中国土地市场-地块公示.csv'):
            names = [name for name in datas.keys()]
            with open(f'./中国土地市场-地块公示.csv', 'a', newline='') as f:
                writer = csv.writer(f)
                if isinstance(names, list):
                    # 单行存储
                    if names:
                        writer.writerow(names)
                        f.close()
        # 存数据
        data = [i for i in datas.values()]
        try:
            with open(f'./中国土地市场-地块公示.csv', 'a', newline='') as f:
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

        link = f'{self.url}default.aspx?tabid=262'
        # print(link)

        #  将for改成了while
        # while True:
        for page in range(1, 201):
            self.data['TAB_QuerySubmitPagerData'] = str(page)

            #  lzl修改了TAB_QuerySubmitConditionData参数   id每天会更新
            if not self.data.get('TAB_QuerySubmitConditionData'):
                response = await session.post(link, data=self.data, headers=self.headers)
                id = response.html.xpath('//input[@id="TAB_QueryConditionItem80"]/@value')[0]
                self.getCityInfo(self.city_code, self.city_name, id)

            # print(self.data)
            try:

                response = await session.post(link, data=self.data, headers=self.headers)

                verifyimg = response.html.xpath('//img[@class="verifyimg"]')
                #  lzl 增加了图片验证码
                if len(verifyimg):
                    img = verifyimg[0].xpath('./@src')[0].split(',')[-1]

                    code = self.check_verify_img(img)
                    # print(img)
                    response = await session.post(
                        link + '&security_verify_img=3' + '3'.join(list(code.replace(' ', '').replace('\n', ''))),
                        data=self.data, headers=self.headers)
                info = response.html.xpath('//*[@id="TAB_contentTable"]/tbody/tr')

                for sub_raw in info[1:]:
                    info_basic = {}
                    basic_value = []
                    sub_list = sub_raw.xpath('//td')

                    for i, info_sub in enumerate(sub_list):
                        if i != 2:
                            info_sub = info_sub.xpath('//text()')[0]
                            # print(info_sub, end=' ')
                            basic_value.append(info_sub)
                        else:
                            link_sub = info_sub.xpath('//a/@href')[0]
                            # detail_link.append(link_sub)
                            try:
                                info_sub = info_sub.xpath('//a/text()')[0]
                            except IndexError:
                                info_sub = info_sub.xpath('//a/span/@title')[0]
                            # print(info_sub, end=' ')
                            basic_value.append(info_sub)
                    # print('\n')
                    details = await self.getDetail(link_sub, self.async_session)

                    info_basic['城市'] = self.city_name
                    info_basic['序号'] = basic_value[0][:-1]
                    info_basic['行政区'] = basic_value[1]

                    info_basic['供应公告标题'] = basic_value[2]
                    # info_basic['公告类型'] = basic_value[3]
                    info_basic['公示发布日期'] = basic_value[3]
                    # info_basic['网上创建时间'] = basic_value[5]

                    # info_basic['地块公示信息'] = details

                    # ['宗地编号', '地块位置', '土地用途', '土地面积(公顷)', '出让年限', '成交价(万元)', '项目名称',
                    #  '用途名称', '面积', '受让单位', '备注',
                    #  ]
                    all_data = {'宗地编号': '', '地块位置': '', '土地用途': '', '土地面积(公顷)': '', '出让年限': '', '成交价(万元)': '',
                                '项目名称': '', '用途名称': '', '面积': '', '受让单位': '',
                                '备注：': '', '公示期开始时间': '', '公示期结束时间': '', '联系单位': '', '单位地址': '', '邮政编码': '',
                                '联系电话': '', '电子邮箱': '', }
                    # info_basic = {**info_basic, **details}

                    #  lzl处理一对多的形式
                    for i in details:
                        detail_basic = {**info_basic, **i}

                        info_all = {**all_data, **detail_basic}
                        # print(info_all)
                        self.to_csv(info_all)
                    # self.info_all.append(info_all)

                if len(info) < 30:
                    break
                else:
                    # page += 1
                    continue
            except Exception as e:
                # print('error', e)
                continue

        # return detail_link

    async def getDetail(self, link, session):

        link = f'{self.url}{link}'
        # link = 'http://www.landchina.com/DesktopModule/BizframeExtendMdl/workList/bulWorkView.aspx?wmguid=6506735a-142d-4bde-9674-121656045ed1&recorderguid=862483fc-ec0b-47c0-8c7b-40ee91956f23&sitePath='
        # print(link)
        response = await session.get(link, headers=self.headers)
        # print(response.content.decode('gb18030'))

        from scrapy.selector import Selector
        ttf_url = \
            re.findall(r"truetype[\s|\S]*styles/fonts/([\s|\S]*?)'[\s|\S]*woff'\)", response.content.decode('gb18030'))[
                0]
        # print(ttf_url)
        ttf_content = await session.get(f'{self.url}/styles/fonts/{ttf_url}', headers=self.headers)
        # new_font_name = "font_china_new.ttf"
        new_font_name = f"{link.split('recorderguid=')[1].split('&')[0]}.ttf"
        with open(new_font_name, 'wb') as f:
            f.write(ttf_content.content)
        #  lzl改成了获取多个表格
        info_text = Selector(text=response.content.decode('gb18030')).xpath('//*[@id="tdContent"]//td/table')
        info_text = [
            list(filter(None, [str(ir).replace(' ', '').replace('\t', '') for ir in j.xpath(".//text()").extract()]))
            for j in info_text]
        #  表格拼接
        info = '===='.join(['#'.join(i) for i in info_text])
        other_text = Selector(text=response.content.decode('gb18030')).xpath(
            '//*[@id="tdContent"]//td/p//text()').extract()

        other = list(filter(None, [str(ot).replace(' ', '').replace('\t', '') for ot in other_text]))

        other = '&'.join(other)
        info_all = replace_content(f'{info}****{other}', new_font_name.split('.')[0])
        # print('简体： ',info[:100])
        # print('******************************************')
        content_dict = dict()
        if not info_all:
            return False
        # print(info_all)
        names = ['宗地编号', '地块位置', '土地用途', '土地面积(公顷)', '出让年限', '成交价(万元)', '项目名称',
                 '用途名称', '面积', '受让单位', '备注：',
                 ]

        keys = ['宗地编号', '地块位置', '土地用途', '土地面积(公顷)', '项目名称',
                '用途名称', '面积', '受让单位', '备注：',
                ]

        other_key = ['公示期开始时间', '公示期结束时间', '联系单位',
                     '单位地址', '邮政编码', '联系电话', '联系人', '电子邮件', '']

        other = info_all.split('****')[1]  # 其他内容信息
        # print(info)
        # print(other)
        # 添加一个空元素，防止后面解析报错

        # 解析表格逻辑
        result_other = other.replace("&", '')
        for i, inf in enumerate(other_key):
            if i + 1 == len(other_key):
                continue
            if inf in ['公示期开始时间', '公示期结束时间']:
                s = re.findall('公示期[:：]?(.*?)三', result_other)
                if len(s):
                    s = s[0].replace("土", '日').replace("士", '日')
                    content_dict['公示期开始时间'], content_dict['公示期结束时间'] = s.split("至")
                else:
                    content_dict['公示期开始时间'] = ''
                    content_dict['公示期结束时间'] = ''

            else:
                try:
                    content_dict[inf] = re.findall(inf + "[:：]?(.*?)" + other_key[i + 1], result_other)[0]
                except:
                    content_dict[inf] = ''
        content_dict['内容链接'] = link
        content_dict['主键MD5'] = self.to_md5(str(link))
        content_dict['爬取时间'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(time.time())))

        infos = info_all.split('****')[0]  # 表格信息
        # print(infos)
        info_list = infos.split("====")
        out_list = []
        for info in info_list:
            result_info = info.split('#')
            # print(info)
            con = dict()
            result_info.append('')
            for i, inf in enumerate(result_info):
                if inf in names:
                    if result_info[i + 1] in names and inf not in ['用途名称', '面积']:
                        con[inf] = ''
                    elif inf in ['用途名称', '面积']:
                        if inf == '用途名称':
                            yt = re.findall(r'用途名称#面积#([\u4E00-\u9FA5]+).*?#', info)
                            if yt:
                                con[inf] = yt[0]
                            else:
                                con[inf] = ''
                        else:
                            mj = re.findall(r'用途名称#面积#.*?#(\d+\.?\d*)#', info)
                            if mj:
                                con[inf] = mj[0]
                            else:
                                con[inf] = ''
                    elif inf in ['项目名称']:
                        s = re.findall(r'项目名称[:：]?#(.*?)#明细用途', info)
                        if len(s):
                            con['项目名称'] = s[0]
                        else:
                            con['项目名称'] = ''
                    else:
                        con[inf] = str(result_info[i + 1])
                else:
                    pass

            out_list.append({**content_dict, **con})
        # print(out_list)

        return out_list

    def to_md5(self, txt):
        import hashlib
        m = hashlib.md5()
        m.update(txt.encode())
        return m.hexdigest()

    async def run(self):
        await self.getCookie()
        await self.getInfo(self.async_session)
        # for info_sub in self.info_all:
        #     print(info_sub, '\n\n')
        #     pass

    def main(self):
        self.async_session.run(self.run)


if __name__ == '__main__':
    bot = LandChinaBot('31', '上海市')
    bot.main()
    # print(bot.stringToHex())
