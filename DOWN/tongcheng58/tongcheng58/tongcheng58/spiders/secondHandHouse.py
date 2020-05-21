# -*- coding: utf-8 -*-
import pymysql, random
import scrapy, time, requests
from urllib.parse import urlencode
from lxml import etree
from pydispatch import dispatcher
from scrapy import signals
from tongcheng58.useragent import agent_list



from tongcheng58.items import *


class ErshouSpider(scrapy.Spider):
    name = 'secondHandHouse'
    allowed_domains = ['www.58.com/ershoufang/changecity']
    start_urls = ['http://www.58.com/ershoufang/changecity/']

    def __init__(self):
        super(ErshouSpider, self).__init__()
        self.connection = pymysql.connect(
            host='127.0.0.1',
            port=3306,
            user='root',
            password='123456',
            database='school',
            use_unicode=True,
            charset='utf8mb4',
            autocommit=True,
            cursorclass=pymysql.cursors.DictCursor,
        )
        dispatcher.connect(self.CloseSpider, signals.spider_closed)
        self.headers = {'User-Agent': random.choice(agent_list)}

    def CloseSpider(self):
        '''
        关闭spider时
        :return:
        '''
        print('爬虫正常关闭')

    def start_requests(self):
        '''
         按照优先级爬取
        '''
        try:

            yield scrapy.Request('http://www.58.com/ershoufang/changecity/', method='GET', callback=self.parse,
                              meta={},
                              # body=requests_data, headers={'Content-Type': 'application/json'}
                              dont_filter=True,
                              )

        except Exception as e:
            self.log('当前爬取失败页数')

    def parse(self, response):
        '''
        获取城市列表
        :param response:
        :return:
        '''
        etreeHtml = etree.HTML(response.text)
        # 城市名
        city_name = etreeHtml.xpath('//dl[@id="clist"]/dd/a/text()')
        # 城市url
        city_url = etreeHtml.xpath('//dl[@id="clist"]/dd/a/@href')

        for index in city_url:
            detail_url = index.split('/e')[0]
            # print(detail_url)
            yield scrapy.Request(url='https:' + index, callback=self.city_parse,
                                 meta={'detail_url': 'https:' + detail_url},
                                 dont_filter=True)

    def city_parse(self, response):
        '''
        获取城市下面各区域
        :param response:
        :return:
        '''
        city_html = etree.HTML(response.text)
        items = city_html.xpath('//div[@id="qySelectFirst"]/a')
        detail_url = response.request.meta.get('detail_url')
        for item in items:
            area_name = item.xpath('text()')[0]
            area_url = item.xpath('@href')[0]
            if area_name != '不限':
                yield scrapy.Request(url=detail_url + area_url, callback=self.area_parse,
                                     meta={'detail_url': detail_url}, dont_filter=True)

    def area_parse(self, response):
        '''
        获取区域下各地区url
        :param response:
        :return:
        '''
        area_html = etree.HTML(response.text)
        # second_area_name = etree_html.xpath('//div[@id="qySelectSecond"]/a/text()')  # 有空格
        second_area_url = area_html.xpath('//div[@id="qySelectSecond"]/a/@href')
        detail_url = response.request.meta.get('detail_url')
        for index in second_area_url:
            yield scrapy.Request(url=detail_url + index, callback=self.area_page, meta={'url': detail_url + index, 'detail_url': detail_url}, dont_filter=True)

    def area_page(self, response):
        '''
        获取各地区的房源页数
        :param response:
        :return:
        '''
        page_html = etree.HTML(response.text)
        second_area_url = response.request.meta.get('url')
        house_url = page_html.xpath('//h2[@class="title"]/a/@href')
        for i,index in enumerate(house_url):
            if 'https:' not in index:
                house_url[i] = 'https:' + index
        house_price = page_html.xpath('//p[@class="sum"]//text()')
        # house_page = etree_html.xpath('//div[@class="pager"]/string/span/text()')
        house_page_next = page_html.xpath('//div[@class="pager"]/a/text()')
        house_prices = []
        for i in range(0, len(house_price), 2):
            house_prices.append(house_price[i] + house_price[i + 1])

        if len(page_html.xpath('//p[@class="noresult-tip noresult-tip2"]/text()')) != 1:
            if len(house_page_next) == 0:
                for i in range(len(house_url)):
                    print('有房源')
                    yield scrapy.Request(url=house_url[i], callback=self.house_message,
                                         meta={'house_price': house_prices[i]}, dont_filter=True)
            else:
                for i in range(1, len(house_page_next) + 1):
                    print('有房源')
                    page_url = second_area_url + '/pn' + str(i)
                    yield scrapy.Request(url=page_url, callback=self.house_page_url, dont_filter=True)

    def house_page_url(self, response):
        '''
        获取每页房源url
        :param response:
        :return:
        '''
        house_html = etree.HTML(response.text)
        house_url = house_html.xpath('//h2[@class="title"]/a/@href')
        house_price = house_html.xpath('//p[@class="sum"]//text()')
        house_prices = []
        for i in range(0, len(house_price), 2):
            house_prices.append(house_price[i] + house_price[i + 1])
        for j in range(len(house_prices)):
            yield scrapy.Request(url=house_url[j], callback=self.house_message, meta={'house_price': house_prices[j]},
                                 dont_filter=True)

    def house_message(self, response):
        '''
        获取房源对应信息
        :param response:
        :return:
        '''
        house_message_html = etree.HTML(response.text)
        print(response.url)
        try:
            city = house_message_html.xpath('//div[@class="nav-top-bar fl c_888 f12"]/a[1]/text()')[0][:-5]
            position = house_message_html.xpath('//ul[@class="house-basic-item3"]/li/span[@class="c_000 mr_10"]/a/text()')
            area = position[2].strip() if len(position) == 4 else '无'
            second_area = position[1].strip() if len(position) == 4 else '无'
            house_address = position[3].strip().replace(' ','')[1:] if len(position) == 4 else '无'
            house_name = position[0].strip() if len(position) == 4 else '无'
            house_price = response.request.meta.get('house_price')
            house_title = house_message_html.xpath('//h1[@class="c_333 f20"]/text()')[0]
            house_describe = \
            house_message_html.xpath('//div[@class="general-item-wrap"]/div[1]/p[@class="pic-desc-word"]/text()')[0]
            house_img = house_message_html.xpath('//ul[@id="leftImg"]/li/@data-value')
            basic_information = house_message_html.xpath('//div[@id="generalSituation"]//span[@class="c_000"]/text()')
            house_type = basic_information[0]
            house_proportion = basic_information[1]
            house_orientation = basic_information[2]
            house_floor = basic_information[4]
            house_fitment = basic_information[5]
            house_year = basic_information[7] if len(basic_information) == 8 else '无'

            item = Tongcheng58Item()
            item['city'] = city
            item['area'] = area
            item['second_area'] = second_area
            item['house_address'] = house_address
            item['house_price'] = house_price
            item['house_title'] = house_title
            item['house_name'] = house_name
            item['house_describe'] = house_describe
            item['house_img'] = str(house_img)
            item['house_type'] = house_type
            item['house_proportion'] = house_proportion
            item['house_orientation'] = house_orientation
            item['house_floor'] = house_floor
            item['house_fitment'] = house_fitment
            item['house_year'] = house_year
            yield item
        except:
            print('错误的url为' + response.url)
