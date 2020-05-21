# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class Tongcheng58Item(scrapy.Item):
    # define the fields for your item here like:
    city = scrapy.Field()
    area = scrapy.Field()
    second_area = scrapy.Field()
    house_title = scrapy.Field()
    house_name = scrapy.Field()
    house_type = scrapy.Field()
    house_floor = scrapy.Field()
    house_price = scrapy.Field()
    house_address = scrapy.Field()
    # 面积
    house_proportion = scrapy.Field()
    # 装修情况
    house_fitment = scrapy.Field()
    # 朝向
    house_orientation = scrapy.Field()
    # 图片
    house_img = scrapy.Field()
    # 描述
    house_describe = scrapy.Field()
    # 年代
    house_year = scrapy.Field()








