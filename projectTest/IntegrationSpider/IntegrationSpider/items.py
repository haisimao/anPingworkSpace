# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class IntegrationPageItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    REGIONALLISM = scrapy.Field()  # 行政区
    TITLE = scrapy.Field()         # 标题
    PUBLISHTIME = scrapy.Field()   # 发布时间
    TYPE = scrapy.Field()          # 类型