# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import pymysql
from tongcheng58.items import Tongcheng58Item
from tongcheng58.settings import *
from tongcheng58.mysql_db_helper import *


class Tongcheng58Pipeline(object):
    def process_item(self, item, spider):
        return item


class TongchengMysqlPipeline(object):
    def __init__(self, host, username, password, database, port):
        self.host = host
        self.username = username
        self.password = password
        self.database = database
        self.port = port

    @classmethod
    def from_crawler(cls, crawler):
        return cls(host=crawler.settings.get('MYSQL_HOST'), username=crawler.settings.get('MYSQL_USERNAME'),
                   password=crawler.settings.get('MYSQL_PASSWORD'), database=crawler.settings.get('MYSQL_DATABASE'),
                   port=crawler.settings.get('MYSQL_PORT'))

    # 当蜘蛛对象生成的时候.创建数据库连接
    def open_spider(self, spider):
        self.db = pymysql.connect(self.host, self.username, self.password, 'school', charset='utf8',
                                  port=self.port)
        self.cursor = self.db.cursor()

    # 当蜘蛛对象销毁的时候, 释放数据库连接
    def close_spider(self, spider):
        self.db.close()

    def process_item(self, item, spider):
        if isinstance(item, Tongcheng58Item):

            try:
                sql = 'insert into second_hand_house (city,area,second_area,house_address,house_title,house_name,house_type, house_floor,house_price,house_proportion,house_fitment,house_orientation,house_img,house_describe,house_year) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                params =(item['city'],
                        item['area'],
                        item['second_area'],
                        item['house_address'],
                        item['house_title'],
                        item['house_name'],
                        item['house_type'],
                        item['house_floor'],
                        item['house_price'],
                        item['house_proportion'],
                        item['house_fitment'],
                        item['house_orientation'],
                        item['house_img'],
                        item['house_describe'],
                        item['house_year'])
                self.cursor.execute(sql, params)
                self.db.commit()
                print('成功')
            except:
                print('插入数据库失败')

        return item


