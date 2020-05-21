# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html
import random

from scrapy import signals
from tongcheng58.useragent import agent_list



class RandUserAgentMiddle(object):
    '''
     执行自定义下载
     '''

    def process_request(self, request, spider):
        '''
        下载网页, 只要返回HTTPResponse就不再执行其他下载中间件
        :param request:
        :param spider:
        :return:
        '''
        agent = random.choice(agent_list)
        request.headers['User-Agent'] = agent
        request.headers['referer'] = 'https://cd.58.com/ershoufang/'
        request.headers['accept-encoding'] = 'gzip, deflate, br'
        request.headers['accept-language'] = 'zh-CN,zh;q=0.9'


import base64

# 代理服务器
proxyServer = "http://http-dyn.abuyun.com:9020"

# 代理隧道验证信息
proxyUser = "HS77A38IF6KOOIGD"
proxyPass = "E164688AFDD02A8F"

proxyAuth = "Basic " + base64.urlsafe_b64encode(bytes((proxyUser + ":" + proxyPass), "ascii")).decode("utf8")


class RandIPROXY(object):
    def process_request(self, request, spider):
        request.meta["proxy"] = proxyServer
        request.headers["Proxy-Authorization"] = proxyAuth
