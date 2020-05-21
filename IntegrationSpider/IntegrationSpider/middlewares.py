# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/spider-middleware.html
import json
import random
import requests

from scrapy import signals
from scrapy.http import HtmlResponse

from IntegrationSpider import useragent
from IntegrationSpider.settings import  ENCRYPTION_ENTITY, ENCRYPTION_BY_HTML_ENTITY, ENCRYPTION_BY_API_AND_NO_IP_ENTITY, \
    ENCRYPTION_BY_HTML_FIREFOX, ENCRYPTION_BY_API_FIREFOX

from SpiderTools.JsEncrypt import GetJsEncryptPage, GetPageHtml, FirefoxGetPage
from SpiderTools.Tool import wash_url
# from requests_html import HTMLSession, HTML


class IntegrationspiderSpiderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, dict or Item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Response, dict
        # or Item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class IntegrationspiderDownloaderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class DLMiddleware(object):
    '''
    执行自定义下载
    '''
    def process_request(self, request, spider):
        '''
        下载网页, 只要返回HTTPResponse就不再执行其他下载中间件
        :param request: scrapy整合的全局参数
        :param spider: spiders里的爬虫对象
        :return:
        '''

            # 修改部分spider的延迟时间
        if spider.name in ['JRCP_JJ_GFYH_GW_ALL', 'JRCP_JJ_BHYH_GW_ALL', 'ZX_ZCGG_YBH_JGDT', 'ZX_ZCGG_ZGRMYH_TFS', 'ZX_ZCGG_YBH_GGTZ']:
            request.meta['download_timeout'] = 6000
            request.meta.setdefault('download_delay', 5)
            agent = random.choice(useragent)
            request.headers['User_Agent'] = agent


class DownLoadsMiddleware(object):

    def __init__(self):
        # self.Session_ = HTMLSession()  # rederSession
        self.session = requests.Session()
        # from SpidersLog.ICrwlerLog import ICrawlerLog
        # self.log = ICrawlerLog('spider').save
        try:
            self.ip = ''
        except Exception as e:
            # self.log.error(e)
            print(6)
            raise e
        self.proxies = {
            "http": 'http://' + self.ip,
            "https": 'https://' + self.ip,
        }

    '''
    执行自定义下载
    '''
    def process_request(self, request, spider):
        '''
        下载网页,
        :param request: scrapy整合的全局参数
        :param spider: spiders里的爬虫对象
        :return:
        '''
        # 使用pyppeteer获取数据
        header = {'User-Agent': "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36"}
        if spider.name in ENCRYPTION_ENTITY:
            try:
                # 修改这个 url 为 Referer 中的url
                try:
                    JS_url = (request.headers.get('Referer') if request.headers.get('Referer') else request.url).decode('utf-8')
                except:
                    JS_url = wash_url(request.url)
                cookies, IP, UA = GetJsEncryptPage().run(JS_url, spider.name)
                try:
                    from urllib.parse import quote
                    data_ = request.meta.get('parm').copy()
                    for key in data_.keys():
                        data_[key] = quote(data_[key])
                except:
                    data_ = ''
                try:
                    data_.pop('SALE_SOURCE_')  # 获取请求参数
                except Exception as e:
                    try:
                        data_.pop('SOURCE_TYPE_')  # 获取请求参数
                    except Exception as e:
                        pass
                    # self.log('请求参数传递错误, {}'.format(e))
                    pass
                view_url = wash_url(request.url)

                # 使用 pyppeteer 中的 UA / IP
                header['User-Agent'] = UA
                proxies = {'http': f'http://{IP}', 'https': f'https://{IP}'}

                if request.meta.get('param_type') == 'PAYLOAD':
                    data_ = json.dumps(data_)

                # 使用代理IP 的实体
                if spider.name not in ENCRYPTION_BY_API_AND_NO_IP_ENTITY:
                    if data_:
                        result = requests.post(url=view_url, headers=header, data=data_, cookies=cookies, proxies=proxies, allow_redirects=False, verify=False) if request.method == 'POST' else requests.get(url=view_url, headers=header, params=data_, cookies=cookies, proxies=proxies, allow_redirects=False, verify=False)
                    else:
                        result = requests.post(url=view_url, headers=header, cookies=cookies, proxies=proxies, allow_redirects=False, verify=False) if request.method == 'POST' else requests.get(url=view_url, headers=header, cookies=cookies, proxies=proxies, allow_redirects=False, verify=False)

                else:
                    if data_:
                        result = requests.post(url=view_url, headers=header, data=data_, cookies=cookies, allow_redirects=False, verify=False) if request.method == 'POST' else requests.get(url=view_url, headers=header, params=data_, cookies=cookies, allow_redirects=False, verify=False)
                    else:
                        result = requests.post(url=view_url, headers=header, cookies=cookies, allow_redirects=False, verify=False) if request.method == 'POST' else requests.get(url=view_url, headers=header, cookies=cookies, allow_redirects=False, verify=False)

                # 更换数据编码
                if request.meta.get('textType') == 'html':
                    if result.status_code in [200, 304]:
                        encode = result.encoding
                        if not encode:
                            encode = 'UTF-8'
                        if encode == 'ISO-8859-1':
                            encodings = requests.utils.get_encodings_from_content(result.text)
                            if encodings:
                                encode = encodings[0]
                            else:
                                encode = result.apparent_encoding
                        sources = result.content.decode(encode, 'replace')
                else:
                    sources = result.json()

                return HtmlResponse(url=request.url, body=str(sources), encoding='utf-8', request=request)

            except Exception as e:
                # self.process_request(request, spider)
                pass
                # self.log.error('响应超时, {}'.format(e))

        # 单独处理直接可以拿到HTML页面的实体--通过 pyppeteer
        if spider.name in ENCRYPTION_BY_HTML_ENTITY:
            try:
                JS_url = wash_url(request.url)
                page = GetPageHtml()
                html, charset = page.run(JS_url, page.work, spider.name)
                return HtmlResponse(url=request.url, body=str(html), encoding=charset, request=request)
            except Exception as e:
                # self.process_request(request, spider)
                pass
                # self.log.error('响应超时, {}'.format(e))

        # 单独处理直接可以拿到HTML页面的实体--通过 Firefox
        if spider.name in ENCRYPTION_BY_HTML_FIREFOX or spider.name in ENCRYPTION_BY_API_FIREFOX:
            try:
                page = FirefoxGetPage()
                if spider.name in ENCRYPTION_BY_API_FIREFOX:
                    # 修改这个 url 为 Referer 中的url
                    try:
                        JS_url = (request.headers.get('Referer') if request.headers.get('Referer') else request.url).decode('utf-8')
                    except:
                        JS_url = wash_url(request.url)
                    charset, cookies, html_url, page_source, UA, IP = page.work(JS_url)
                    try:
                        from urllib.parse import quote
                        data_ = request.meta.get('parm').copy()
                        for key in data_.keys():
                            data_[key] = quote(data_[key])
                    except:
                        data_ = ''
                    try:
                        data_.pop('SALE_SOURCE_')  # 获取请求参数
                    except Exception as e:
                        try:
                            data_.pop('SOURCE_TYPE_')  # 获取请求参数
                        except Exception as e:
                            pass
                        # self.log('请求参数传递错误, {}'.format(e))
                        pass
                    view_url = wash_url(request.url)

                    # 使用 firefox 中的 UA / IP
                    header['User-Agent'] = UA
                    proxies = {'http': f'http://{IP}', 'https': f'https://{IP}'}

                    if request.meta.get('param_type') == 'PAYLOAD':
                        data_ = json.dumps(data_)

                    # 使用代理IP 的实体
                    if spider.name not in ENCRYPTION_BY_API_AND_NO_IP_ENTITY:
                        if data_:
                            result = requests.post(url=view_url, headers=header, data=data_, cookies=cookies,
                                                   proxies=proxies, allow_redirects=False,
                                                   verify=False) if request.method == 'POST' else requests.get(
                                url=view_url, headers=header, params=data_, cookies=cookies, proxies=proxies,
                                allow_redirects=False, verify=False)
                        else:
                            result = requests.post(url=view_url, headers=header, cookies=cookies, proxies=proxies,
                                                   allow_redirects=False,
                                                   verify=False) if request.method == 'POST' else requests.get(
                                url=view_url, headers=header, cookies=cookies, proxies=proxies, allow_redirects=False,
                                verify=False)

                    else:
                        if data_:
                            result = requests.post(url=view_url, headers=header, data=data_, cookies=cookies,
                                                   allow_redirects=False,
                                                   verify=False) if request.method == 'POST' else requests.get(
                                url=view_url, headers=header, params=data_, cookies=cookies, allow_redirects=False,
                                verify=False)
                        else:
                            result = requests.post(url=view_url, headers=header, cookies=cookies, allow_redirects=False,
                                                   verify=False) if request.method == 'POST' else requests.get(
                                url=view_url, headers=header, cookies=cookies, allow_redirects=False, verify=False)

                    # 更换数据编码
                    if request.meta.get('textType') == 'html':
                        if result.status_code in [200, 304]:
                            encode = result.encoding
                            if not encode:
                                encode = 'UTF-8'
                            if encode == 'ISO-8859-1':
                                encodings = requests.utils.get_encodings_from_content(result.text)
                                if encodings:
                                    encode = encodings[0]
                                else:
                                    encode = result.apparent_encoding
                            sources = result.content.decode(encode, 'replace')
                    else:
                        sources = result.json()

                    return HtmlResponse(url=request.url, body=str(sources), encoding='utf-8', request=request)

                else:
                    JS_url = wash_url(request.url)
                    charset, cookie, html_url, page_source, UA, IP = page.work(JS_url)
                    return HtmlResponse(url=request.url, body=str(page_source), encoding=charset, request=request)
            except Exception as e:
                # self.process_request(request, spider)
                pass
                # self.log.error('响应超时, {}'.format(e))

        # 为平安的爬虫自定义下载
        if request.meta.get('userDefind'):
            url = request.meta.get('url')
            # Session = HTMLSession()
            # results = Session.get(url)
            # results.html.render(scrolldown=50, sleep=.2)
            # page_source = results.html.html
            page = GetPageHtml()
            response, charset = page.run(url, page.work, '')
            # charset, cookie, html_url, response, UA = FirefoxGetPage().work(url)

            return HtmlResponse(url=request.url, body=str(response), encoding=charset if charset else 'utf-8', request=request)
