import asyncio
import base64
import os, sys
import ssl
import time, random
import pyppeteer
import requests
from pyppeteer.launcher import launch  # 控制模拟浏览器用
from pyppeteer import errors
from selenium import webdriver
from selenium.webdriver.firefox.firefox_profile import FirefoxProfile

from IntegrationSpider import useragent
from IntegrationSpider.settings import ENCRYPTION_NOT_IP_ENTITY

curPath = os.path.abspath(os.path.dirname(__file__))
sys.path.append(curPath[:-12])


from OperateDB.OpRedis import RedisClient


class GetJsEncryptPage(object):

    def __init__(self):
        self.loop = asyncio.get_event_loop()
        # self.log = ICrawlerLog('spider').save

    async def main(self, url, entityCode):  # 定义main协程函数，
        # 以下使用await 可以针对耗时的操作进行挂起
        IP = RedisClient().get()
        if entityCode in ENCRYPTION_NOT_IP_ENTITY:
            browser = await launch({'headless': True, 'timeout': 20,
                                'args': [
                                    '--no-sandbox',
                                    '--disable-gpu',
                                    '--disable-infobars',
                                ], })
        else:
            browser = await launch({'headless': True, 'timeout': 20,
                                'args': [
                                    '--no-sandbox',
                                    '--disable-gpu',
                                    '--disable-infobars',
                                    '--proxy-server={}'.format(IP),
                                ], })
        page = await browser.newPage()  # 启动个新的浏览器页面标签
        UA = random.choice(user_agent_list)
        await page.setUserAgent(UA)
        await page.setJavaScriptEnabled(enabled=True)  # 使用 JS 渲染
        # await page.setRequestInterception(True)
        # page.on('request', self.intercept_request)
        # page.on('response', self.intercept_response)
        cookies = {}
        try:
            await self.change_status(page)
            await self.goto(page, url)  # 访问页面
            time.sleep(3)
            await self.change_status(page)
            await self.goto(page, url)
            await asyncio.sleep(1)
        except Exception as e:
            # self.log.error(f'浏览器请求出错, {e}')
            await self.change_status(page)
            # await page.evaluate('window.open("{}");'.format(url))
            await page.evaluate('window.location="{}";'.format(url))
            # await page.goto(url)  # 访问登录页面
            await asyncio.sleep(2)
        try:
            # cookie = await page.evaluate('''() = > { return document.cookie}''')
            cookies = await self.get_cookie(page)
            # content = await page.content()
        except Exception as e:
            # self.log.error(f'浏览器获取cookies出错, {e}')
            await browser.close()
        finally:
            await browser.close()
        return cookies, IP, UA

    async def goto(self, page, url):
        for _ in range(5):
            try:
                await page.goto(url, {'timeout': 180000, 'waitUntil': 'networkidle0'})
                await asyncio.sleep(1)
                break
            except (pyppeteer.errors.NetworkError, pyppeteer.errors.PageError) as ex:
                # 无网络 'net::ERR_INTERNET_DISCONNECTED','net::ERR_TUNNEL_CONNECTION_FAILED'
                if 'net::' in str(ex):
                    await asyncio.sleep(10)
                else:
                    raise

    async def change_status(self, page):
        # 始终让window.navigator.webdriver=false
        # navigator是windiw对象的一个属性，同时修改plugins，languages，navigator 且让
        # 以下为插入中间js，将淘宝会为了检测浏览器而调用的js修改其结果。
        await page.evaluate('''() =>{ Object.defineProperties(navigator,{ webdriver:{ get: () => false } }) }''')
        await page.evaluate('''() =>{ window.navigator.chrome = { runtime: {},  }; }''')
        await page.evaluate('''() =>{ Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] }); }''')
        await page.evaluate('''() =>{ Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5,6], }); }''')

    async def intercept_request(req):
        """请求过滤"""
        if req.resourceType in ['image', 'media', 'eventsource', 'websocket']:
            await req.abort()
        else:
            await req.continue_()

    async def intercept_response(res):
        resourceType = res.request.resourceType
        if resourceType in ['xhr', 'fetch']:
            pass

    async def get_cookie(self, page):
        # res = await page.content()
        cookies_list = await page.cookies()
        cookies = {}
        for cookie in cookies_list:
            cookies[cookie.get('name')] = cookie.get('value')
        return cookies

    def run(self, url, entityCode):
        result = {}
        try:
            result = self.loop.run_until_complete(self.main(url, entityCode))  # 将协程注册到事件循环，并启动事件循环
        except Exception as e:
            # self.log.info(f'协程被动结束, chrome关闭, {e}')
            for task in asyncio.Task.all_tasks():
                task.cancel()
                self.loop.stop()
                self.loop.run_forever()
        return result


class BaseJsEncryptPage(object):

    def __init__(self):
        self.loop = asyncio.get_event_loop()
        # self.log = ICrawlerLog('spider').save

    async def get_cookie(self, page):
        # res = await page.content()
        cookies_list = await page.cookies()
        cookies = {}
        for cookie in cookies_list:
            cookies[cookie.get('name')] = cookie.get('value')
        return cookies

    def work(self, url, entityCode):
        pass

    def run(self, url, func, entityCode):
        result = {}
        try:
            # task = asyncio.wait([])
            result = self.loop.run_until_complete(func(url, entityCode))  # 将协程注册到事件循环，并启动事件循环
        except Exception as e:
            # self.log.info('协程被动结束, chrome关闭')
            for task in asyncio.Task.all_tasks():
                task.cancel()
                self.loop.stop()
                self.loop.run_forever()
        finally:
            self.loop.close()
        return result


class GetPageHtml(BaseJsEncryptPage):
    '''
    只能用于地址模板
    '''

    async def change_status(self, page):
        await page.evaluate('''() =>{ Object.defineProperties(navigator,{ webdriver:{ get: () => false } }) }''')
        await page.evaluate('''() =>{ window.navigator.chrome = { runtime: {},  }; }''')
        await page.evaluate(
            '''() =>{ Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] }); }''')
        await page.evaluate(
            '''() =>{ Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5,6], }); }''')

    async def get_charset(self, page):
        await page.evaluate('''() =>{ var charset = document.charset; return charset; } ''')

    async def request_check(self, req):
        '''请求过滤, 指定类型的请求被处理'''
        if req.resourceType in ['image', 'media', 'eventsource', 'websocket']:
            await req.abort()
        else:
            await req.continue_()

    async def intercept_response(self, res):
        resourceType = res.request.resourceType
        if resourceType in ['image', 'media']:
            resp = await res.text()
            print(resp)

    async def goto(self, page, url):
        for _ in range(5):
            try:
                await page.goto(url, {'timeout': 180000, 'waitUntil': 'networkidle0'})
                break
            except (pyppeteer.errors.NetworkError, pyppeteer.errors.PageError) as ex:
                # 无网络 'net::ERR_INTERNET_DISCONNECTED','net::ERR_TUNNEL_CONNECTION_FAILED'
                if 'net::' in str(ex):
                    await asyncio.sleep(10)
                else:
                    raise

    async def work(self, url, entityCode):
        IP = RedisClient().get()
        if entityCode in ENCRYPTION_NOT_IP_ENTITY:
            browser = await launch({'headless': True, 'timeout': 10,
                                'args': [
                                    '--no-sandbox',
                                    '--disable-gpu',
                                    '--disable-infobars',
                                ], })
        else:
            browser = await launch({'headless': True, 'timeout': 10,
                                'args': [
                                    '--no-sandbox',
                                    '--disable-gpu',
                                    '--disable-infobars',
                                    '--proxy-server={}'.format(IP),
                                ], })
        page = await browser.newPage()  # 启动个新的浏览器页面标签
        UA = random.choice(user_agent_list)
        await page.setUserAgent(UA)
        await page.setJavaScriptEnabled(enabled=True)  # 使用 JS 渲染
        # await page.setRequestInterception(True)
        # page.on('request', self.intercept_response)
        data = ''
        charset = 'utf-8'
        try:
            await self.change_status(page)
            await asyncio.wait_for(self.goto(page, url), timeout=10.0)
            # page_ = await browser.pages()  # 获取标签page, pyppeteer 的tag是一个page
            time.sleep(5)
            await self.change_status(page)
            await asyncio.wait_for(page.reload(), timeout=5.0)
            # data = await page.content()
            data = await asyncio.wait_for(page.content(), timeout=10.0)
            await asyncio.sleep(5)
            charset = await page.evaluate('''() =>{ var charset = document.charset; return charset; } ''')
        except Exception as e:
            # self.log.info('获取失败')
            pass
        finally:
            await browser.close()
        return data, charset


class FirefoxGetPage(object):
    '''
    返回  (charset: '编码', cookie, html_url: '搜索结果的实体url', response: '具体响应的')
    '''
    def __init__(self):
        # self.log = ICrawlerLog('spider').save
        self.IP = RedisClient().get()
        self.UA = random.choice(user_agent_list)
        option = webdriver.FirefoxOptions()
        option.add_argument('-headless')  # 启用无头
        option.add_argument("--start-maximized")
        option.add_argument('--no-sandbox')
        option.add_argument('user-agent="{}"'.format(self.UA))
        option.add_argument('--disable-gpu')  # 禁用 GPU 硬件加速，防止出现bug

        profile = FirefoxProfile()
        # 激活手动代理配置（对应着在 profile（配置文件）中设置首选项）
        profile.set_preference("network.proxy.type", 1)
        # ip及其端口号配置为 http 协议代理
        profile.set_preference("network.proxy.http", self.IP.split(':')[0])
        profile.set_preference("network.proxy.http_port", self.IP.split(':')[-1])

        # 所有协议共用一种 ip 及端口，如果单独配置，不必设置该项，因为其默认为 False
        profile.set_preference("network.proxy.share_proxy_settings", True)

        # 默认本地地址（localhost）不使用代理，如果有些域名在访问时不想使用代理可以使用类似下面的参数设置
        # profile.set_preference("network.proxy.no_proxies_on", "localhost")
        self.browser = webdriver.Firefox(options=option)
        # self.browser = webdriver.Firefox(options=option, firefox_profile=profile, firefox_binary='C:\Program Files\Mozilla Firefox/firefox.exe')
        self.browser.maximize_window()
        # self.browser.get('http://www.ip138.com/')
        self.browser.set_page_load_timeout(60)
        self.browser.set_script_timeout(60)  # 这两种设置都进行才有效

    def changeWebdriver(self, browser):
        # 设置 webdriver 属性
        browser.execute_script('''() =>{ Object.defineProperties(navigator,{ webdriver:{ get: () => false } }) }''')
        browser.execute_script('''() =>{ window.navigator.chrome = { runtime: {},  }; }''')
        browser.execute_script('''() =>{ Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] }); }''')
        browser.execute_script('''() =>{ Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5,6], }); }''')

    def work(self, url):
        cookie = {}
        html_url = ''
        response = ''
        charset = 'utf-8'
        # 打开网页
        try:
            self.changeWebdriver(self.browser)
            self.browser.execute_script('window.open("{}");'.format('https://www.baidu.com/?tn=22073068_3_oem_dg'))
            time.sleep(3)
            js = 'window.open("{}");'.format(url)
            self.changeWebdriver(self.browser)
            self.browser.execute_script(js)
            time.sleep(9)
            self.changeWebdriver(self.browser)
            charset = self.browser.execute_script('return document.charset;')
            handles = self.browser.window_handles
            if len(handles) > 4:
                raise ValueError
            for handle in handles:  # 切换窗口
                if handle != self.browser.current_window_handle:
                    self.browser.switch_to_window(handle)
                    break
            self.browser.refresh()
            if self.browser.get_cookies():
                for i in self.browser.get_cookies():
                    cookie[i["name"]] = i["value"]
            if len(handles) > 4:
                for handle in handles:  # 切换窗口
                    if handle != handles[0]:
                        self.browser.switch_to_window(handle)
                        self.browser.close()  # 关闭当前窗口
            response = self.browser.page_source
        except Exception as e:
            # self.log(f'selenium直接获取数据异常异常终止, {e}')
            pass
        finally:
            self.browser.quit()
        return (charset, cookie, html_url, response, self.UA, self.IP)


if __name__ == '__main__':
    url = 'http://www.ip138.com/'

    url_ = 'http://www.chinabidding.cc/'

    referer = 'http://www.pbc.gov.cn/zhengwugongkai/127924/128038/128109/17475/index1.html'

    # cookies = GetJsEncryptPage().run(url, '')
    # print(cookies)
    page = GetPageHtml()
    data = page.run(referer, page.work, '')
    print(data )
