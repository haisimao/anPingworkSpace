# -*- coding: utf-8 -*-

# Scrapy settings for IntegrationSpider project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://doc.scrapy.org/en/latest/topics/settings.html
#     https://doc.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://doc.scrapy.org/en/latest/topics/spider-middleware.html
import datetime
import os
import time
from urllib.parse import quote

BOT_NAME = 'IntegrationSpider'

SPIDER_MODULES = ['IntegrationSpider.spiders']
NEWSPIDER_MODULE = 'IntegrationSpider.spiders'


# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'IntegrationSpider (+http://www.yourdomain.com)'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Configure maximum concurrent requests performed by Scrapy (default: 16)
CONCURRENT_REQUESTS = 2

# Configure a delay for requests for the same website (default: 0)
# See https://doc.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
DOWNLOAD_DELAY = 1
# The download delay setting will honor only one of:
CONCURRENT_REQUESTS_PER_DOMAIN = 4
CONCURRENT_REQUESTS_PER_IP = 4

# Disable cookies (enabled by default)
#COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
#DEFAULT_REQUEST_HEADERS = {
#   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
#   'Accept-Language': 'en',
#}

# Enable or disable spider middlewares
# See https://doc.scrapy.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    'IntegrationSpider.middlewares.IntegrationspiderSpiderMiddleware': 543,
#}

# Enable or disable downloader middlewares
# See https://doc.scrapy.org/en/latest/topics/downloader-middleware.html
DOWNLOADER_MIDDLEWARES = {
   'IntegrationSpider.middlewares.IntegrationspiderDownloaderMiddleware': 543,
    'IntegrationSpider.middlewares.DLMiddleware': 544,
    'IntegrationSpider.middlewares.DownLoadsMiddleware': 545,
}

# Enable or disable extensions
# See https://doc.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
#}

# Configure item pipelines
# See https://doc.scrapy.org/en/latest/topics/item-pipeline.html
# ITEM_PIPELINES = {
#    'IntegrationSpider.pipelines.IntegrationPageItem': 300,
# }

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://doc.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://doc.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = 'httpcache'
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'
COOKIES_DEBUG = True

CLOSESPIDER_TIMEOUT = 60 * 3000  # 设置爬虫定时关闭时间

# The initial download delay
AUTOTHROTTLE_START_DELAY = 2
# The maximum download delay to be set in case of high latencies
AUTOTHROTTLE_MAX_DELAY = 10

LOG_LEVEL = 'INFO'
DNS_TIMEOUT = 30
DOWNLOAD_TIMEOUT = 120

# 日志目录配置

today = datetime.datetime.now()
log_file_path = "{}/Logs/{}{}-{}-{}.log".format(os.path.abspath(os.path.dirname(os.path.dirname(__file__))), BOT_NAME, today.year, today.month, today.day)
LOG_FILE =log_file_path


# 下载中间件参数
ENCRYPTION_BY_HTML_ENTITY = [
    'ZX_GWDT_PFYH_XWDT',
]
# ENTITY_CODE 使用接口请求不使用IP 代理的, 在中间件中
ENCRYPTION_BY_API_AND_NO_IP_ENTITY = [
    'ZTB_ZGCGZB',
]

# 在 pyppeteer 中也不使用代理
ENCRYPTION_NOT_IP_ENTITY = [
                            'ZTB_ZGCGZB',
]

# 需要中间件单独处理的 且 通过 pyppeteer 获取数据的 ENTITY_CODE
ENCRYPTION_ENTITY = ['JRCP_XYK_WAK_ALL',
                     ]

ENCRYPTION_ENTITY.extend(ENCRYPTION_BY_API_AND_NO_IP_ENTITY)

# 通过Firefox 直接获取数据的
ENCRYPTION_BY_HTML_FIREFOX = [
        'ZX_ZCGG_ZGRMYH_TFS2',
]

# 通过Firefox 获取cookies 再获取数据
ENCRYPTION_BY_API_FIREFOX = [
                     'CCBPage',
]

# 去重开关, 需要爬取增量数据时开启
DUPLICATE_SWITCH = False
DUPLICATE_SWITCH_LIST = []

# 重定向 HTTP_CODE
# RETRY_HTTP_CODES = [500, 503, 504, 400, 403, 404, 408]