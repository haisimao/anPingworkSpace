import logging
# from requests_html import HTMLSession, HTML
import re
import time
from urllib.parse import quote
import urllib3
import jsonpath
import requests
import ssl
from scrapy import Selector

urllib3.disable_warnings()
logging.captureWarnings(True)
ssl._create_default_https_context = ssl._create_unverified_context
# 要访问的目标页面
now_time = int(time.time() * 1000)

sourceUrl = 'https://www.lyggzy.com.cn/lyztb/tdky/084002/?pageing=4'
            # 'https://miniapp.58.com/common/relation/?wqr=1&param_id=tangram_prop_view_1_7e6f582b587e23d'

header = {'User-Agent': 'Mozilla/5.0 (Linux; U; Android 8.1.0; zh-cn; BLA-AL00 Build/HUAWEIBLA-AL00) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/57.0.2987.132 MQQBrowser/8.9 Mobile Safari/537.36',

# 'Upgrade-Insecure-Requests': '1',
# 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
# 'Accept-Encoding': 'gzip, deflate, br',
# 'Accept-Language': 'zh-CN,zh;q=0.9',
# 'Cache-Control': 'max-age=0',
# 'Connection': 'keep-alive',

         }

session = requests.session()

data = {
'serchTitle': '',
'beginTime': '',
'endTime': '',
'countyid': '',
'tdYt': '0',
'State': '',
'Afficetype': '',
'Selltype': '',
'limit': '13',
'offset': '39',
'order': 'asc',
}
cookies = {
    'yfx_c_g_u_id_10002742':'_ck20050814334619870235553750931' ,
'yfx_f_l_v_t_10002742':'f_t_1588919626971__r_t_1588919626971__v_t_1588919626971__r_c_0',
'Hm_lvt_0404e8a4e2a4fb574bf8dc18126db6a8':'1588919628',
'Hm_lpvt_0404e8a4e2a4fb574bf8dc18126db6a8':'1588925363',

}
resp = session.get(sourceUrl, headers={'User-Agent':'Mozilla/5.0 (Linux; U; Android 4.3; en-us; SM-N900T Build/JSS15J) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30'}, allow_redirects=False, timeout=60)


print(resp.status_code)
print(resp.text)
