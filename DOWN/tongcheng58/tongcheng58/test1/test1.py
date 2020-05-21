"""__author__=李佳林"""
import requests
from lxml import etree

def get_url():

    url = 'https://bj.58.com/ershoufang/42135407367687x.shtml'
    headers = {
        "User-Agent": "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; 360SE)"
    }
    response = requests.get(url,headers=headers)
    if response.status_code == 200:
        return response.content.decode('utf8')
    raise Exception('error')

def city_parse(html):
    etree_html = etree.HTML(html)

    area_name = etree_html.xpath('//div[@id="qySelectFirst"]/a]/text()')
    area_url = etree_html.xpath('//div[@id="qySelectFirst"]/a/@href')[0]

    return area_name,area_url

def area_parse(html):
    etree_html = etree.HTML(html)
    second_area_name = etree_html.xpath('//div[@id="qySelectSecond"]/a/text()')
    second_area_names = []
    for item in second_area_name:
        second_area_names.append(item.strip())
    second_area_url = etree_html.xpath('//div[@id="qySelectSecond"]/a/@href')
    return second_area_names,second_area_url


def area_page(html):
    etree_html = etree.HTML(html)
    house_url = etree_html.xpath('//div[@class="list-info"]/h2[@class="title"]/a/@href')
    house_price = etree_html.xpath('//p[@class="sum"]//text()')
    house_prices = []


    house_page = etree_html.xpath('//div[@class="pager"]/strong/span/text()')
    no_house = etree_html.xpath('//p[@class="noresult-tip noresult-tip2"]/text()')
    return house_url

def house_message(html):
    etree_html = etree.HTML(html)
    city = etree_html.xpath('//div[@class="nav-top-bar fl c_888 f12"]/a[1]/text()')[0][:-5]
    position = etree_html.xpath(
                '//ul[@class="house-basic-item3"]/li/span[@class="c_000 mr_10"]/a/text()')

    area = position[2].strip()
    second_area = position[1].strip()
    house_name = position[0].strip()
    house_title = etree_html.xpath('//h1[@class="c_333 f20"]/text()')
    # house_name = \
    # etree_html.xpath('//ul[@class="house-basic-item3"]/li/span/a/text()')[2].strip()
    house_describe = etree_html.xpath('//div[@class="general-item-wrap"]/div[1]/p[@class="pic-desc-word"]/text()')
    house_img = etree_html.xpath('//ul[@id="leftImg"]/li/@data-value')
    house_address = position[3].strip().replace(' ','')[1:] if len(position) == 4 else '无'

    basic_information = etree_html.xpath('//div[@id="generalSituation"]//span[@class="c_000"]/text()')
    a = etree_html.xpath('//span[@id="basicInfo"]/span/text()')
    # house_type = basic_information[0]
    # house_proportion = basic_information[1]
    # house_orientation = basic_information[2]
    # house_floor = basic_information[4]
    # house_fitment = basic_information[5]
    # house_year = basic_information[7]


    return house_address

def get_area(html):
    etree_html = etree.HTML(html)
    # print(response.text)
    city_name = etree_html.xpath('//dl[@id="clist"]/dd/a/text()')
    city_url = etree_html.xpath('//dl[@id="clist"]/dd/a/@href')
    return city_url

def get_areas(html):

    etree_html = etree.HTML(html)
    area = etree_html.xpath('//div[@id="qySelectFirst"]/a/text()')



def main():
    # html = get_url()
    # print(html)
    # with open('house_message.txt','w',encoding='utf-8') as f:
    #     f.write(html)
    # print(get_area(html))

    # 测试区域
    # with open('three.txt','r',encoding='utf-8') as f:
    #     html = f.read()
    # print(area_page(htmwith open('three.txt','r',encoding='utf-8') as f:
    #         html = f.read()l))
    # area_list = area_page(html)
    # for i,index in enumerate(area_list):
    #     if 'https' not in index:
    #         # print(index)
    #         area_list[i] = 'https:' + index
    # print(area_list)
    # print(area_page(html))


    # 测试区域下面
    # with open('house_message.txt','r',encoding='utf-8') as f:
    #     html = f.read()
    # print(house_message(html))

    # 测试二层区域数据
    # with open('four.txt','r',encoding='utf-8') as f:
    #     html = f.read()
    # print(area_page(html))

    # 测试房子数据
    with open('house_message.txt','r',encoding='utf-8') as f:
        html = f.read()
    print(house_message(html))

if __name__ == '__main__':
    main()