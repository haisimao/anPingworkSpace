# coding=utf-8
import requests
import re
import time
import lxml.html as H
import base64
from fontTools.ttLib import TTFont


def get_data(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36',
    }
    session = requests.session()
    # con = session.get(url, headers=headers)
    # doc = H.document_fromstring(con.content)
    # "   url('../../../styles/fonts/china.ttf?fdipzone') format('woff'),   "

    # font_data_origin = re.search(r"styles/fonts/([.\w\?\.])*\'\) format\(\'woff\'\)", con.content.decode('gb18030'), re.S).group(1)
    # resp = session.get('https://www.landchina.com/styles/fonts/' + font_data_origin, headers=headers)
    resp = session.get('https://www.landchina.com/styles/fonts/china.woff?fdipzone', headers=headers)

    # new_font_name = "font_china_new.ttf"
    # with open(new_font_name, 'wb') as f:
    #     f.write(resp.content)

    map_data = tff_parse('font_china_new.ttf')  # 解析字体
    # names = doc.xpath('//td[@id="tdContent"]//text()')
    names = '鈽鈣廰喪磚錞媙糘场，合舗配置錞媙絒赑，墵冄園挥軍划甴导釢瀍，切实餀認毣談嵿閩瀍媙供应匬舗，依葒毣錞絒赑峯'
    # 有的时候会找不到，可以多执行几次；
    if names:
        content = []
        for name in names:
            # print(name, ':', map_data.get(name))
            if map_data.get(name):
                content.append(map_data.get(name))
            else:
                content.append(name)
        print(names)
        print(''.join(content))
        return ''.join(content)


def tff_parse(font_parse_name):
    # 我这里的字体的顺序，如果你的不同，一定要修改
    font_dict = [u'日', u'坚', u'无', u'竞', u'东', u'顿', u'大', u'汉', u'德', u'省', u'简', u'十', u'革', u'补', u'史', u'件', u'阿',
                 u'注', u'网', u'极', u'络', u'经', u'敌', u'属', u'洁', u'术', u'承', u'续', u'与', u'想', u'说', u'公', u'里', u'管',
                 u'涯', u'名', u'讲', u'黑', u'红', u'平', u'申', u'调', u'讯', u'去', u'示', u'规', u'来', u'度', u'变', u'铁', u'犯',
                 u'酷', u'宣', u'治', u'窗', u'发', u'统', u'制', u'积', u'质', u'成', u'都', u'博', u'办', u'云', u'习', u'脸', u'字',
                 u'复', u'地', u'兵', u'历', u'素', u'水', u'入', u'型', u'丽', u'布', u'财', u'迎', u'部', u'决', u'认', u'委', u'建',
                 u'方', u'要', u'除', u'户', u'学', u'密', u'荣', u'页', u'狗', u'展', u'夫', u'黄', u'上', u'命', u'不', u'台', u'进',
                 u'书', u'退', u'赋', u'范', u'产', u'多', u'点', u'企', u'宽', u'群', u'斯', u'个', u'困', u'团', u'论', u'登', u'于',
                 u'弘', u'扬', u'责', u'家', u'载', u'移', u'准', u'程', u'鞋', u'关', u'通', u'育', u'臂', u'聚', u'务', u'端', u'转',
                 u'当', u'明', u'失', u'胡', u'印', u'共', u'精', u'裁', u'改', u'北', u'先', u'末', u'国', u'情', u'工', u'性', u'广',
                 u'化', u'数', u'之', u'集', u'人', u'政', u'更', u'拥', u'盒', u'华', u'李', u'膀', u'吸', u'用', u'湾', u'标', u'卫',
                 u'电', u'周', u'践', u'吾', u'机', u'排', u'九', u'典', u'做', u'恶', u'张', u'巴', u'活', u'其', u'总', u'七', u'六',
                 u'引', u'艺', u'的', u'思', u'信', u'首', u'本', u'新', u'步', u'请', u'代', u'传', u'锋', u'控', u'列', u'神', u'同',
                 u'下', u'哲', u'年', u'律', u'员', u'城', u'赢', u'西', u'市', u'息', u'央', u'版', u'海', u'技', u'软', u'资', u'报',
                 u'波', u'光', u'远', u'山', u'文', u'善', u'众', u'动', u'面', u'单', u'位', u'任', u'军', u'深', u'社', u'江', u'伦',
                 u'款', u'浪', u'渔', u'欢', u'理', u'局', u'见', u'开', u'录', u'据', u'搜', u'舆', u'对', u'田', u'梦', u'告', u'亲',
                 u'泽', u'勇', u'干', u'飞', u'系', u'错', u'扎', u'遗', u'全', u'涉', u'参', u'微', u'扫', u'态', u'役', u'专', u'幕',
                 u'祠', u'何', u'指', u'修', u'事', u'强', u'园', u'克', u'淀', u'所', u'有', u'乐', u'厅', u'站', u'府', u'康', u'廉',
                 u'期', u'听', u'选', u'届', u'境', u'主', u'抱', u'血', u'洛', u'天', u'源', u'疆', u'绩', u'我', u'层', u'协', u'计',
                 u'行', u'毛', u'线', u'会', u'时', u'高', u'欧', u'利', u'堡', u'能', u'知', u'农', u'存', u'作', u'招', u'近', u'中',
                 u'第', u'科', u'得', u'为', u'挂', u'体', u'热', u'具', u'义', u'舟', u'埃', u'土', u'顶', u'马', u'频', u'持', u'音',
                 u'健', u'绿', u'权', u'外', u'生', u'正', u'牌', u'如', u'收', u'南', u'聘', u'临', u'设', u'皮', u'区', u'魂', u'担',
                 u'凰', u'津', u'推', u'量', u'狐', u'服', u'业', u'视', u'民', u'好', u'悬', u'穿', u'腾', u'贸', u'浙', u'定', u'加',
                 u'优', u'藏', u'坛', u'冀', u'头', u'沉', u'党', u'凤', u'百', u'荐', u'常', u'在', u'闻', u'负', u'法', u'争', u'客']
    font_base = TTFont('china.ttf')
    font_base_order = font_base.getGlyphOrder()[4:]  # 对应切字 uniE77C
    # font_base.saveXML('font_base.xml')    # 调试用

    font_parse = TTFont(font_parse_name)

    # font_parse.saveXML('font_parse_2.xml')  # 调试用
    font_parse_order = font_parse.getGlyphOrder()[4:]

    f_base_flag = []
    for i in font_base_order:  # 遍历 uniE77C 拿到字形的编码
        flags = font_base['glyf'][i].flags
        f_base_flag.append(list(flags))  # 获取到 flags 值 并写入列表

    f_flag = []
    for i in font_parse_order:
        flags = font_parse['glyf'][i].flags
        f_flag.append(list(flags))

    result_dict = {}
    for a, i in enumerate(f_base_flag):
        for b, j in enumerate(f_flag):
            if comp(i, j):
                key = font_parse_order[b].replace('uni', '')
                key = eval(r'u"\u' + str(key) + '"').lower()
                result_dict[key] = font_dict[a]  # 对应的逻辑 字形对应上了,, 就取 baseFont 的值
    return result_dict  # 返回对应的字体


def comp(L1, L2):
    # 效验列表每一位, 看连个列表是否相等,,,,,是否用 hash 值代替
    if len(L1) != len(L2):
        return 0
    for i in range(len(L2)):
        if L1[i] == L2[i]:
            pass
        else:
            return 0
    return 1


# 替换内容
def replace_content(names,font_name):
    import os
    try:
        # map_data = tff_parse('./font_china_new.ttf')  # 解析字体
        map_data = tff_parse(f'./{font_name}.ttf')  # 解析字体
    except:
        map_data = tff_parse('./china.ttf')  # 解析字体
    # names = """鈽鈣廰喪磚錞媙糘场，合舗配置錞媙絒赑，墵冄園挥軍划甴导釢瀍，切实餀認毣談嵿閩瀍媙供应匬舗，依葒毣錞絒赑峯"""
    # 有的时候会找不到，可以多执行几次；
    if names:
        content = []
        for name in names:
            # print(name, ':', map_data.get(name))
            if map_data.get(name):
                content.append(map_data.get(name))
            else:
                content.append(name)
        os.remove(f'./{font_name}.ttf')
        return ''.join(content)


if __name__ == '__main__':
    url = "https://www.landchina.com/DesktopModule/BizframeExtendMdl/workList/bulWorkView.aspx?wmguid=6506735a-142d-4bde-9674-121656045ed1&recorderguid=906139c6-e869-46c4-a97b-6a5a95f857e9&sitePath="
    replace_content(names = """鈽鈣廰喪磚錞媙糘场，合舗配置錞媙絒赑，墵冄園挥軍划甴导釢瀍，切实餀認毣談嵿閩瀍媙供应匬舗，依葒毣錞絒赑峯""")
