import re

# s = '用途名称#面积#上海#投资强度'
# s = '用途名称#面积#123#投资强度'
# s = '用途名称#面积#上海#1.23#投资强度'
# 
# a = re.findall(r'用途名称#面积#.*?([\d|\.|#]+?)#投资强度', s)
# b = re.findall(r'用途名称#面积#([\u4E00-\u9FA5]+).*?#投资强度', s)
# # ytmj = re.findall(r'用途名称#面积#(\d+?)#投资强度', s)
# print(a,b)

t = [{
	"id": 2,
	"group": "1",
	"value": "0",
	"name": "行政区划",
	"isParent": True
}, {
	"id": 1,
	"group": "1",
	"value": "",
	"name": "中国",
	"pId": 2,
	"isParent": False
}, {
	"id": 4545,
	"group": "1",
	"value": "11",
	"name": "北京市",
	"pId": 2,
	"isParent": True
}, {
	"id": 4624,
	"group": "1",
	"value": "12",
	"name": "天津市",
	"pId": 2,
	"isParent": True
}, {
	"id": 4701,
	"group": "1",
	"value": "13",
	"name": "河北省",
	"pId": 2,
	"isParent": True
}, {
	"id": 5450,
	"group": "1",
	"value": "14",
	"name": "山西省",
	"pId": 2,
	"isParent": True
}, {
	"id": 5991,
	"group": "1",
	"value": "15",
	"name": "内蒙古",
	"pId": 2,
	"isParent": True
}, {
	"id": 6354,
	"group": "1",
	"value": "21",
	"name": "辽宁省",
	"pId": 2,
	"isParent": True
}, {
	"id": 6597,
	"group": "1",
	"value": "22",
	"name": "吉林省",
	"pId": 2,
	"isParent": True
}, {
	"id": 76,
	"group": "1",
	"value": "23",
	"name": "黑龙江省",
	"pId": 2,
	"isParent": True
}, {
	"id": 378,
	"group": "1",
	"value": "31",
	"name": "上海市",
	"pId": 2,
	"isParent": True
}, {
	"id": 419,
	"group": "1",
	"value": "32",
	"name": "江苏省",
	"pId": 2,
	"isParent": True
}, {
	"id": 671,
	"group": "1",
	"value": "33",
	"name": "浙江省",
	"pId": 2,
	"isParent": True
}, {
	"id": 885,
	"group": "1",
	"value": "34",
	"name": "安徽省",
	"pId": 2,
	"isParent": True
}, {
	"id": 1147,
	"group": "1",
	"value": "35",
	"name": "福建省",
	"pId": 2,
	"isParent": True
}, {
	"id": 1345,
	"group": "1",
	"value": "36",
	"name": "江西省",
	"pId": 2,
	"isParent": True
}, {
	"id": 1577,
	"group": "1",
	"value": "37",
	"name": "山东省",
	"pId": 2,
	"isParent": True
}, {
	"id": 1909,
	"group": "1",
	"value": "41",
	"name": "河南省",
	"pId": 2,
	"isParent": True
}, {
	"id": 2279,
	"group": "1",
	"value": "42",
	"name": "湖北省",
	"pId": 2,
	"isParent": True
}, {
	"id": 2522,
	"group": "1",
	"value": "43",
	"name": "湖南省",
	"pId": 2,
	"isParent": True
}, {
	"id": 2807,
	"group": "1",
	"value": "44",
	"name": "广东省",
	"pId": 2,
	"isParent": True
}, {
	"id": 3109,
	"group": "1",
	"value": "45",
	"name": "广西壮族",
	"pId": 2,
	"isParent": True
}, {
	"id": 3370,
	"group": "1",
	"value": "46",
	"name": "海南省",
	"pId": 2,
	"isParent": True
}, {
	"id": 3424,
	"group": "1",
	"value": "50",
	"name": "重庆市",
	"pId": 2,
	"isParent": True
}, {
	"id": 3507,
	"group": "1",
	"value": "51",
	"name": "四川省",
	"pId": 2,
	"isParent": True
}, {
	"id": 3927,
	"group": "1",
	"value": "52",
	"name": "贵州省",
	"pId": 2,
	"isParent": True
}, {
	"id": 4119,
	"group": "1",
	"value": "53",
	"name": "云南省",
	"pId": 2,
	"isParent": True
}, {
	"id": 4410,
	"group": "1",
	"value": "54",
	"name": "西藏",
	"pId": 2,
	"isParent": True
}, {
	"id": 4586,
	"group": "1",
	"value": "61",
	"name": "陕西省",
	"pId": 2,
	"isParent": True
}, {
	"id": 5079,
	"group": "1",
	"value": "62",
	"name": "甘肃省",
	"pId": 2,
	"isParent": True
}, {
	"id": 5509,
	"group": "1",
	"value": "63",
	"name": "青海省",
	"pId": 2,
	"isParent": True
}, {
	"id": 5701,
	"group": "1",
	"value": "64",
	"name": "宁夏回族",
	"pId": 2,
	"isParent": True
}, {
	"id": 5818,
	"group": "1",
	"value": "65",
	"name": "新疆维吾尔",
	"pId": 2,
	"isParent": True
}, {
	"id": 6670,
	"group": "1",
	"value": "66",
	"name": "新疆建设兵团",
	"pId": 2,
	"isParent": True
}]

m = []

for i in t:
    j = {}
    j['city_code'] = i['value']
    j['city_name'] = i['name']
    m.append(j)
print(len(t))
print(len(m))
print(m)

for page in range(1, 201):
    self.data['TAB_QuerySubmitPagerData'] = str(page)
