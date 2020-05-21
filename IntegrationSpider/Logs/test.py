import re
import time

from scrapy import Selector

from SpiderTools.Tool import reFunction
for ID in [
            1,
           # 2,
           # 3,
           #  4,
           ]:
    with open(f'./model{ID}.html', 'r', encoding='utf-8') as fp:
        # datas = Selector(text=fp.read())
        data = Selector(text=fp.read()).xpath('string(.)').extract()[0].replace('\xa0', '').replace('\u3000', '')

        # print(re.sub(r'\s*', '', data))
        items = data
        # item = reFunction('一、[\s\S]*二、', items)
        reStr = '[（）\w\.:： —\(\)〔〕㎡㎡≤≥《》\-\/\%,；，、\.﹪]*'
        # for item in ['宗地编号' + _ for _ in re.findall('一、([\s\S]*)二、', items)[0].split('宗地编号')[1:]]:
            # print(item)

        print(data)

        for item in ['地块编号' + _ for _ in re.findall('一([\s\S]*)二、', items)[0].split('地块编号')[1:]]:
            # 地块编号
            DKBH_54 = reFunction('备注：*\s*([（）\w\.:：—\￥ (\)〔〕㎡≤≥《》\-\/\%,；，、\.﹪]*)\s', item)

            print(f'ID:{ID}最终结果:\n', DKBH_54)

