from scrapy import cmdline
from multiprocessing import Process

spiderList = ['supplyLandPlan', 'dealFormula', 'transferNotice', 'supplyResult']

if __name__ == '__main__':
    cmdline.execute(['scrapy', 'crawl', 'supplyLandPlan'])
    # process_list = []
    # for i in range(len(spiderList)):
    #     p = Process(target=cmdline.execute, args=(['scrapy', 'crawl', spiderList[i]],))  # 实例化进程对象
    #     p.start()
    #     process_list.append(p)
    #
    # for i in process_list:
    #     i.join()
