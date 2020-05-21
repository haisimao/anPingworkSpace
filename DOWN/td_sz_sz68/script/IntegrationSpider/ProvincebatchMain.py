from scrapy import cmdline
from multiprocessing import Process
import sys

# 批量启动 scrapy
# string    "[.....]"
projects = eval(sys.argv[1]) if eval(sys.argv[1]) != ['supplyLandPlan', 'dealFormula', 'transferNotice', 'supplyResult'] else ['supplyLandPlan', 'dealFormula', 'transferNotice', 'supplyResult']

if __name__ == '__main__':
    process_list = []
    for i in range(len(projects)):
        p = Process(target=cmdline.execute, args=(['scrapy', 'crawl', projects[i]],))  # 实例化进程对象
        p.start()
        process_list.append(p)

    for i in process_list:
        i.join()
