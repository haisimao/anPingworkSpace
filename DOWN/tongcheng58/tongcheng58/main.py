"""__author__=李佳林"""

# from scrapy.cmdline import execute
# import os
# import sys
#
# if __name__ == '__main__':
#     sys.path.append(os.path.dirname(os.path.abspath(__file__)))
#     execute(['scrapy', 'crawl', 'ershou'])

a = [1,2,3,4,5,6,7,8]
for i in  range(0,len(a),2):
    print(a[i]+a[i+1])