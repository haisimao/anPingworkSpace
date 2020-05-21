"""__author__=李佳林"""
import pymysql

# 获取数据库连接
def get_connection():
    host = '127.0.0.1'
    port = 3306
    user = 'root'
    password = '123456'
    database = 'tongcheng'
    con = pymysql.connect(host, user, password, database, charset='utf8', port=port)
    return con

# 获取游标
def get_cursor(con):
    return con.cursor()


# 关闭连接
def close_connection(con):
    con.close()


