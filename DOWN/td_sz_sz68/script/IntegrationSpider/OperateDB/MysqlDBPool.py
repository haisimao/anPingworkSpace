#!/usr/bin/env python
# -*- coding:utf-8 -*-
import pymysql, os, configparser
from pymysql.cursors import DictCursor
from DBUtils.PooledDB import PooledDB


class Config(object):
    """
    类似字典套字典
    Config().get_content("user_information")

    配置文件里面的参数
    [notdbMysql]
    host = 192.168.1.101
    port = 3306
    user = root
    password = python123
    """

    def __init__(self, config_filename="Config.cnf", *args, **kwargs):
        file_path = os.path.join(os.path.dirname(__file__), config_filename)
        self.cf = configparser.ConfigParser()
        self.cf.read('./Config.cnf')
        super(Config, self).__init__(*args, **kwargs)

    def get_sections(self):
        # 返回模块的值
        return self.cf.sections()

    def get_options(self, section=None):
        section = section if section else self.get_sections()[0]
        return self.cf.options(section)

    def get_content(self, section=None):
        result = {}
        section = section if section else self.get_sections()[0]
        for option in self.get_options():
            value = self.cf.get(section, option)
            result[option] = int(value) if value.isdigit() else value
        return result


class BasePymysqlPool(object):

    def __init__(self, host, port, user, password, database=None):
        self.db_host = host
        self.db_port = int(port)
        self.user = user
        self.password = str(password)
        self.database = database
        self.conn = None
        self.cursor = None


class MyPymysqlPool(BasePymysqlPool):
    """
    MYSQL数据库对象，负责产生数据库连接 , 此类中的连接采用连接池实现获取连接对象：conn = Mysql.getConn()
            释放连接对象;conn.close()或del conn
    """
    # 连接池对象
    __pool = None

    def __init__(self, conf_name=None):
        #  返回文件配置 {'host': '172.22.69.43', 'port': 3306, 'user': 'spider', 'password': 'spider#O2018', 'database': 'spider'}
        self.conf = Config().get_content(conf_name)
        super(MyPymysqlPool, self).__init__(**self.conf)  # 传入父类的实例化参数
        # 数据库构造函数，从连接池中取出连接，并生成操作游标
        self._conn = self.__getConn()
        self._cursor = self._conn.cursor()

    def __getConn(self):
        """
        @summary: 静态方法，从连接池中取出连接
        @return MySQLdb.connection
        """
        if MyPymysqlPool.__pool is None:
            __pool = PooledDB(creator=pymysql,
                              mincached=1,
                              maxcached=1000,
                              host=self.db_host,
                              port=self.db_port,
                              user=self.user,
                              passwd=self.password,
                              database=self.database,
                              use_unicode=True,
                              charset="utf8",  # 指定字符编码
                              autocommit=True,
                              cursorclass=pymysql.cursors.DictCursor,)
        return __pool.connection()

    # 不获取返回值的查询
    def query(self, sql, param=None):
        if param is None:
            count = self._cursor.execute(sql)
        else:
            count = self._cursor.execute(sql, param)
        return count

    # 有返回值得查询
    def getAll(self, sql, param=None):
        """
        @summary: 执行查询，并取出所有结果集
        @param sql:查询ＳＱＬ，如果有查询条件，请只指定条件列表，并将条件值使用参数[param]传递进来
        @param param: 可选参数，条件列表值（元组/列表）
        @return: result list(字典对象)/boolean 查询到的结果集
        """
        if param is None:
            count = self._cursor.execute(sql)
        else:
            count = self._cursor.execute(sql, param)
        if count > 0:
            result = self._cursor.fetchall()
        else:
            result = False
        return result  #

    # 有返回值得查询
    def getOne(self, sql, param=None):
        """
        @summary: 执行查询，并取出第一条
        @param sql:查询ＳＱＬ，如果有查询条件，请只指定条件列表，并将条件值使用参数[param]传递进来
        @param param: 可选参数，条件列表值（元组/列表）
        @return: result list/boolean 查询到的结果集
        """
        if param is None:
            count = self._cursor.execute(sql)
        else:
            count = self._cursor.execute(sql, param)
        if count > 0:
            result = self._cursor.fetchone()
        else:
            result = False
        return result

    # 有返回值得查询
    def getMany(self, sql, num, param=None):
        """
        @summary: 执行查询，并取出num条结果
        @param sql:查询ＳＱＬ，如果有查询条件，请只指定条件列表，并将条件值使用参数[param]传递进来
        @param num:取得的结果条数
        @param param: 可选参数，条件列表值（元组/列表）
        @return: result list/boolean 查询到的结果集
        """
        if param is None:
            count = self._cursor.execute(sql)
        else:
            count = self._cursor.execute(sql, param)
        if count > 0:
            result = self._cursor.fetchmany(num)
        else:
            result = False
        return result

    # 批量插入
    def insertMany(self, sql, values):
        """
        @summary: 向数据表插入多条记录
        @param sql:要插入的ＳＱＬ格式
        @param values:要插入的记录数据tuple(tuple)/list[list]
        @return: count 受影响的行数
        """
        count = self._cursor.executemany(sql, values)
        return count

    # 单条数据插入
    def insert(self, sql, param=None):
        """
        @summary: 更新数据表记录
        @param sql: ＳＱＬ格式及条件，使用(%s,%s)
        @param param: 要更新的  值 tuple/list
        @return: count 受影响的行数
        """
        return self.query(sql, param)

    # 更新数据表记录
    def update(self, sql, param=None):
        """
        @summary: 更新数据表记录
        @param sql: ＳＱＬ格式及条件，使用(%s,%s)
        @param param: 要更新的  值 tuple/list
        @return: count 受影响的行数
        """
        return self.query(sql, param)

    def delete(self, sql, param=None):
        """
        @summary: 删除数据表记录
        @param sql: ＳＱＬ格式及条件，使用(%s,%s)
        @param param: 要删除的条件 值 tuple/list
        @return: count 受影响的行数
        """
        return self.query(sql, param)

    def begin(self):
        """
        @summary: 开启事务
        """
        self._conn.autocommit(0)

    def end(self, option='commit'):
        """
        @summary: 结束事务
        """
        if option == 'commit':
            self._conn.commit()
        else:
            self._conn.rollback()

    def dispose(self, isEnd=1):
        """
        @summary: 释放连接池资源
        """
        if isEnd == 1:
            self.end('commit')
        else:
            self.end('rollback')
        self._cursor.close()
        self._conn.close()


if __name__ == '__main__':
    # mysql = MyPymysqlPool("notdbMysql")
    #
    # sqlAll = "select * from myTest.aa;"
    # result = mysql.getAll(sqlAll)
    # print(result)
    #
    # sqlAll = "select * from myTest.aa;"
    # result = mysql.getMany(sqlAll, 2)
    # print(result)
    #
    # result = mysql.getOne(sqlAll)
    config = Config()
    # print(config.get_options(config.get_sections()))
    # print(config.get_content())  spi_auto_login
    print(MyPymysqlPool().getAll('''select count(*) from spi_captcha'''))
    # mysql.insert("insert into myTest.aa set a=%s", (1))

    # 释放资源
    # mysql.dispose()