import random
import redis
from config import *


class RedisClient(object):
    def __init__(self, website, block, host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD):
        """
        初始化Redis连接
        :param host: 地址
        :param port: 端口
        :param password: 密码
        """
        self.db = redis.StrictRedis(host=host, port=port, password=password, decode_responses=True,db=10)
        # self.db_ = redis.Redis(host=host, port=port, password=password, decode_responses=True)
        self.block = block
        self.website = website

    def name(self):
        """
        获取 set 的名称
        :return: Hash名称
        """
        return f"{self.website}:{self.block}"

    def set(self, value):
        """
        添加元素
        :param value: MD5 加密的values
        :return:
        """

        if isinstance(value, list):
            return eval(f'self.db.sadd(self.name(), {",".join(value)})')
        return self.db.sadd(self.name(), value)

    def get(self,):
        """
        根据键名获取值
        :return:
        """
        return self.db.smembers(self.name())

    def expires(self):
        '''
        设置键的过期时间
        :return:
        '''
        return self.db.expire(self.name(), CYCLE)

    def delete(self,):
        """
        根据键名删除键值
        :return: 随机元素
        """
        return self.db.spop(self.name())

    def isExist(self, value):
        """
        判断元素是否存在
        :return: 数目
        """
        return self.db.sismember(self.name(), value)


if __name__ == '__main__':
    conn = RedisClient('cookies', 'linkedin')
    conn.set(['1', '2'])
    result = conn.isExist('1')
    print(conn.get())
    print(result)
