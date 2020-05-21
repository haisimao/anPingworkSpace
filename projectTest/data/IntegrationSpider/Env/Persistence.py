# encoding: utf-8
from staticparm import root_path
from Env.ParseYaml import FileConfigParser
from SpiderTools.Tool import platform_system
import dbm
import time


class DBMPersistence(object):
    '''
    DBM字符串键值对持久化或者缓存持久化
    '''
    persistence_dir = root_path + FileConfigParser().get_path(server=platform_system(), key='persistence')

    def save(self, key, value, name='DBMPersistence'):
        try:
            with dbm.open('%s%s' % (self.persistence_dir,name), 'c') as dbm_file:
                dbm_file[key] = str(value)
        except:
            pass

    def load(self, name='DBMPersistence', key=None):
        try:
            with dbm.open('%s%s' % (self.persistence_dir, name), 'r') as dbm_file:
                if key in dbm_file:
                    result = str(dbm_file[key],encoding='utf-8')
                else:
                    result = None
        except:
            result = None
        return result

    def load_ip(self,name='DBMPersistence'):
        try:
            with dbm.open('%s%s' % (self.persistence_dir, name), 'r') as dbm_file:
                if list(dbm_file.keys()):
                    ip = str(list(dbm_file.keys())[0], encoding='utf-8')
                    # del dbm_file[ip]
                    return ip
                else:
                    return None

        except:
            return None

    def del_key(self, name='DBMPersistence',key=None):
        try:
            with dbm.open('%s%s' % (self.persistence_dir, name), 'r') as dbm_file:
                if key in dbm_file:
                    del dbm_file[key]
                else:
                    for key in dbm_file:
                        del dbm_file[key]
        except:
            pass

    def clear_keys(self,name='DBMPersistence'):
        try:
            with dbm.open('%s%s' % (self.persistence_dir, name), 'r') as dbm_file:
               dbm_file.clear()
        except:
            pass


    def test(self, name='DBMPersistence'):
        db = dbm.open('%s%s' % (self.persistence_dir, name), 'r')
        print(db.keys())

