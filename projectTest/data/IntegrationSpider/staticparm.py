# encoding: utf-8
import os
from Env.ParseYaml import FileConfigParser
from SpiderTools.Tool import platform_system

root_path = os.path.dirname(__file__)

img_dir = root_path + FileConfigParser().get_path(server=platform_system(), key='img')
pdf_dir = root_path + FileConfigParser().get_path(server=platform_system(), key='pdf')
word_dir = root_path + FileConfigParser().get_path(server=platform_system(), key='word')

max_duplicate = 10000


