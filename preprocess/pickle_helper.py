# -*- coding: utf-8 -*-
"""
Created on Mar 22, 2016

@author: John Pan
"""
import pickle


# 导出pickle文件，自动补充 .pickle 的后缀
def pickle_dump(file_name, dump_object):
    if '.pickle' in file_name:
        pickle_name = file_name
    else:
        pickle_name = file_name + '.pickle'

    f = open(pickle_name, 'wb')
    pickle.dump(dump_object, f)
    f.close()
    return


# 导入pickle文件，自动补充 .pickle 的后缀
def pickle_load(file_name):
    if '.pickle' in file_name:
        pickle_name = file_name
    else:
        pickle_name = file_name + '.pickle'

    f = open(pickle_name, 'rb')
    load_object = pickle.load(f)
    f.close()
    return load_object


# 单元测试case
if __name__ == '__main__':
    # 当前文件夹操作
    dump_object = 'test'
    dump_file = 'test'
    pickle_dump(dump_file, dump_object)
    read_file = pickle_load(dump_file)
    print read_file
    dump_object = 'test2'
    dump_file = 'test2.pickle'
    pickle_dump(dump_file, dump_object)
    read_file = pickle_load(dump_file)
    print read_file
    # 跨文件夹操作
    # import sys
    # import os
    # sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../'))
