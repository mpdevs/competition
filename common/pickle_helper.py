# coding: utf-8
# __author__: "John"
import pickle


# 导出pickle文件，自动补充 .pickle 的后缀
def pickle_dump(file_name, dump_object):
    if u".pickle" in file_name:
        pickle_name = file_name
    else:
        pickle_name = file_name + u".pickle"

    f = open(pickle_name, u"wb")
    pickle.dump(dump_object, f)
    f.close()
    return


# 导入pickle文件，自动补充 .pickle 的后缀
def pickle_load(file_name):
    if u".pickle" in file_name:
        pickle_name = file_name
    else:
        pickle_name = file_name + u".pickle"

    f = open(pickle_name, u"rb")
    load_object = pickle.load(f)
    f.close()
    return load_object


# 单元测试case
if __name__ == u"__main__":
    # 当前文件夹操作
    _dump_object = u"test"
    _dump_file = u"test"
    pickle_dump(_dump_file, _dump_object)
    read_file = pickle_load(_dump_file)
    print read_file
    _dump_object = u"test2"
    _dump_file = u"test2.pickle"
    pickle_dump(_dump_file, _dump_object)
    read_file = pickle_load(_dump_file)
    print read_file
    # 跨文件夹操作
    # import sys
    # import os
    # sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../"))
