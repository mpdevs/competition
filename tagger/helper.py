# coding: utf-8
# __author__ = "John"
from os import chdir
import jieba
import re
from db_apis import get_color


def format_tag(parsed_attr_desc_list):
    """
    把dict格式的标签转换成unicode类型
    :param parsed_attr_desc_list: list(dict)
    :return: list(unicode)
    """
    for i in xrange(len(parsed_attr_desc_list)):
        format_for_db = u","
        for dimension, value_list in parsed_attr_desc_list[i].iteritems():
            if value_list:
                for value in list(set(value_list)):
                    format_for_db += u"{0}:{1},".format(dimension, value)
        if len(format_for_db) <= 2:
            format_for_db = u""
        parsed_attr_desc_list[i] = format_for_db
    return parsed_attr_desc_list


def desc_maker():
    """
    把dict格式的标签转换成unicode类型
    :param : list(dict)
    :return: list(unicode)
    """
    return


def format_color_group_tag(parsed_attr_desc_list):
    """
    把dict格式的标签转换成unicode类型
    :param parsed_attr_desc_list: list(unicode)
    :return: list(unicode)
    """
    for i in xrange(len(parsed_attr_desc_list)):
        format_for_db = u","
        for dimension, value_list in parsed_attr_desc_list[i].iteritems():
            if value_list:
                for value in value_list:
                    if value:
                        format_for_db += u"{0},".format(value)
        if len(format_for_db) < 2:
            format_for_db = u""
        parsed_attr_desc_list[i] = format_for_db
    return parsed_attr_desc_list


def brand_unify(brand_name, brand_list):
    if brand_list:
        for brand in brand_list:
            if brand_name in brand:
                for nick_name in brand[1].split(u","):
                    if unicode_decoder(nick_name) == brand_name:
                        return brand[0], 0
        return brand_name, 1
    else:
        return brand_name, 2


def unicode_decoder(string):
    string = strip(string)
    try:
        string = string.encode(u"latin-1")
    except UnicodeEncodeError:
        return string
    try:
        string = string.decode(u"utf-8")
        return string
    except UnicodeDecodeError:
        pass
    try:
        string = string.decode(u"gbk")
        return string
    except UnicodeDecodeError:
        pass
    try:
        string = string.decode(u"gb2312")
        return string
    except UnicodeDecodeError:
        # return "UnknownEncoding" + original_string
        return string.decode(u"latin-1")


def strip(string):
    try:
        string.strip()
        return string.strip()
    except AttributeError:
        return string


def is_tag(tag, tag_list):
    if not tag_list:
        return True
    if tag in tag_list:
        return True
    else:
        return False


def attr_value_chunk(value):
    if isinstance(value, unicode):
        value = [strip(value)]
    else:
        pass
    return value


def export_excel(data, file_name, sheet_name, dir_path):
    import pandas as pd
    # 将文件保存至指定目录
    chdir(dir_path)
    df = pd.DataFrame(list(data), columns=[u"ItemID", u"DisplayName", u"AttrValue"])
    writer = pd.ExcelWriter(u"{0}.xlsx".format(file_name.replace(u"/", u"")), engine=u"xlsxwriter")
    df.to_excel(excel_writer=writer, sheet_name=sheet_name, encoding=u"utf-8")
    writer.save()
    writer.close()
    return


def generate_color_dict():
    data = get_color().values.tolist()
    dump_words = []
    # word_frequency = u"1"
    part_of_speech = u"n"
    for row in data:
        # 颜色组1分 模糊匹配5分 颜色匹配20分 相似匹配100分
        if row[3]:
            for color in row[3].split(u","):
                # dump_words.append(u"{0}\r\n".format(color).encode(u"utf-8"))
                dump_words.append(u"{0} {1} {2}\r".format(color, 5, part_of_speech).encode(u"utf-8"))
        if row[2]:
            for color in row[2].split(u","):
                # dump_words.append(u"{0}\r\n".format(color).encode(u"utf-8"))
                dump_words.append(u"{0} {1} {2}\r".format(color, 100, part_of_speech).encode(u"utf-8"))
        if row[1]:
            for color in row[1].split(u","):
                # dump_words.append(u"{0}\r\n".format(color).encode(u"utf-8"))
                dump_words.append(u"{0} {1} {2}\r".format(color, 20, part_of_speech).encode(u"utf-8"))
        if row[0]:
            for color in row[0].split(u","):
                # dump_words.append(u"{0}\r\n".format(color).encode(u"utf-8"))
                dump_words.append(u"{0} {1} {2}\r".format(color, 1, part_of_speech).encode(u"utf-8"))
    print len(dump_words), len(data)
    with open(u"color_dict.txt", u"wb") as f:
        f.writelines(set(dump_words))
    f.close()


def generate_color_dict_for_jieba():
    data = get_color().values.tolist()
    return


def color_cut(string):
    punctuations_string = ur"[\s+\.\!\/_,$%^*(+\"\']+|[+——！，。？、~@#￥%……&*（）]+"
    punctuations_removed = re.sub(punctuations_string, u"", strip(string))
    keep_list = [u"色", u"粉", u"红", u"黄", u"灰", u"金", u"蓝", u"绿", u"紫", u"黑", u"白"]
    ret = []
    for word in list(jieba.cut(punctuations_removed)):
        for keep in keep_list:
            if word.find(keep) > -1 and strip(word) not in [u"", u"色"]:
                ret.append(word)
    return set(ret)


if __name__ == u"__main__":
    # generate_color_dict()
    # s = u"酒红色（三件套）蓝色（三件套）浅灰色（三件套）"
    # for c in color_cut(s):
    #     print c
    # from common.settings import TAGGED_ITEMS_ATTR_LIST
    # from common.pickle_helper import pickle_load
    # tagged_items_attr_list = pickle_load(TAGGED_ITEMS_ATTR_LIST)
    # for i, l in enumerate(tagged_items_attr_list):
    #     item_id = l[1]
    #     dict_list = [l[0]]
    #     try:
    #         format_tag(dict_list)
    #     except Exception:
    #         print item_id, dict_list
    #         break
    from pandas import DataFrame
    _data = [(u"row[1,1]", u"row[1,2]", u"[1,3]"),
             (u"row[2,1]", u"row[2,2]", u"[2,3]"),
             (u"row[3,1]", u"row[3,2]", u"[3,3]")]
    _file_name = u"test_file"
    _sheet_name = u"test_sheet"
    _dir_path = ur"D:\WorkSpace\MarcPoint\mppreprocess\tagger\attribute_analyze\中老年女装"
    export_excel(data=_data, file_name=_file_name, sheet_name=_sheet_name, dir_path=_dir_path)

