# coding: utf-8
# __author__ = "John"
from os import path
import sys
import jieba
import re
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from db_apis import get_color


def tag_setter(parsed_attr_desc_list):
    """
    把dict格式的标签转换成unicode类型
    :param parsed_attr_desc_list: list(dict)
    :return: list(unicode)
    """
    for i in xrange(len(parsed_attr_desc_list)):
        new_format = u","
        for dimension, value_list in parsed_attr_desc_list[i].iteritems():
            if value_list:
                for value in value_list:
                    new_format += u"{0}:{1},".format(dimension, value)
        if len(new_format) <= 2:
            new_format = u""
        parsed_attr_desc_list[i] = new_format
    return parsed_attr_desc_list


def brand_unify(brand_name, brand_list):
    if brand_list:
        for brand in brand_list:
            if brand_name in brand:
                for nick_name in brand[1].split(u","):
                    if unicode_decoder(nick_name) == brand_name:
                        return brand[0]
        return brand_name
    else:
        return brand_name


def tag_fuzzy_attr(documents, library):
    return


def tag_multiple_value(documents, library):
    return


def unicode_decoder(string):
    try:
        string = strip(string).encode(u"latin-1")
    except UnicodeEncodeError:
        return strip(string)
    try:
        string = strip(string).decode(u"utf-8")
        return string
    except UnicodeDecodeError:
        pass
    try:
        string = strip(string).decode(u"gbk")
        return string
    except UnicodeDecodeError:
        pass
    try:
        string = strip(string).decode(u"gb2312")
        return string
    except UnicodeDecodeError:
        # return "UnknownEncoding" + original_string
        return strip(string).decode(u"latin-1")


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


def export_excel(data, category, category_id):
    import pandas as pd
    df = pd.DataFrame(list(data), columns=[u"ItemID", u"DisplayName", u"AttrValue"])
    df.to_excel(
        excel_writer=pd.ExcelWriter(u"{0}.xlsx".format(category)), sheet_name=str(category_id), encoding=u"utf-8"
    )
    return


def generate_color_dict():
    data = get_color().values.tolist()
    dump_words = []
    word_frequency = u"1"
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


def color_cut(string):
    punctuations_string = ur"[\s+\.\!\/_,$%^*(+\"\']+|[+——！，。？、~@#￥%……&*（）]+"
    punctuations_removed = re.sub(punctuations_string, u"", strip(string))
    ret = []
    for word in list(jieba.cut(punctuations_removed)):
        if u"色" in word:
            ret.append(word)
    return set(ret)


if __name__ == u"__main__":
    generate_color_dict()
    s = u"酒红色（三件套）蓝色（三件套）浅灰色（三件套）"
    for i in color_cut(s):
        print i
