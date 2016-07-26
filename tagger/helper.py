# coding: utf-8
# __author__ = "John"
from os import path, sys
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from common.debug_helper import debug


def attr_desc_parser(attr_desc_list):
    """
    将爬虫获取的字符串数据转换成字典的格式
    :param attr_desc_list:
    :return: items_attr: list(dict(key=维度,value=维度值)), error_info: list, error_items: list
    """
    error_info = []
    error_items = []
    items_attr = []
    for item in attr_desc_list:
        attr_dict = dict()
        # 结尾逗号去除
        try:
            if item[1][-1] == u",":
                item[1] = item[1][0:-1]
            else:
                pass
        except IndexError:
            error_info.append(unicode(e))
            error_items.append(item[0])
        attr_desc = item[1].split(u",")
        for dimension_value in attr_desc:
            key_pair = dimension_value.split(u":")
            try:
                key = key_pair[0]
                value = key_pair[1]
            except IndexError as e:
                error_info.append(unicode(e))
                error_items.append(item[0])
                continue
            try:
                if key in attr_dict.keys():
                    attr_dict[key].append(value.strip())
                else:
                    attr_dict[key] = [value.strip()]
            except KeyError as e:
                error_info.append(unicode(e))
                error_items.append(item[0])
                continue
        items_attr.append(attr_dict)
    return items_attr, error_items, error_info


def tag_setter(parsed_attr_desc_list):
    """
    把dict格式的标签转换成unicode类型
    :param parsed_attr_desc_list: list(dict)
    :return: list(unicode)
    """
    for i in xrange(len(parsed_attr_desc_list)):
        new_format = u","
        for dimension, value_list in parsed_attr_desc_list[i].iteritems():
            for value in value_list:
                new_format += u"{0}:{1},".format(dimension, value)
        if len(new_format) <= 2:
            new_format = u""
        parsed_attr_desc_list[i] = new_format
    return parsed_attr_desc_list


def brand_parse():
    return


def tag_fuzzy_attr(documents, library):
    return


def tag_multiple_value(documents, library):
    return


if __name__ == "__main__":
    pass

