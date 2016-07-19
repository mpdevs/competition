# -*- coding: utf-8 -*-
# __author__ = 'Dragon'
import numpy as np
import json
import random
from tqdm import tqdm
from collections import OrderedDict
from enums import DEBUG


def debug(var):
    if DEBUG:
        print (var)


def tag_to_dict(df):
    """
    用于竟品计算
    :param df: DataFrame
    :return: dict(key=CID, value=dict(key=AttrName, value=AttrValue list))
    """
    cid_list = df.CID.unique().tolist()
    tag_dict = OrderedDict()
    for cid in cid_list:
        attr_name_value_dict = OrderedDict()
        for row in df[df.CID == cid].values.tolist():
            attr_name_value_dict[row[1]] = row[2].split(',')
        tag_dict[cid] = attr_name_value_dict
    return tag_dict


def make_similarity_feature(attr1, attr2, tag_dict):
    """
    计算相似度的方法
    需要把两个已经转换好格式的属性以及需要用来计算竞品的标签
    竞品的标签必须是一个排序字典，用于后续的训练训练模型生成
    计算步骤：
        遍历每个维度
            查看两个商品之间和维度词典中是否有对应的维度
                如果有，则计算这些维度值的jaccard相似度: 交集数 / 并集数
                否则， 直接为0
    :param attr1: dict(key=unicode, value=list)
    :param attr2: dict(key=unicode, value=list)
    :param tag_dict: OrderedDict(key=unicode, value=list)
    :return: list
    """
    feature = []
    for dimension, value_list in tag_dict.iteritems():
        try:
            set(value_list)
        except TypeError as e:
            print u"tag_dict element type error, error_message={0}".format(str(e))
            return
        try:
            set1 = set(value_list) & set(attr1[dimension])
            set2 = set(value_list) & set(attr2[dimension])
        except KeyError:
            feature.append(0.0)
            continue
        try:
            feature.append(float(len(set1 & set2)) / len(set1 | set2))
        except ZeroDivisionError:
            feature.append(0.0)
            continue
    return feature


def attributes_to_dict(attributes):
    """
    attributes是数据库里商品已经打好的标签
    :param attributes: unicode
    :return: OrderedDict
    """
    od = OrderedDict()
    for attribute in attributes[1: -1].split(','):
        name_value_pair = attribute.split(':')
        if name_value_pair[0] in od.keys():
            od[name_value_pair[0]].append(name_value_pair[1])
        else:
            od[name_value_pair[0]] = [name_value_pair[1]]
    return od


def construct_feature(attr1, attr2, tag_dict):
    """
    根据品类-属性-值的字典返回两个商品之间的相似度向量
    每个维度就是属性
    :param attr1: unicode
    :param attr2: unicode
    :param tag_dict: OrderedDict
    :return: list
    """
    attr1, attr2 = attributes_to_dict(attr1), attributes_to_dict(attr2)
    return make_similarity_feature(attr1=attr1, attr2=attr2, tag_dict=tag_dict)


def construct_train_feature(raw_data, tag_dict):
    """
    构造训练数据特征, 维度为某个属性的相似度
    :param raw_data: DataFrame
    :param tag_dict: OrderedDict
    :return: numpy.array, numpy.array
    """
    x_set = []
    y_set = []
    for row in tqdm(raw_data):
        feature_vector = construct_feature(attr1=row[0], attr2=row[1], tag_dict=tag_dict)
        x_set.append(feature_vector)
        y_set.append(row[2])
    return np.asarray(x_set), np.asarray(y_set)


def sample_balance(train_x, train_y):
    """
    将 y < 0.5 和 y > 0.5 以 1: 1 的比例返回
    :param train_x: numpy.array
    :param train_y: numpy.array
    :return: numpy.array
    """
    t = np.array(xrange(len(train_y)))
    t = random.sample(t[train_y < 0.5], sum(train_y > 0.5)) + t[train_y > 0.5].tolist()
    t = random.sample(t, len(t))
    train_x = train_x[t]
    train_y = train_y[t]
    return train_x, train_y


def construct_prediction_feature(customer_data, competitor_data, tag_dict, essential_tag_dict):
    """
    构造预测数据特征, 维度为两个商品在所有相关维度上的相似度
    :param customer_data: DataFrame
    :param competitor_data: DataFrame
    :param tag_dict: OrderedDict
    :param essential_tag_dict: OrderedDict
    :return: numpy.array, tuple(long, long)
    """
    x_set = []
    item_pair = []
    # ItemID, TaggedItemAttr, DiscountPrice, CategoryID
    for customer_item in tqdm(customer_data):
        # ItemID, TaggedItemAttr, DiscountPrice, CategoryID
        for competitor_item in competitor_data:
            # 价格区间筛选
            lower_bound, upper_bound = 0.8 * customer_item[2], 1.2 * customer_item[2]
            if float(competitor_item[2]) < lower_bound or float(competitor_item[2]) > upper_bound:
                continue
            # 品类筛选 chunk 操作
            if competitor_item[3] != customer_item[3]:
                continue
            # 必要维度法
            attr1, attr2 = attributes_to_dict(competitor_item[1]), attributes_to_dict(customer_item[1])
            if not essential_dimension_trick(attr1=attr1, attr2=attr2, essential_tag_dict=essential_tag_dict):
                continue
            # 都没问题才纳入预测数据返回
            feature_vector = make_similarity_feature(attr1=attr1, attr2=attr2, tag_dict=tag_dict)
            x_set.append(feature_vector)
            item_pair.append((customer_item[0], competitor_item[0]))
    return np.asarray(x_set), item_pair


def parse_essential_dimension(df):
    od = OrderedDict()
    for row in df.values.tolist():
        if row[0] in od.keys():
            if row[1] in od[row[0]].keys():
                od[row[0]][row[1]].append(row[2])
            else:
                od[row[0]][row[1]] = [row[2]]
        else:
            dimension_value = OrderedDict()
            dimension_value[row[1]] = [row[2]]
            od[row[0]] = dimension_value
    return od


def essential_dimension_trick(attr1, attr2, essential_tag_dict):
    """
    必要维度法，用于竞品计算的时候过滤一些同属性，不同值的情况
    返回True的话就开始构造特征
    返回False的话就跳过
    :param attr1: unicode
    :param attr2: unicode
    :param essential_tag_dict: OrderedDict
    :return: boolean
    """
    if not essential_tag_dict:
        return True
    a1_a2_intersection = set(attr1.keys()) & set(attr2.keys())
    a1_tag_intersection = set(attr1.keys()) & set(essential_tag_dict.keys())
    a2_tag_intersection = set(attr2.keys()) & set(essential_tag_dict.keys())
    public_intersection = a1_tag_intersection & a2_tag_intersection

    # 1. 商品之间有维度相交
    if a1_a2_intersection:
        # 1.1 都和必要维度无交集
        if not a1_tag_intersection and not a2_tag_intersection:
            debug(u'商品之间有维度相交, 都必要维度无交集, 我不管')
            return True
        # 1.2 只有一个商品有必要维度相交
        elif (a1_tag_intersection and not a2_tag_intersection) or (a2_tag_intersection and not a1_tag_intersection):
            debug(u'商品之间有维度相交, 只有一个商品有必要维度相交, 异位')
            return False
        # 两个商品都有必要维度
        else:
            # 1.3 两个商品的必要维度相等
            if a1_tag_intersection == a2_tag_intersection:
                for key in public_intersection:
                    # print "str(attr1[key])=%s, str(attr2[key])=%s" % (str(attr1[key]), str(attr2[key]))
                    # 1.30 判断每个属性下面是否所有值都相等
                    if set(attr1[key]) == set(attr2[key]):
                        continue
                    # 1.31 同维度下有不同的维度值
                    else:
                        debug(u'商品之间有维度相交, 同维度下有不同的维度值, 异位')
                        return False
                # 1.32 所有维度值相等
                debug(u'商品之间有维度相交, 两个商品的所有维度值相等, 我不管')
                return True
                # 1.32 判断每个属性下面是否所有值都相等
            # 1.4 两个商品的必要维度不等
            else:
                debug(u'商品之间有维度相交, 两个商品的必要维度不等, 异位')
                return False
    # 2. 商品之间没有维度相交
    else:
        # 2.1 都没有必要维度
        if not a1_tag_intersection and not a2_tag_intersection:
            debug(u'商品之间没有维度相交, 都没有必要维度, 我不管')
            return True
        # 2.2 有一个有必要维度
        elif (a1_tag_intersection and not a2_tag_intersection) or (a2_tag_intersection and not a1_tag_intersection):
            debug(u'商品之间没有维度相交, 有一个有必要维度, 异位')
            return False
        # 2.3 都有必要维度
        else:
            debug(u'商品之间没有维度相交, 都有必要维度, 异位')
            return False


# region 废弃函数
def tag_to_matrix(attr_list, tag_dict, cid):
    """
    废弃
    :param attr_list:  json_list: dict(unicode: dict(unicode: list))
    :param tag_dict:  dict(unicode: int)
    :return: np.array
    """
    # 创建一个矩阵， 大小是 数据的行数 * 维度:值个数 (1000多维)
    dimension_size = 0
    for value in tag_dict[cid].keys():
        dimension_size += len(tag_dict[cid])

    tag_matrix = np.zeros((len(attr_list), len(tag_dict)))
    for index, string in enumerate(attr_list):
        # 去掉数据头尾的逗号，将原来数据的格式的分割符从冒号换成减号
        attr_set = set(string[1:-1].replace(':', '-').split(','))
        for attr in attr_set:
            if attr != '':
                try:
                    tag_matrix[index - 1][tag_dict[attr]] = 1
                except KeyError:
                    # print u'Tag key error ,key is {}'.format(attr)
                    continue
                except IndexError:
                    print 'Index Error shape tag_matrix is {0},index = {1}'.format(np.shape(tag_matrix), index)
                    continue
    return tag_matrix


def jaccard(u, v):
    """
    废弃
    :param u:
    :param v:
    :return:
    """
    t = np.bitwise_or(u != 0, v != 0)
    q = t.sum()
    if q == 0:
        return 0
    return 1 - float(np.bitwise_and((u != v), t).sum()) / q


def w_jaccard(x, y, cut, weights):
    """
    废弃
    :param x:
    :param y:
    :param cut:
    :param weights:
    :return:
    """
    result = 0
    n0 = len(cut[0])
    n1 = len(cut[1])
    a0 = weights[0]/n0

    for i in xrange(n0):
        t = cut[0][i]
        result += jaccard(x[t], y[t])
    result *= a0
    
    c = 0.0
    r = 0
    for i in xrange(n1):
        t = cut[1][i]
        if np.bitwise_and(x[t] != 0, y[t] != 0).sum() != 0:
            c += 1
            r += jaccard(x[t], y[t])
    
    if c == 0:
        return result
    else:
        return result + min(weights[1]/c, a0) * r


def label_rebuilt_to_set(unicode_string):
    """
    废弃
    :param unicode_string:
    :return:
    """
    j = json.loads(unicode_string.replace("'", '"'))
    j_list = []
    for k, v in j.iteritems():
        if isinstance(v, list):
            for i in v:
                j_list.append(k + i)
        else:
            j_list.append(k + v)
    return set(j_list)


def db_json_to_python_json(json_string):
    """
    废弃
    :param json_string:
    :return:
    """
    return json.loads(json_string.replace("'", '"'))


def string_or_unicode_to_list(string_or_unicode):
    """
    废弃
    :param string_or_unicode:
    :return:
    """
    if isinstance(string_or_unicode, str):
        ret_list = string_or_unicode.split('\r\n')
    elif isinstance(string_or_unicode, unicode):
        ret_list = string_or_unicode.split('\r\n')
    else:
        ret_list = string_or_unicode
    return ret_list


def tag_process(attr_name, attr_value):
    """
    废弃
    :param attr_name: unicode 维度，单值
    :param attr_value: unicode 维度值 值1,值2,值3
    :return:
    """
    wrapper = zip(attr_name, attr_value)
    attr_list = []
    for row in wrapper:
        for attr in row[1].split(u","):
            attr = u'{0}-{1}'.format(row[0], attr)
            attr_list.append(attr)
    tag_list = list(set(attr_list))
    tag_dict = OrderedDict()
    for i in xrange(len(tag_list)):
        tag_dict[tag_list[i]] = i
    print u"tag_process finished len(tag_list) = {0}".format(len(tag_list))
    return tag_list, tag_dict


def get_cut(include_tag_list, tag_list):
    """
    废弃
    :param include_tag_list: list(list) 需要用到的维度，去重值
    :param tag_list: list 所有的维度
    :return: list(list(list))
    """
    result = []
    print u'get_cut start, len(include_tag_list)={0}, len(tag_list)={0}'.format(len(include_tag_list), len(tag_list))
    for index, unique_attr_name in enumerate(include_tag_list):
        t = []
        for x in unique_attr_name:
            t.append([])
            for j in xrange(len(tag_list)):
                if tag_list[j].find(x)+1:
                    t[-1].append(j)
            if not t[-1]:
                del t[-1]
        result.append(t)
    return result[0]


def parser_label(json_list, dict_head):
    """
    废弃
    :param json_list:  json_list: dict(unicode: dict(unicode: list))
    :param dict_head:  dict(unicode: int)
    :return: np.array
    """
    a_list = []
    # json_list : {'a': [1,2], 'b': 3}
    for x in json_list:
        # d 数据类型 dict
        d = json.loads(x.replace("'", '"'))
        t = []
        # i 是L2，y是L3
        for i, y in d.iteritems():
            if isinstance(y, list):
                t += [i+"-"+z for z in y]
            else:
                t.append(i+"-"+y)
        a_list.append(",".join(t))

    # size 数据行数 * 维度数量
    label = np.zeros((len(a_list), len(dict_head)))
    # df = pd.read_excel(os.path.dirname(__file__)+'/Combine.xlsx', encoding='utf8')#t,o

    for i, t in enumerate(a_list):
        # for j in df.index:
            # t = t.replace(df['o'][j], df['t'][j])
        for x in set(t.split(",")):
            if x != "":
                label[i][dict_head[x]] = 1
    return label
# endregion


def parse_raw_desc(attr_desc_list):
    """
    将爬虫获取的字符串数据转换成字典的格式
    :param attr_desc_list:
    :return:
    """
    error_list = []
    processed_list = []
    for row in tqdm(attr_desc_list):
        row = row.split(u"，")
        attr_dict = dict()
        for col in row:
            spl = col.split(u":")
            try:
                key = spl[0]
                value = spl[1]
            except IndexError:
                error_list.append(u"index error")
                continue
            try:
                if key in attr_dict.keys():
                    attr_dict[key].append(value)
                else:
                    attr_dict[key] = [value]
            except KeyError:
                error_list.append(u"key error")
                continue
        processed_list.append(attr_dict)
    return processed_list


def tag_setter(processed_list):
    """
    把dict格式的标签转换成unicode类型
    :param processed_list: list(dict)
    :return: list(unicode)
    """
    for i in xrange(len(processed_list)):
        new_format = u","
        for dimension, value in processed_list[i].iteritems():
            new_format += u"{0}:{1},".format(dimension, value)
        if len(new_format) <= 2:
            new_format = u""
        processed_list[i] = new_format
    return


def tag_fuzzy_attr(documents, library):
    return


def tag_multiple_value(documents, library):
    return


if __name__ == "__main__":
    from datetime import datetime
    # region 废弃函数的unit test
    # test_string = u"{'att-图案': '花式', 'att-适用季节': '秋季', " \
    #     u"'att-色系': '多色', 'app-感官': ['优雅', '淑女', '时尚'], " \
    #     u"'att-面料': '蕾丝', 'att-做工工艺': '拼接', " \
    #     u"'att-款式-裙长': '短裙', 'fun': '打底', " \
    #     u"'att-做工工艺-流行元素': ['镂空', '蕾丝拼接', '印花', '纱网'], " \
    #     u"'att-款式-裙型': 'A字裙', " \
    #     u"'app-风格': ['通勤', '百搭', '韩风']}"
    # label_rebuilt_to_set(test_string)
    #
    # print u'{0}开始测试tag_process'.format(datetime.now())
    # _attr_name = [u'衣长', u'风格', u'款式']
    # _attr_value = [u'超短,短款,常规,中长款,长款', u'原创设计,甜美,百搭,街头,通勤', u'工字型,裹胸,挂脖式,斜肩,背带,吊带']
    # _tag_list, _tag_dict = tag_process(attr_name=_attr_name, attr_value=_attr_value)
    # for a in _tag_list:
    #     print (a)
    # for key, value in _tag_dict.iteritems():
    #     print (u"{0}:{1}".format(key, value))
    #
    # print u'{0}开始测试tag_parse'.format(datetime.now())

    # print u'{0}开始测试get_cut'.format(datetime.now())
    # _include_tag_list = [[u'衣长', u'风格']]
    # cut = get_cut(_include_tag_list, _tag_list)
    # print cut
    # endregion

    _attr_list = [u",风格:通勤,款式品名:小背心/小吊带,图案:纯色,领型:圆领,颜色分类:花色,颜色分类:黑色,颜色分类:白色,颜色分类:深灰,颜色分类:浅灰,颜色分类:灰色,颜色分类:黄色,颜色分类:蓝色,颜色分类:绿色,颜色分类:西瓜红,颜色分类:玫红,颜色分类:红色,上市年份季节:2015年秋季,组合形式:单件,厚薄:厚,厚薄:薄,厚薄:适中,通勤:韩版,穿着方式:套头,衣长:常规,适用季节:春,适用季节:夏,适用季节:秋,适用季节:春夏,",
                  u",款式品名:卫衣/绒衫,服装款式细节:口袋,领型:半开领,服饰工艺:立体裁剪,颜色分类:紫色,颜色分类:黄色,颜色分类:酒红,颜色分类:红色,颜色分类:红黑,上市年份季节:2014年秋季,组合形式:两件套,厚薄:厚,厚薄:薄,厚薄:适中,袖长:长袖,裤长:长裤,面料:棉,面料:聚酯,衣门襟:拉链,适用年龄:35-39周岁,袖型:常规,服装版型:宽松,穿着方式:开衫,衣长:中长款,适用季节:春,适用季节:秋,"]

    print u'{0}开始测试tag_to_dict'.format(datetime.now())
    from mysql_helper import connect_db
    from enums import ATTR_META_QUERY
    import pandas as pd
    industry = "mp_women_clothing"
    attr_meta = pd.read_sql_query(ATTR_META_QUERY.format(industry), connect_db(industry))
    cut = tag_to_dict(attr_meta[["CID", "Attrname", "AttrValue"]])
    _cid = 50000671
    _attr_name = u'颜色分类'

    print u'{0}开始测试attributes_to_dict'.format(datetime.now())
    a1 = u",a:我,a:你,b:你,"
    a2 = u",c:他,a:我,"
    _attr1 = attributes_to_dict(a1)
    _attr2 = attributes_to_dict(a2)
    print str(_attr1)
    print str(_attr2)
    for k, v in _attr1.iteritems():
        print k, v
    for k, v in _attr2.iteritems():
        print k, v

    print u'{0}开始测试make_similarity_feature'.format(datetime.now())
    _tag_dict = {"a": [u"我", u"你"], "b": [u"你"], "c": [u"他"], "d": [u"啥"]}
    fv = make_similarity_feature(_attr1, _attr2, _tag_dict)
    print fv

    print u'{0}parse_essential_dimension'.format(datetime.now())
    from db_apis import get_essential_dimensions
    d = parse_essential_dimension(get_essential_dimensions(db="mp_women_clothing"))
    print d.keys()

