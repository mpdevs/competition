# coding: utf-8
# __author__ = "John"
from collections import OrderedDict
from os import path, sys
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from common.settings import DEBUG


def debug(var):
    if DEBUG:
        print(var)


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
    for attribute in attributes[1: -1].split(u","):
        name_value_pair = attribute.split(u":")
        if name_value_pair[0] in od.keys():
            od[name_value_pair[0]].append(name_value_pair[1])
        else:
            od[name_value_pair[0]] = [name_value_pair[1]]
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
            debug(u"商品之间有维度相交, 都必要维度无交集, 我不管")
            return True
        # 1.2 只有一个商品有必要维度相交
        elif (a1_tag_intersection and not a2_tag_intersection) or (a2_tag_intersection and not a1_tag_intersection):
            debug(u"商品之间有维度相交, 只有一个商品有必要维度相交, 异位")
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
                        debug(u"商品之间有维度相交, 同维度下有不同的维度值, 异位")
                        return False
                # 1.32 所有维度值相等
                debug(u"商品之间有维度相交, 两个商品的所有维度值相等, 我不管")
                return True
                # 1.32 判断每个属性下面是否所有值都相等
            # 1.4 两个商品的必要维度不等
            else:
                debug(u"商品之间有维度相交, 两个商品的必要维度不等, 异位")
                return False
    # 2. 商品之间没有维度相交
    else:
        # 2.1 都没有必要维度
        if not a1_tag_intersection and not a2_tag_intersection:
            debug(u"商品之间没有维度相交, 都没有必要维度, 我不管")
            return True
        # 2.2 有一个有必要维度
        elif (a1_tag_intersection and not a2_tag_intersection) or (a2_tag_intersection and not a1_tag_intersection):
            debug(u"商品之间没有维度相交, 有一个有必要维度, 异位")
            return False
        # 2.3 都有必要维度
        else:
            debug(u"商品之间没有维度相交, 都有必要维度, 异位")
            return False


if __name__ == u"__main__":
    from datetime import datetime
    _attr_list = [u",风格:通勤,款式品名:小背心/小吊带,图案:纯色,领型:圆领,颜色分类:花色,颜色分类:黑色,颜色分类:白色,颜色分类:深灰,颜色分类:浅灰,颜色分类:灰色,颜色分类:黄色,颜色分类:蓝色,颜色分类:绿色,颜色分类:西瓜红,颜色分类:玫红,颜色分类:红色,上市年份季节:2015年秋季,组合形式:单件,厚薄:厚,厚薄:薄,厚薄:适中,通勤:韩版,穿着方式:套头,衣长:常规,适用季节:春,适用季节:夏,适用季节:秋,适用季节:春夏,",
                  u",款式品名:卫衣/绒衫,服装款式细节:口袋,领型:半开领,服饰工艺:立体裁剪,颜色分类:紫色,颜色分类:黄色,颜色分类:酒红,颜色分类:红色,颜色分类:红黑,上市年份季节:2014年秋季,组合形式:两件套,厚薄:厚,厚薄:薄,厚薄:适中,袖长:长袖,裤长:长裤,面料:棉,面料:聚酯,衣门襟:拉链,适用年龄:35-39周岁,袖型:常规,服装版型:宽松,穿着方式:开衫,衣长:中长款,适用季节:春,适用季节:秋,"]

    print u"{0}开始测试tag_to_dict".format(datetime.now())
    print u"{0}开始测试attributes_to_dict".format(datetime.now())
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

    print u"{0}开始测试make_similarity_feature".format(datetime.now())
    _tag_dict = {u"a": [u"我", u"你"], u"b": [u"你"], u"c": [u"他"], u"d": [u"啥"]}
    fv = make_similarity_feature(_attr1, _attr2, _tag_dict)
    print fv
