# coding: utf-8
# __author__ = "John"
import numpy as np
import random
import re
from tqdm import tqdm
from collections import OrderedDict
from common.debug_helper import debug


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
            attr_name_value_dict[row[1]] = row[2].split(u",")
        tag_dict[cid] = attr_name_value_dict
    return tag_dict


def make_similarity_feature(attr1, attr2, tag_dict, demo=False):
    """
    计算相似度的方法
    需要把两个已经转换好格式的属性以及需要用来计算竞品的标签
    竞品的标签必须是一个排序字典，用于后续的训练训练模型生成
    计算步骤：
        遍历每个维度
            特殊维度——材质成分
                将两个商品的材质维度做成字典，数据格式为 材质: 纯度
                    材质交集 mi = attr1.材质成分 & attr2.材质成分为空则，直接为0
                    非空，sigma(min(attr1.mi[i], attr2.mi[i]))
            查看两个商品之间和维度词典中是否有对应的维度
                如果有，则计算这些维度值的jaccard相似度: 交集数 / 并集数
                否则， 直接为0
    :param attr1: dict(key=unicode, value=list)
    :param attr2: dict(key=unicode, value=list)
    :param tag_dict: OrderedDict(key=unicode, value=list)
    :param demo: boolean
    :return: list
    """
    feature = []
    for dimension, value_list in tag_dict.iteritems():
        similarity = round(float(0), 4)
        if dimension in attr1.keys() and dimension in attr2.keys():
            pass
        else:
            if demo:
                pass
            else:
                feature.append(similarity)
                continue
        if dimension == u"材质成分":
            try:
                m1 = material_string_to_dict(attr1[dimension])
                if len(m1) == 0:
                    similarity -= 1
            except KeyError:
                similarity -= 1
            try:
                m2 = material_string_to_dict(attr2[dimension])
                if len(m2) == 0:
                    similarity -= 1
            except KeyError:
                similarity -= 1
            if similarity >= 0:
                mi = set(m1.keys()) & set(m2.keys())
                if len(mi) > 0:
                    material_similar_score = similarity
                    for i in mi:
                        material_similar_score += min(float(m1[i]), float(m2[i]))
                    feature.append(round(float(material_similar_score) / 100, 4))
                elif len(mi) == 0:
                    feature.append(similarity)
            else:
                feature.append(similarity)
            continue
        else:
            pass
        try:
            set(value_list)
        except TypeError as e:
            print u"tag_dict element type error, error_message={0}".format(str(e))
            feature.append(similarity)
            continue
        if demo:
            try:
                set1 = set(value_list) & set(attr1[dimension])
                if len(set1) == 0:
                    similarity -= 1
                else:
                    pass
            except KeyError:
                similarity -= 1
            try:
                set2 = set(value_list) & set(attr2[dimension])
                if len(set2) == 0:
                    similarity -= 1
                else:
                    pass
            except KeyError:
                similarity -= 1
            if similarity < 0:
                feature.append(similarity)
            else:
                try:
                    feature.append(round(float(len(set1 & set2)) / len(set1 | set2), 4))
                except ZeroDivisionError:
                    feature.append(similarity)
        else:
            set1 = set(value_list) & set(attr1[dimension])
            set2 = set(value_list) & set(attr2[dimension])
            try:
                feature.append(round(float(len(set1 & set2)) / len(set1 | set2), 4))
            except ZeroDivisionError:
                feature.append(similarity)
        continue
    return feature


def material_string_to_dict(material):
    material_dict = dict()
    for material_purity in material:
        purity = re.findall(ur"\d+\.?\d*", material_purity)
        if purity:
            purity = purity[0]
        else:
            continue
        material = material_purity.replace(purity, u"").replace(u"%", u"")
        if u"(" in material and u")" in material:
            material = material[material.find(u"(") + len(u"("): material.find(u")")]
        if u"（" in material and u"）" in material:
            material = material[material.find(u"（") + len(u"（"): material.find(u"）")]
        if material not in [u"其他", u"其它"]:
            material_dict.update({material: purity})
    return material_dict


def attributes_to_dict(attributes):
    """
    attributes是数据库里商品已经打好的标签
    :param attributes: unicode
    :return: OrderedDict(unicode: list(unicode))
    """
    od = OrderedDict()
    for attribute in attributes[1: -1].split(u","):
        dimension_value_pair = attribute.split(u":")
        if dimension_value_pair[0] in od.keys():
            od[dimension_value_pair[0]].append(dimension_value_pair[1])
        else:
            od[dimension_value_pair[0]] = [dimension_value_pair[1]]
    return od


def construct_feature(attr1, attr2, tag_dict, demo=False):
    """
    根据品类-属性-值的字典返回两个商品之间的相似度向量
    每个维度就是属性
    :param attr1: unicode
    :param attr2: unicode
    :param tag_dict: OrderedDict
    :param demo: boolean
    :return: list
    """
    attr1, attr2 = attributes_to_dict(attr1), attributes_to_dict(attr2)
    return make_similarity_feature(attr1=attr1, attr2=attr2, tag_dict=tag_dict, demo=demo)


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
    return np.array(x_set), np.array(y_set)


def sample_balance(train_x, train_y):
    """
    将 y <= 0.5 和 y > 0.5 以 1: 1 的比例返回
    :param train_x: numpy.array
    :param train_y: numpy.array
    :return: numpy.array
    """
    t = np.array(xrange(len(train_y)))
    t = random.sample(t[train_y <= 0.5], sum(train_y > 0.5)) + t[train_y > 0.5].tolist()
    t = random.sample(t, len(t))
    train_x = train_x[t]
    train_y = train_y[t]
    return train_x, train_y


def construct_prediction_feature(customer_data, competitor_data, tag_dict, essential_tag_dict, demo=False):
    """
    构造预测数据特征, 维度为两个商品在所有相关维度上的相似度
    :param customer_data: DataFrame
    :param competitor_data: DataFrame
    :param tag_dict: OrderedDict
    :param essential_tag_dict: OrderedDict
    :param demo: boolean
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
            feature_vector = make_similarity_feature(attr1=attr1, attr2=attr2, tag_dict=tag_dict, demo=demo)
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
    _attr_list = [u",风格:通勤,款式品名:小背心/小吊带,图案:纯色,领型:圆领,颜色分类:花色,颜色分类:黑色,颜色分类:白色,"
                  u"颜色分类:深灰,颜色分类:浅灰,颜色分类:灰色,颜色分类:黄色,颜色分类:蓝色,颜色分类:绿色,颜色分类:西瓜红,"
                  u"颜色分类:玫红,颜色分类:红色,上市年份季节:2015年秋季,组合形式:单件,厚薄:厚,厚薄:薄,厚薄:适中,"
                  u"通勤:韩版,穿着方式:套头,衣长:常规,适用季节:春,适用季节:夏,适用季节:秋,适用季节:春夏,",
                  u",款式品名:卫衣/绒衫,服装款式细节:口袋,领型:半开领,服饰工艺:立体裁剪,颜色分类:紫色,颜色分类:黄色,"
                  u"颜色分类:酒红,颜色分类:红色,颜色分类:红黑,上市年份季节:2014年秋季,组合形式:两件套,厚薄:厚,厚薄:薄,"
                  u"厚薄:适中,袖长:长袖,裤长:长裤,面料:棉,面料:聚酯,衣门襟:拉链,适用年龄:35-39周岁,袖型:常规,"
                  u"服装版型:宽松,穿着方式:开衫,衣长:中长款,适用季节:春,适用季节:秋,"]

    print u"{0}开始测试tag_to_dict".format(datetime.now())
    industry = u"mp_women_clothing"
    _cid = 50000671
    _attr_name = u"颜色分类"

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

    from common.pickle_helper import pickle_load
    from common.settings import *
    _raw_tag_dict = pickle_load(TAG_DICT_PICKLE)

    _raw_attr1 = u",组合形式:单件,颜色分类:灰色,颜色分类:黄色,颜色分类:红色,服装版型:直筒,年份季节:2014年冬季," \
                 u"材质成分:山羊绒(羊绒)100%,品牌:博依格,图案:纯色,风格:百搭,款式:背带,衣长:常规,适用年龄:25-29周岁,"
    _raw_attr2 = u",组合形式:单件,服装版型:修身,年份季节:2015年秋季," \
                 u"材质成分:聚对苯二甲酸乙二酯(涤纶)94% 聚氨酯弹性纤维(氨纶)6%,品牌:型色缤纷,图案:纯色,风格:通勤," \
                 u"衣长:常规,"
    _attr1 = attributes_to_dict(_raw_attr1)
    _attr2 = attributes_to_dict(_raw_attr2)
    _tag_dict = _raw_tag_dict[121412004]
    fv = make_similarity_feature(_attr1, _attr2, _tag_dict, demo=False)
    fv = make_similarity_feature(_attr1, _attr2, _tag_dict, demo=True)
    print fv

    print u"{0}parse_essential_dimension".format(datetime.now())
    from db_apis import get_essential_dimensions
    d = parse_essential_dimension(get_essential_dimensions(db=u"mp_women_clothing"))
    print d.keys()

