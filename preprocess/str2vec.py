# -*- coding: utf-8 -*-


def string2dict(string, attr_type='attr'):
    """
    方便模型处理
    :param string:
    :param attr_type:
    :return: dict
    """
    result = dict()
    for x in string.strip(",").split(","):
        k, v = x.split(":")
        if attr_type == "attr":
            t = result.get(k)
            if t is None:
                result[k] = {v}
            else:
                t.add(v)
        elif attr_type == "material":
            result[k] = float(v)  # or float(v.rstrip("%")) * 0.01
        else:
            print "Unknown attr_type!"
            raise SystemExit
    return result


def string2vector(attr1, material1, attr2, material2, feature2value):
    """
    feature2value's amount or values is a categorical ordered dictionary, key: feature, value: float or list, where the
    list is the material feature
    :param attr1:  dictionaries key: feature, value: feature_value(set of string)
    :param material1: key: feature, value: float
    :param attr2:  dictionaries key: feature, value: feature_value(set of string)
    :param material2: key: feature, value: float
    :param feature2value: ordered dictionary, key: feature, value: float or list
    :return: dict
    """
    # readability version:
    result = []
    for i, x in feature2value.iteritems():
        if i == u"材质":
            result.append(sum([min(material1[j], material2[j]) for j in x if material1.get(j) and material2.get(j)]))
        else:
            result.append(len(attr1[i] & attr2[i]) / x if attr1.get(i) and attr2.get(i) else 0)
    return result


def test_string2vector():
    from collections import OrderedDict
    attr1 = {u"风格": set([u"简单", u"时尚"]), u"裤长": set([u"七分裤"]), u"袖长": set([u"七分袖"])}
    attr2 = {u"风格": set([u"简单"]), u"袖长": set([u"五分袖"])}
    material1 = {1: 0.5, 2: 0.5}
    material2 = {1: 1}
    feature2value = OrderedDict()
    feature2value[u"风格"] = 5.0
    feature2value[u"裤长"] = 3.0
    feature2value[u"袖长"] = 3.0
    feature2value[u"材质"] = [1, 2, 3]
    print "attr1 = {} ".format(attr1)
    print "material1 = {} ".format(material1)
    print "attr2 = {} ".format(attr2)
    print "material2 = {} ".format(material2)
    print "feature2value = {} ".format(feature2value)
    print string2vector(attr1, material1, attr2, material2, feature2value)


def test_string2dict():
    string = u",功能:保暖发热,颜色分类:白色,颜色分类:深灰,颜色分类:灰色,颜色分类:天蓝,颜色分类:蓝色,袖型:常规"
    print string2dict(string, "attr")
    string = u", 面料1:0.2, 面料2:0.8"
    print string2dict(string, "material")


if __name__ == "__main__":
    test_string2vector()
    # test_string2dict()
