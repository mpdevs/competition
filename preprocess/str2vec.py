# -*- coding: utf-8 -*-


def string2vector(attr1, material1, attr2, material2, feature2value):
    """
    attr1, attr2, are dictionaries, key: feature, value: feature_value(set of string)
    material1,  material2 are dictionaries, key: feature, value: float
    feature2value's amount or values is a categorical ordered dictionary, key: feature, value: float or list, where the
    list is the material feature
    # simplified version:
    return [sum([min(material1[j], material2[j]) for j in x if material1.get(j) and material2.get(j)]) if i == u"材质"
       else len(attr1[i] & attr2[i]) / x if attr1.get(i) and attr2.get(i) else 0 for i, x in feature2value.iteritems()]
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
    print string2vector(attr1, material1, attr2, material2, feature2value)


if __name__ == "__main__":
    test_string2vector()