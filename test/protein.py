# coding: utf-8
import io
import re

def find_weight(weight_list):
    weight_combine = ' '.join(list(set(weight_list)))
    weight = []

    guige = re.findall(u'规格.{0,15}\d+[g|G|克|磅|kg|KG|kG|Kg|LBS|公斤]', weight_combine)
    weight = guige
    if not guige:
        danbai = re.findall(u'蛋白.{0,20}\d+[g|G|克|磅|kg|KG|kG|Kg|LBS|公斤]', weight_combine)
        weight = danbai
        if not danbai:
            jinghan = re.findall(u'净含.{0,20}\d+[g|G|克|磅|kg|KG|kG|Kg|LBS|公斤]', weight_combine)
            weight = jinghan

    # print weight[0]
    if weight:
        weight_num = re.search(u'\d+\.*\d*[g|G|克|磅|kg|KG|kG|Kg|LBS|公斤]', weight[0])

        return weight_num.group(0)
    else:
        return u''
    pass

def find_unit(unit_list):
    unit = [1]
    if unit_list:
        unit_str = re.findall(u'\d+', unit_list)
        # 轉為 int
        unit_str = [int(u) for u in unit_str]

        unit = unit_str
        # unit = u','.join(unit_str)

        divide = u'/\d'
        unit_divide = re.search(divide, unit_list)
        if unit_divide:
            # unit = u'-' + unit
            unit = [1]
    return unit
    pass

def tagging(name, attr):
    unit_pattern = u'[/|*].{0,5}[\d]+[盒|罐|瓶|桶|袋|支|条]'
    weight_pattern = u'[规格|净含量|蛋白].{0,20}\d+[g|G|克|磅|kg|KG|kG|Kg|LBS|公斤]'
    if attr:
        # unit
        unit_match = re.search(unit_pattern, attr)

        if not unit_match:
            unit_match = re.search(unit_pattern, name)

        if unit_match:
            unit_match = unit_match.group(0)

        unit = find_unit(unit_match)

        # weight
        weight_match = re.findall(weight_pattern, attr)
        if not weight_match:
            weight_match = re.findall(weight_pattern, name)
        weight = find_weight(weight_match)

        pass
    else:
        # unit
        unit_match = re.search(unit_pattern, name)
        if unit_match:
            unit_match = unit_match.group(0)
        unit = find_unit(unit_match)

        # weight
        weight_match = re.findall(weight_pattern, name)
        weight = find_weight(weight_match)

        pass

    gram = re.search(u'\d+\.*\d*', weight)
    if gram:
        gram = gram.group(0)

    scale = re.search(u'[g|G|克|磅|kg|KG|kG|Kg|LBS|公斤]', weight)
    if scale:
        scale = scale.group(0)

    # 轉成克數
    if scale in [u'磅', u'LBS', u'L']:
        gram = float(gram) * 453.59
    elif scale in [u'KG', u'kg', u'Kg', u'kG', u'k', u'K', u'公斤']:
        gram = float(gram) * 1000
    else:
        if gram:
            gram = float(gram)

    if len(unit) == 1:
        unit1 = 1
        unit2 = unit[0]
    else:
        unit1 = unit[0]
        unit2 = unit[1]

    return unit, unit1, unit2, gram


if __name__ == u"__main__":
    filename = 'protein_data.txt'
    with io.open(filename, mode='r', encoding='utf-8') as f:
        header = f.readline()
        data = [line.strip().split('\t') for line in f]

    for line in data:
        name = line[0]
        attr = False
        if len(line) == 2:
            attr = line[1]
        print tagging(name, attr)
