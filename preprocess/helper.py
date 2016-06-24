# -*- coding: utf-8 -*-
__author__ = 'Dragon'
import os
import numpy as np
import pandas as pd
import json

DEBUG_FLAG = True

def parser_label(json_list, dict_head):    
    a_list = []
    for x in json_list:
        d = json.loads(x.replace("'", '"'))
        t = []
        for i, y in d.iteritems():
            if isinstance(y, list):
                t += [i+'-'+z for z in y]
            else:
                t.append(i+'-'+y)
        a_list.append(','.join(t))
    
    label = np.zeros((len(a_list), len(dict_head)))
    #df = pd.read_excel(os.path.dirname(__file__)+'/Combine.xlsx', encoding='utf8')#t,o
    
    for i, t in enumerate(a_list):
        #for j in df.index:
            #t = t.replace(df['o'][j], df['t'][j])
        for x in set(t.split(',')):
            if x!='':
                label[i][dict_head[x]] = 1
    return label


def getcut(miu, head):
    result = []
    for i in miu:
        t = []
        for x in i:
            t.append([])
            for j in xrange(len(head)):
                if head[j].find(x)+1:
                    t[-1].append(j)
            if not t[-1]:
                del t[-1]
        result.append(t)
    return result


def Jaca(u,v):
    t = np.bitwise_or(u != 0, v != 0)
    q = t.sum()
    if q == 0: return 0
    return 1 - float(np.bitwise_and((u != v), t).sum())/ q

def WJacca(x, y, cut, weights):
    result = 0
    n0 = len(cut[0])
    n1 = len(cut[1])
    a0 = weights[0]/n0

    for i in xrange(n0):
        t = cut[0][i]
        result += Jaca(x[t],y[t])
    result *= a0
    
    c = 0.0
    r = 0
    for i in xrange(n1):
        t = cut[1][i]
        if np.bitwise_and(x[t] != 0, y[t] != 0).sum() != 0:
            c += 1
            r += Jaca(x[t],y[t])
    
    if c == 0:
        return result
    else:
        return result + min(weights[1]/c, a0) * r


def label_rebuilt_to_set(unicode_string):
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
    return json.loads(json_string.replace("'", '"'))


def string_or_unicode_to_list(string_or_unicode):
    if isinstance(string_or_unicode, str):
        ret_list = string_or_unicode.split('\r\n')
    elif isinstance(string_or_unicode, unicode):
        ret_list = string_or_unicode.split('\r\n')
    else:
        ret_list = string_or_unicode
    return ret_list


def debug(var):
    if DEBUG_FLAG:
        print var


if __name__ == '__main__':
    test_string = "{'att-图案': '花式', 'att-适用季节': '秋季', " \
        "'att-色系': '多色', 'app-感官': ['优雅', '淑女', '时尚'], " \
        "'att-面料': '蕾丝', 'att-做工工艺': '拼接', " \
        "'att-款式-裙长': '短裙', 'fun': '打底', " \
        "'att-做工工艺-流行元素': ['镂空', '蕾丝拼接', '印花', '纱网'], " \
        "'att-款式-裙型': 'A字裙', " \
        "'app-风格': ['通勤', '百搭', '韩风']}"
    label_rebuilt_to_set(test_string)
