# -*- coding: utf-8 -*-
# __author__: huang_yanhua
"""
打标签工具包
"""
import json
import re
from numpy import array_split
from glob import glob
from multiprocessing import Pool
from tqdm import tqdm
from collections import OrderedDict


def tagging_ali_brands_preparation(brands_list):
    tags = glob(brands_list)
    # word to brand w2b
    w2b = []
    error_tag_files = []
    for t in tags:
        tag_name = t.replace('\\', '/').split('/')[-1][:-4]
        try:
            f = open(t, 'r')
            tl = list(set([w.strip('\t\n\r') for w in f if w != '']))
            l = []
            for w in tl:
                for p in ['\\', '+', '*', '?', '^', '$', '|', '.', '[']:
                    w = w.replace(p, '\\'+p)
                l.append([w, tag_name])
            w2b += l
        except UnicodeDecodeError:
            error_tag_files.append(t)
            
    if len(error_tag_files) > 0:
        print u"""
            无法读取以下词库文件，请转码UTF-8后再次尝试。\n\n***\n\n{}
            """.format('\n'.join(error_tag_files))
        raise SystemExit()

    # 打品牌，优先打长的词，一样长就按照字典的排序来操作
    w2b.sort(key=lambda x: (len(x[0]), x[0]), reverse=True)
    ordered_dict = OrderedDict()
    # w2b的key是正则表达式，value是品牌名
    # 正则表达式的用处是对品牌名进行遍历，做映射
    for _ in w2b:
        ordered_dict[re.compile(_[0].decode('utf8'), re.IGNORECASE)] = _[1]
    
    return ordered_dict


# 找品牌
def tagging_ali_brands(att, st_list, w2b, split=10000):
    # 多线程处理
    pool = Pool()
    batch = 2 if len(att) <= split else len(att) / split
    att_list = array_split(att, batch)
    # shopname and title st_list
    # 每个线程处理一批商品名和属性
    splited_st_list = array_split(st_list, batch)
    w2b_list = [w2b] * batch
    result = pool.map(brand_core, zip(att_list, splited_st_list, w2b_list))
    pool.close()
    pool.join()
    t = []
    for x in result:
        t += x
    return t


def brand_core(a_zip):
    #  att=商品详情 np.array(string)
    att = a_zip[0]
    # st_list=店名和title list
    st_list = a_zip[1]
    # dict
    w2b = a_zip[2]
    # 每个商品映射一个品牌
    brand_list = [None] * len(att)

    for i, x in enumerate(tqdm(att)):       
        if x is not None:
            n = max(x.find(u'品牌:'), x.find(u'品牌：')) + 3
            # 商品详情有品牌信息
            if n != 2:
                t = x[n:x.find(u',', n)]
                for p in w2b.iterkeys():
                    if p.search(t):
                        brand_list[i] = w2b[p]
                        break

        st = st_list[i]
        # 在商品详情里没有品牌信息，则去点名和title里匹配
        if brand_list[i] is None and st is not None:
            for p in w2b.iterkeys():
                if p.search(st):
                    brand_list[i] = w2b[p]
                    break
        
    return brand_list


# 属性打标签
def tagging_ali_items(data, tag_preparation, exclusive_list):
    """
    属性匹配的时候 str.split 是大小写敏感的
    pandas.Series.str.contains 支持正则表达式
    可能会改
    :param data:
    :param tag_preparation:
    :param exclusive_list:
    :return: list
    """
    result = []
    for i in tqdm(data.index):
        row_dict = dict()
        try:
            # use_dict  商品对应的品类的标签词库
            use_dict = tag_preparation[int(data['CategoryID'][i])]
        except KeyError:
            print 'CategoryID {} don\'t have attrdict.'.format(data['CategoryID'][i])
            # 会影响竟品的数据抽取
            result.append('')
            continue
            
        attr = data['Attribute'][i]
        title = data['Title'][i]
        # type(name) = tuple
        # type(values) = list
        for name, values in use_dict.iteritems():
            # name[2]是FLAG
            # 阿里和宏原的属性名都不一样
            # 'A' 是阿里
            if name[2] == 'A':
                j1 = attr.find(name[1] + u':')
                if j1 != -1:
                    j2 = attr.find(u'，', j1)
                    t = [value for value in values if attr[j1:j2].find(value) != -1]
                    if t:
                        row_dict[name[0]] = t[0] if len(t) == 1 else t
            # 'M' 是宏原
            elif name[2] == 'M' and name[0] not in exclusive_list:
                t = [value for value in values if (attr+title).find(value) != -1]
                if t:
                    row_dict[name[0]] = t[0] if len(t) == 1 else t
            # 'M' 宏原属性会互斥
            elif name[2] == 'M' and name[0] in exclusive_list:
                # value 是词库 , attr 是网页数据
                t = [value for value in values if attr.find(value) != -1]
                t = t if t else [value for value in values if title.find(value) != -1]
                if t:
                    # name[0] 是 AttributeName
                    # 影响竟品
                    row_dict[name[0]] = t[0] if len(t) == 1 else t
            else:
                print 'Unknown type of tag: {}'.format(name[2])
                raise SystemExit()
        
        result.append(json.dumps(row_dict, ensure_ascii=False).replace('"', '\'') if len(row_dict) else '')
    return result
