# -*- coding: utf-8 -*-

import re
import numpy as np
import pandas as pd
import itertools as it
from numpy import array_split
from glob import glob
from multiprocessing import Pool
from datetime import datetime
from tqdm import tqdm
from collections import OrderedDict


def tagging_ali_items(data, tag_list, exclusives, nrows=None, sep=',', processes=None):

    tags = glob(tag_list)                                                # load tag files

    # seperate exclusive and non-exclusive tags
    x_tag_dict = {tag_type: [tag for tag in tags if tag_type in tag] for tag_type in exclusives}
    x_tags = [t for x_tags in x_tag_dict.values() for t in x_tags]       # exclusive tags
    nx_tags = [t for t in tags if t not in x_tags]                       # non-exclusive tags

    if len(x_tags) + len(nx_tags) > len(tags):
        yes = ('yes', 'y', 'ye')
        print u'有一些互斥属性不处于同一层级，是否继续([y/n])？'
        choice  = raw_input().lower()
        if choice in yes:
            pass
        else:
            raise SystemExit(u'好的。再见。')

    #print u'标记互斥属性'
    df_x_tags = pd.DataFrame()
    for tag_type, tags in x_tag_dict.iteritems():
        #print tag_type
        df_x_tags_title = tagging(data['Title'], tags, 'items', processes=processes)
        df_x_tags_attribute = tagging(data['Attribute'], tags, 'items', processes=processes)

        mask_na_att = (df_x_tags_attribute.sum(axis=1) == 0)
        df_x_tags_attribute[mask_na_att] = df_x_tags_title[mask_na_att]

        df_x_tags = pd.concat([df_x_tags, df_x_tags_attribute], axis=1)     # 当exclusives 为空时，返回空表

    #print u'标记非互斥属性'
    col = data['Attribute'].fillna('\t') + data['Title'].fillna('\t')
    df_nx_tags = tagging(col, nx_tags, 'items', processes=processes)

    return pd.concat([df_nx_tags, df_x_tags], axis=1, join_axes=[data.index])
    #return pd.concat([data, df_nx_tags, df_x_tags], axis=1, join_axes=[data.index])
'''
    """    
    这部分是之前的一个版本
    将每个词为键,包含键的词为键值
    打品牌时仍对品牌名循环,这样当每个品牌词库下的词多的时候可以很大提升速度,不过存在一些bug没写好
    """
def tagging_ali_brands_preparation(brands_list):
    tags = glob(brands_list)
    tags.sort(reverse = True)
    
    tag_name = []
    reg = []
    w2b_key = []
    w2b_val = []
    error_tag_files = []
    for t in tags:
        tag_name.append(t.replace('\\','/').split('/')[-1][:-4])
        try:
            f = open(t, 'r')
            tl = list(set([w.strip('\t\n\r') for w in f if w != '']))
            l = []
            for w in tl:
                for p in ['\\', '+', '*', '?', '^', '$', '|', '.', '[']:
                    w = w.replace(p, '\\'+p)
                l.append(w)
            
            w2b_key += l
            w2b_val += [tag_name[-1]]*len(l)
            reg.append(re.compile('|'.join(l).decode('utf8'), re.IGNORECASE))
        except UnicodeDecodeError, e:
            error_tag_files.append(t)
            
    if len(error_tag_files) > 0:
        print u"""
            无法读取以下词库文件，请转码UTF-8后再次尝试。\n\n***\n\n{}
            """.format('\n'.join(error_tag_files))
        raise SystemExit()
    
    #这段为了处理品牌词之间有包含,大概要1分钟
    brand2longer = {}
    for i in xrange(len(w2b_key)):
        w = w2b_key[i]
        regw = re.compile(w)
        b = w2b_val[i]
        t = []
        for j in xrange(len(w2b_key)):
            if w2b_val[j] != b and regw.search(w2b_key[j], re.IGNORECASE): t.append([w2b_key[j], w2b_val[j]])
        if len(t) != 0: brand2longer[b] = brand2longer[b] + t if b in brand2longer.keys() else t 
    
    for i in brand2longer.keys():
        brand2longer[i].sort(key=lambda x:len(x[0]), reverse=True)    
        for _ in brand2longer[i]:
            print i,_[0]
            _[0] = re.compile(_[0].decode('utf8'), re.IGNORECASE)
    
    return [reg, tag_name, brand2longer]
def tagging_ali_brands(data, from_preparation):
    
    reg = from_preparation[0]
    tag_name = from_preparation[1]
    brand2longer = from_preparation[2]
        
    BRAND = []
    N = len(data)
    pinpai_from_attribute = [0]*N
    for i, x in enumerate(data['Attribute']):
        if x is None: continue
        n = max(x.find(u'品牌:'), x.find(u'品牌：')) + 3
        if n != 2:
            pinpai_from_attribute[i] = x[n:x.find(u'，',n)]
    ST = data['ShopNameTitle'].values
      
    for i in tqdm(xrange(len(data))):
        BRAND.append(0)
        t = pinpai_from_attribute[i]
        if t != 0: 
            t = t.replace(' ', '')
            for j in xrange(len(reg)):          
                if reg[j].search(t):
                    BRAND[-1] = tag_name[j]
                    try:
                        for com in brand2longer[tag_name]:
                            if com[0].search(t):
                                BRAND[-1] = com[1]
                                break
                    except:
                        pass
                    break
                        
        if BRAND[-1] == 0 and ST[i] is not None:
            t = ST[i].replace(' ', '')
            for j in xrange(len(reg)):
                if reg[j].search(t):
                    BRAND[-1] = tag_name[j]
                    try:
                        for com in brand2longer[tag_name]:
                            if com[0].search(t): 
                                BRAND[-1] = com[1]
                                break
                    except:
                        pass
                    break
    
    return BRAND
'''

#这个版本对词循环
def tagging_ali_brands_preparation(brands_list):
    tags = glob(brands_list)
   
    w2b = []
    error_tag_files = []
    for t in tags:
        tag_name = t.replace('\\','/').split('/')[-1][:-4]
        try:
            f = open(t, 'r')
            tl = list(set([w.strip('\t\n\r') for w in f if w != '']))
            l = []
            for w in tl:
                for p in ['\\', '+', '*', '?', '^', '$', '|', '.', '[']:
                    w = w.replace(p, '\\'+p)
                l.append([w,tag_name])            
            w2b += l
        except UnicodeDecodeError, e:
            error_tag_files.append(t)
            
    if len(error_tag_files) > 0:
        print u"""
            无法读取以下词库文件，请转码UTF-8后再次尝试。\n\n***\n\n{}
            """.format('\n'.join(error_tag_files))
        raise SystemExit()
    
    w2b.sort(key=lambda x:(len(x[0]),x[0]), reverse=True)
    
    W2B = OrderedDict()
    for _ in w2b:
        W2B[re.compile(_[0].decode('utf8'), re.IGNORECASE)] = _[1]
    
    return W2B
'''
def tagging_ali_brands(att, ST, w2b):
        
    N = len(att)
    BRAND = [0]*N

    for i, x in enumerate(tqdm(att)):       
        if x is not None:
            n = max(x.find(u'品牌:'), x.find(u'品牌：')) + 3           
            if n != 2:
                t = x[n:x.find(u',',n)]
                for p in w2b.iterkeys():
                    if p.search(t):
                        BRAND[i] = w2b[p]
                        break
        
        st = ST[i]          
        if BRAND[i] == 0 and st is not None:           
            for p in w2b.iterkeys():
                if p.search(st):
                    BRAND[i] = w2b[p]
                    break
        
    return BRAND
'''
def tagging_ali_brands(att, ST, w2b, split=10000):
    pool = Pool()
    indices = 2 if len(att) <= split else len(att) / split 
    att_list = array_split(att, indices)
    ST_list = array_split(ST, indices)
    w2b_list = [w2b] * indices 
    result = pool.map(brand_core, zip(att_list, ST_list, w2b_list))
    pool.close()
    pool.join()
    t = []
    for x in result:
        t += x
    return t
    
def brand_core(a_zip):
    att = a_zip[0]
    ST = a_zip[1]
    w2b = a_zip[2]
    N = len(att)
    BRAND = [None] * N

    for i, x in enumerate(tqdm(att)):       
        if x is not None:
            n = max(x.find(u'品牌:'), x.find(u'品牌：')) + 3           
            if n != 2:
                t = x[n:x.find(u',', n)]
                for p in w2b.iterkeys():
                    if p.search(t):
                        BRAND[i] = w2b[p]
                        break
        
        st = ST[i]                 
        if BRAND[i] is None and st is not None:
            for p in w2b.iterkeys():
                if p.search(st):
                    BRAND[i] = w2b[p]
                    break
        
    return BRAND
    

def tagging(col, tags, split=10000, binary=True, processes=None):
    """
    Tags a pandas Series object by a list of tag files.
    :param col:    a pandas Series object.
    :param tags:   a list of tag file path.
    :param split:
    :param binary: boolean, default True. If False, returns tag names.
    :param processes:  processes to use for multiprocessing. By default equivalent to number of processers.
    :return: pd.concat
    """

    pool = Pool() if processes is None else Pool(int(processes))
    indices = 2 if len(col) <= split else len(col) / split 
    col_list = array_split(col, indices)
    tags_list = [tags] * indices
    binary_list = [binary] * indices
    result = pool.map(tagging_core, zip(col_list, tags_list, binary_list))
    pool.close()
    pool.join()
    return pd.concat(result)


def tagging_core(col_tag_binary):
    """
    :param col_tag_binary:  为了配合pool.map函数，只能传入一个参数
    :return:
    """
    import warnings
    warnings.filterwarnings("ignore")
    
    col = col_tag_binary[0]                  # pandas series object
    tags = col_tag_binary[1]                 # tag list
    binary = col_tag_binary[2]               # if return binary value
    df = pd.DataFrame()
    error_tag_files = []

    for t in tqdm(tags):
        tag_name = t.replace('\\','/').split('/')[-1][:-4].replace(' ', '')
        #print tag_name
        try:
            f = open(t, 'r')
            reg = '|'.join([w.strip('\t\n\r') for w in f if w != '']).decode('utf-8')
            matchs = col.str.contains(reg)
            if binary:
                df[tag_name] = matchs
            else:
                df[tag_name] = pd.Series([tag_name if m == True else None for m in matchs])
        except UnicodeDecodeError, e:
            error_tag_files.append(t)

    if len(error_tag_files) > 0:
        print u"""
            无法读取以下词库文件，请转码UTF-8后再次尝试。\n\n***\n\n{}
            """.format('\n'.join(error_tag_files))
        raise SystemExit()
    return df.fillna(0) * 1 if binary else df