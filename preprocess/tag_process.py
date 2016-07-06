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
    
def tagging_ali_items(data, tag_preparation, EXCLUSIVES):
    import json
    result = []
    for i in tqdm(data.index):
        idict = dict()
        try:
            use_dict = tag_preparation[int(data['CategoryID'][i])]
        except KeyError:
            print 'CategoryID {} don\'t have attrdict.'.format(data['CategoryID'][i])
            result.append('')
            continue
            
        attr = data['Attribute'][i]
        title = data['Title'][i]
        for name, values in use_dict.iteritems():
            if name[2] == 'A':
                j1 = attr.find(name[1] + u':')
                if j1 != -1:
                    j2 = attr.find(u'，', j1)
                    t = [value for value in values if attr[j1:j2].find(value) != -1]
                    if t: idict[name[0]] = t[0] if len(t) == 1 else t
            elif name[2] == 'M' and name[0] not in EXCLUSIVES:
                t = [value for value in values if (attr+title).find(value) != -1]
                if t: idict[name[0]] = t[0] if len(t) == 1 else t 
            elif name[2] == 'M' and name[0] in EXCLUSIVES:
                t = [value for value in values if attr.find(value) != -1]
                t = t if t else [value for value in values if title.find(value) != -1]
                if t: idict[name[0]] = t[0] if len(t) == 1 else t 
            else:
                print 'Unknown type of tag: {}'.format(name[2])
                raise SystemExit()
        
        result.append(json.dumps(idict, ensure_ascii=False).replace('"', '\'') if len(idict) else '')
    return result
            
        
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    

    



