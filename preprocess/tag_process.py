# -*- coding: utf-8 -*-

import re
import pandas as pd
import itertools as it
from numpy import array_split
from glob import glob
from multiprocessing import Pool
from datetime import datetime
from tqdm import tqdm

def tagging_ali_items(file_or_frame, tag_list, exclusives, nrows=None, sep=',', processes=None):
    """
    Tags a table by Title and Attribute column with given tag list.
    :param file_or_frame: csv file path or pandas DataFrame object.
    :param tag_list:      unix style pathname pattern.
    :param exclusives:
    :param nrows:         number of rows to read from file or DataFrame.
    :param sep:           Dilimiter to use.
    :param processes:     processes to use for multiprocessing. By default equivalent to number of processers.
    :return: pd.concat
    """


    data = file_or_frame if isinstance(file_or_frame, pd.DataFrame) else pd.read_csv(file_or_frame, nrows=nrows, sep=sep)
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


def tagging_ali_brands(data, brands_list, nrows=None, sep=',', processes=None):
    
    tags = glob(brands_list)
    tags.sort(reverse = True)
           
    BRAND = []
    pinpai_from_attribute= []
    for i, x in enumerate(data['Attribute']):
        if x is None:
            pinpai_from_attribute.append(0)
            continue
        temp = max(x.find(u'品牌:'), x.find(u'品牌：'))
        if temp != -1:
            pinpai_from_attribute.append(x[temp+3:x.find(u'，',temp+3)])
        else:
            pinpai_from_attribute.append(0)
    
    tag_name = []
    reg = []
    error_tag_files = []
    for t in tags:
        tag_name.append(t.replace('\\','/').split('/')[-1][:-4])
        try:
            f = open(t, 'r')
            temp = '|'.join(set([w.strip('\t\n\r') for w in f if w != ''])).decode('utf8')
            reg.append(re.compile(temp, re.IGNORECASE))
        except UnicodeDecodeError, e:
            error_tag_files.append(t)
            
    if len(error_tag_files) > 0:
        print u"""
            无法读取以下词库文件，请转码UTF-8后再次尝试。\n\n***\n\n{}
            """.format('\n'.join(error_tag_files))
        raise SystemExit()
    
    ST = data['ShopNameTitle'].values
         
    for i in tqdm(xrange(len(data))):
        BRAND.append(0)
        if pinpai_from_attribute[i] != 0: 
            for j in xrange(len(reg)):          
                if reg[j].search(pinpai_from_attribute[i].replace(' ', '')):
                    BRAND[-1] = tag_name[j]
                    break
        if BRAND[-1] == 0 and ST[i] is not None:
            for j in xrange(len(reg)):
                if reg[j].search(ST[i].replace(' ', '')):
                    BRAND[-1] = tag_name[j]
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
    indices = 2 if len(col) < split else len(col) / split + 1
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

    try:
        col = col                            #.str.decode('utf-8')
    except UnicodeDecodeError:
        print u"""待标记数据未使用UTF-8格式编码，请转码后再次尝试。"""

    for t in tqdm(tags):
        tag_name = t.replace('\\','/').split('/')[-1][:-4]
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
