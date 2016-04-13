# -*- coding: utf-8 -*-

import pandas as pd
from numpy import array_split
from glob import glob
from multiprocessing import Pool
from tqdm import tqdm

def tagging_ali_items(file_or_frame, tag_list, exclusives, 
    nrows=None, sep=',', processes=None):
    """
    Tags a table by Title and Attribute column with given tag list.
    
    Parameters
    ----------
    file_or_frame: csv file path or pandas DataFrame object.
    tag_list: unix style pathname pattern.
    exclusives: 
    nrows: number of rows to read from file or DataFrame.
    sep: Dilimiter to use.
    processes: processes to use for multiprocessing. By default equivalent
    to number of processers.
    
    """
    # load data
    if isinstance(file_or_frame, pd.DataFrame):
        data = file_or_frame
        # filename = 't'
    else:
        data = pd.read_csv(file_or_frame, nrows=nrows, sep=sep)
        # filename = file_or_frame.replace('\\','/').split('/')[-1][:-4]
    # load tag files
    tags = glob(tag_list) 
    # seperate exclusive and non-exclusive tags
    x_tag_dict = {tag_type: [tag for tag in tags if tag_type in tag]
                  for tag_type in exclusives}
    x_tags = [t for x_tags in x_tag_dict.values() for t in x_tags] # exclusive tags
    nx_tags = [t for t in tags if t not in x_tags]             # non-exclusive tags

    if len(x_tags) + len(nx_tags) > len(tags):
        yes = ('yes', 'y', 'ye')
        print u'有一些互斥属性不处于同一层级，是否继续([y/n])？'
        choice  = raw_input().lower()
        if choice in yes:
            pass
        else:
            raise SystemExit(u'好的。再见。')

    print u'标记互斥属性'
    df_x_tags = pd.DataFrame()
    for tag_type, tags in x_tag_dict.iteritems():
        print tag_type
        df_x_tags_title = tagging(data['Title'], tags, processes=processes)
        df_x_tags_attribute = tagging(data['Attribute'], tags, processes=processes)
        # 仅Attribute标签全部为空时，采信Title标签数据
        # for i, t in enumerate(df_x_tags_attribute.sum(axis=1)):
        #     # 仅Attribute标签全部为空时，采信Title标签数据
        #     if t == 0:
        #         df_x_tags_attribute.iloc[i] = df_x_tags_title.iloc[i]
        mask_na_att = (df_x_tags_attribute.sum(axis=1) == 0)
        df_x_tags_attribute[mask_na_att] = df_x_tags_title[mask_na_att]
        # 当exclusives 为空时，返回空表
        df_x_tags = pd.concat([df_x_tags, df_x_tags_attribute], axis=1)

    print u'标记非互斥属性'
    col = data['Attribute'].fillna('\t') + data['Title'].fillna('\t')
    df_nx_tags = tagging(col, nx_tags, processes=processes)

    return pd.concat([data, df_nx_tags, df_x_tags], 
                     axis=1, join_axes=[data.index])

def tagging(col, tags, split=10000, binary=True, processes=None):
    """
    Tags a pandas Series object by a list of tag files.
    
    Parameters
    ----------
    col: a pandas Series object.
    tags: a list of tag file path.
    binary: boolean, default True. If False, returns tag names.
    processes: processes to use for multiprocessing. By default equivalent
    to number of processers.
    """
    if processes is not None:
        pool = Pool(int(processes))
    else:
        pool = Pool()
    if len(col) < split:
        indices = 2
    else:
        indices = len(col) / split + 1
    col_list = array_split(col, indices)
    tags_list = [tags] * indices
    binary_list = [binary] * indices
    result = pool.map(tagging_core, zip(col_list, tags_list, binary_list))
    pool.close()
    pool.join()   
    return pd.concat(result)

def tagging_core(col_tag_binary):
    
    # 为了配合pool.map函数，只能传入一个参数
    col = col_tag_binary[0]                  # pandas series object
    tags = col_tag_binary[1]                 # tag list
    binary = col_tag_binary[2]               # if return binary value
    df = pd.DataFrame()
    error_tag_files = []

    try:
        col = col#.str.decode('utf-8')
    except UnicodeDecodeError:
        print u"""待标记数据未使用UTF-8格式编码，请转码后再次尝试。"""

    for t in tqdm(tags):
        tag_name = t.replace('\\','/').split('/')[-1][:-4]
        try:
            f = open(t, 'r')
            reg = '|'.join(
                [w.strip('\t\n\r') for w in f if w !='']).decode('utf-8')
            matchs = col.str.contains(reg)
            
            if binary:
                df[tag_name] = matchs
            else:
                df[tag_name] = pd.Series(
                    [tag_name if m == True else None for m in matchs])
        except UnicodeDecodeError, e:
            error_tag_files.append(t)
    if len(error_tag_files) > 0:
        print u"""
        无法读取以下词库文件，请确保文件编码被设置为UTF-8格式。\n\n***\n\n{}
        """.format('\n'.join(error_tag_files))
    return df.fillna(0) * 1 if binary else df                    
