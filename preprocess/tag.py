# -*- coding: utf-8 -*-

import os
import MySQLdb
from tqdm import tqdm
from glob import glob
from datetime import datetime
import pandas as pd
import numpy as np
from math import ceil
import itertools as it

from tag_process import tagging_ali_items
from tag_process import tagging_ali_brands_preparation, tagging_ali_brands

from helper import parser_label, getcut, WJacca
from enums import DICT_EXCLUSIVES

from mp_preprocess.settings import host, user, pwd



BASE_DIR = os.path.join(os.path.dirname(__file__), 'dicts')



def process_tag(industry, table_name):
    
    ifmonthly = True
    if table_name.find('monthly') == -1:
        ifmonthly = False
     
    # 词库文件列表，unix style pathname pattern
    TAGLIST =  BASE_DIR + u'/feature/'+industry+'/*.txt'# 标签词库
    BRANDSLIST = BASE_DIR + u'/brand/'+industry+'/*.txt'# 品牌词库

    # EXCLUSIVES 互斥属性类型：以商品详情为准的标签类型
    EXCLUSIVES = DICT_EXCLUSIVES[industry]

    print '{} Connecting DB{} ...'.format(datetime.now(), host)
    connect = MySQLdb.Connect(host=host, user=user, passwd=pwd, db=industry, charset='utf8')

    # 选取数据,ItemID用于写回数据库对应的行,分行业打,因为要用不同的词库
    if ifmonthly:
        query = """
                SELECT ItemID,concat(ItemSubTitle,ItemName) as Title,
                ItemAttrDesc as Attribute,concat(ItemSubTitle,ItemName,ShopName) as ShopNameTitle
                FROM %s;
                """%(table_name)
        
        update_sql = """UPDATE """+table_name+""" SET TaggedItemAttr=%s, TaggedBrandName=%s WHERE ItemID=%s ;"""
    else:
        query = """
                SELECT ItemID,concat(ItemSubTitle,ItemName) as Title,
                ItemAttrDesc as Attribute,concat(ShopName,ItemSubTitle,ItemName) as ShopNameTitle
                FROM %s WHERE NeedReTag='y';
                """%(table_name)
        
        update_sql = """UPDATE """+table_name+""" SET TaggedItemAttr=%s, NeedReTag='n', TaggedBrandName=%s WHERE ItemID=%s ;"""
        
    print '{} Loading data ...'.format(datetime.now())
    data = pd.read_sql_query(query, connect) 
    
    n = len(data)
    if n > 0:
        print '{} Preprocess ...'.format(datetime.now())       
        batch = 10000    
        brand_preparation = tagging_ali_brands_preparation(BRANDSLIST)    
        data['ShopNameTitle'] = data['ShopNameTitle'].str.replace(' ','')
        data['Attribute'] = data['Attribute'].str.replace(' ','')
        
        print u'Total number of data: {}, batch_size = {}'.format(n, batch)
                
        cursor = connect.cursor()
        for j in xrange(int(ceil(float(n)/batch))):
            print '{} Start batch {}'.format(datetime.now(), j+1)
            batch_data = data.iloc[j*batch:min((j+1)*batch, n)]
            
            print '{} Tagging brands ...'.format(datetime.now())            
            brand = tagging_ali_brands(batch_data['Attribute'].values, batch_data['ShopNameTitle'].values, brand_preparation)
            
            print '{} Tagging feature ...'.format(datetime.now())
            label= tagging_ali_items(batch_data, TAGLIST, EXCLUSIVES)# 0-1 label          
            
            feature = label.columns
            label = label.values
            ID = map(int, batch_data['ItemID'].values)  
            update_items = zip([','.join(feature[label[i]==1]) for i in xrange(len(batch_data))], brand, ID)
            
            print u'{} Writing this batch to database ...'.format(datetime.now())
            cursor.executemany(update_sql, update_items)
            connect.commit()
        
        connect.close()
        print u'{} Done!'.format(datetime.now())

    else:
        print u'Data in %s had been tagged!'%table_name
