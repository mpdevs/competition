# -*- coding: utf-8 -*-
# __author__: "Huang Yanhua"
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

from mp_preprocess.settings import host, user, pwd



BASE_DIR = os.path.join(os.path.dirname(__file__), 'dicts')



def process_color(industry, table_name):
    
    ifmonthly = True if table_name.find('month') != -1 else False    
     
    # 词库文件列表，unix style pathname pattern
    TAGLIST =  BASE_DIR + u'/color/'+industry+'/*.txt'# 标签词库

    print '{} Connecting DB{} ...'.format(datetime.now(), host)
    connect = MySQLdb.Connect(host=host, user=user, passwd=pwd, db=industry, charset='utf8')

    # 选取数据,ItemID用于写回数据库对应的行,分行业打,因为要用不同的词库
    if ifmonthly:
        query = """
                SELECT ItemID,concat_ws(' ',ItemSubTitle,ItemName) as Title,
                ItemAttrDesc as Attribute,concat_ws(' ',ShopName,ItemSubTitle,ItemName) as ShopNameTitle, ID 
                FROM %s;
                """%(table_name)
        
        update_sql = """UPDATE """+table_name+""" SET TaggedColor=%s WHERE ID=%s ; """
    else:
        query = """
                SELECT ItemID,concat_ws(' ',ItemSubTitle,ItemName) as Title,
                ItemAttrDesc as Attribute,concat_ws(' ',ShopName,ItemSubTitle,ItemName) as ShopNameTitle
                FROM %s WHERE NeedReTag='y';
                """%(table_name)
        
        update_sql = """UPDATE """+table_name+""" SET TaggedColor=%s, NeedReTag='n' WHERE ItemID=%s ;"""
        
    print '{} Loading data ...'.format(datetime.now())
    data = pd.read_sql_query(query, connect) 
    
    n = len(data)
    if n > 0:
        print '{} Preprocess ...'.format(datetime.now())       
        batch = 20000 # 20000的整数倍
        data['ShopNameTitle'] = data['ShopNameTitle'].str.replace(' ','')
        data['Attribute'] = data['Attribute'].str.replace(' ','')
        
        print u'Total number of data: {}, batch_size = {}'.format(n, batch)
        
        split_df = [data.iloc[j*batch:min((j+1)*batch, n)] for j in xrange(int(ceil(float(n)/batch)))]        
        cursor = connect.cursor()
        for j in xrange(len(split_df)):
            print '{} Start batch {}'.format(datetime.now(), j+1)
            batch_data = split_df[0]
                       
            print '{} Tagging color ...'.format(datetime.now())
            label = tagging_ali_items(batch_data, TAGLIST, [])# 0-1 label          
            
            feature = label.columns
            label = label.values
            ID = map(int, batch_data['ItemID'].values)  
            
            def features2json(fs):
                import json
                d = dict()
                for f in fs:
                    index = [i for i, x in enumerate(f) if x == '-'][-1]
                    try:
                        if isinstance(d[f[:index]], list):
                            d[f[:index]].append(f[index+1:])
                        else:
                            d[f[:index]] = [d[f[:index]], f[index+1:]]
                    except:
                        d[f[:index]] = f[index+1:]
                if not d.keys():
                    return None
                else:
                    return json.dumps(d, ensure_ascii=False).replace('"', '\'')
            
            if ifmonthly:                     
                update_items = zip([features2json(feature[label[i]==1]) for i in xrange(len(batch_data))], batch_data['ID'].values)
            else:
                update_items = zip([features2json(feature[label[i]==1]) for i in xrange(len(batch_data))], ID)

           
            print u'{} Writing this batch to database ...'.format(datetime.now())
            cursor.executemany(update_sql, update_items)
            connect.commit()
            del split_df[0]
            
        connect.close()
        print u'{} Done!'.format(datetime.now())

    else:
        print u'Data in %s had been tagged!'%table_name
