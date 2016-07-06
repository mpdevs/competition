# -*- coding: utf-8 -*
import os
import MySQLdb
from datetime import datetime
import pandas as pd
from math import ceil
from tag_process import tagging_ali_items
from tag_process import tagging_ali_brands_preparation, tagging_ali_brands
from enums import DICT_EXCLUSIVES
from mp_preprocess.settings import host, user, pwd


BASE_DIR = os.path.join(os.path.dirname(__file__), 'dicts')


def process_tag(industry, table_name):
 
    # 词库文件列表，unix style pathname pattern
    BRANDSLIST = BASE_DIR + u'/brand/'+industry+'/*.txt'  # 品牌词库

    # EXCLUSIVES 互斥属性类型：以商品详情为准的标签类型
    EXCLUSIVES = DICT_EXCLUSIVES[industry]

    print '{} Connecting DB{} ...'.format(datetime.now(), host)
    connect = MySQLdb.Connect(host=host, user=user, passwd=pwd, db=industry, charset='utf8')
    portal = MySQLdb.Connect(host=host, user=user, passwd=pwd, db='mp_portal', charset='utf8')

    # 选取商品数据
    query = """
                SELECT ItemID, CategoryID, concat_ws(' ',ItemSubTitle,ItemName) as Title,
                ItemAttrDesc as Attribute, concat_ws(' ',ShopName,ItemSubTitle,ItemName) as ShopNameTitle
                FROM %s WHERE NeedReTag='y';
                """ % table_name
        
    update_sql = """UPDATE """ + table_name + \
                     """ SET TaggedItemAttr=%s, NeedReTag='n', TaggedBrandName=%s WHERE ItemID=%s ;"""
        
    print '{} Loading data ...'.format(datetime.now())
    data = pd.read_sql_query(query, connect) 
    
    n = len(data)
    if n > 0:
        print '{} Preprocess ...'.format(datetime.now())       
        batch = 20000  # 20000的整数倍    
        data['ShopNameTitle'] = data['ShopNameTitle'].str.replace(' ', '')
        data['Attribute'] = data['Attribute'].str.replace(' ', '')
        
        # 标签准备
        brand_preparation = tagging_ali_brands_preparation(BRANDSLIST)
        # tag_dicts = pd.read_sql_query("SELECT attr_value.CID, attr_value.Attrname, attr_value.DisplayName, attr_value.AttrValue, attr_dict.Flag from attr_value INNER JOIN attr_dict on attr_value.AttrName=attr_dict.AttrName where attr_dict.IsTag='y'", portal)    
        industry_id = pd.read_sql_query("SELECT IndustryID from industry where DBName='{}'".format(industry), portal)['IndustryID'].values[0]
        tag_dicts = pd.read_sql_query("SELECT CID, Attrname, DisplayName, AttrValue, Flag from attr_value where IsTag='y' and IndustryID={}".format(industry_id), portal)     
        tag_preparation = dict()
             
        for cid in tag_dicts['CID'].unique():
            tag_preparation[int(cid)] = {(x[1], x[2], x[4]): x[3].rstrip(',').replace(' ', '').split(',') for x in tag_dicts[tag_dicts['CID'] == cid].values}
            
            
        
        print u'Total number of data: {}, batch_size = {}'.format(n, batch)
        
        split_df = [data.iloc[j*batch:min((j+1)*batch, n)] for j in xrange(int(ceil(float(n)/batch)))]        
        cursor = connect.cursor()
        for j in xrange(len(split_df)):
            print '{} Start batch {}'.format(datetime.now(), j+1)
            batch_data = split_df[0]
            
            print '{} Tagging brands ...'.format(datetime.now())            
            brand = tagging_ali_brands(batch_data['Attribute'].values, batch_data['ShopNameTitle'].values, brand_preparation)
            
            print '{} Tagging features ...'.format(datetime.now())
            label = tagging_ali_items(batch_data, tag_preparation, EXCLUSIVES)
            
            ID = map(int, batch_data['ItemID'].values)  
            
            update_items = zip(label, brand, ID)
            # update_items = zip([','.join(feature[label[i]==1]) for i in xrange(len(batch_data))], brand, ID)

            print u'{} Writing this batch to database ...'.format(datetime.now())
            cursor.executemany(update_sql, update_items)
            connect.commit()
            del split_df[0]
           
        connect.close()
        print u'{} Done!'.format(datetime.now())

    else:
        print u'Data in %s had been tagged!' % table_name

