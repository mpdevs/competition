# -*- coding: utf-8 -*
# __author__: huang_yanhua
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
    brand_list = BASE_DIR + u'/brand/'+industry+'/*.txt'  # 品牌词库

    # exclusive_list 互斥属性类型：以商品详情为准的标签类型
    exclusive_list = DICT_EXCLUSIVES[industry]

    print '{} Connecting DB{} ...'.format(datetime.now(), host)
    connect = MySQLdb.Connect(host=host, user=user, passwd=pwd, db=industry, charset='utf8')
    portal = MySQLdb.Connect(host=host, user=user, passwd=pwd, db='mp_portal', charset='utf8')

    # 选取商品数据
    query = """
                SELECT ItemID, CategoryID, concat_ws(' ',ItemSubTitle,ItemName) AS Title,
                ItemAttrDesc AS Attribute, concat_ws(' ',ShopName,ItemSubTitle,ItemName) AS ShopNameTitle
                FROM %s WHERE NeedReTag='y';
                """ % table_name
        
    update_sql = """UPDATE {0}
    SET TaggedItemAttr=%s, NeedReTag='n', TaggedBrandName=%s
    WHERE ItemID=%s ;""".format(table_name)
        
    print '{} Loading data ...'.format(datetime.now())
    data = pd.read_sql_query(query, connect) 
    
    n = len(data)
    if n > 0:
        print '{} Preprocess ...'.format(datetime.now())       
        batch = 20000  # 20000的整数倍    
        data['ShopNameTitle'] = data['ShopNameTitle'].str.replace(' ', '')
        data['Attribute'] = data['Attribute'].str.replace(' ', '')
        
        # 标签准备
        brand_preparation = tagging_ali_brands_preparation(brand_list)

        industry_id_query = "SELECT IndustryID FROM industry WHERE DBName='{}'".format(industry)
        industry_id = pd.read_sql_query(industry_id_query, portal)['IndustryID'].values[0]

        tag_dicts_query = """SELECT CID, Attrname, DisplayName, AttrValue, Flag
        FROM attr_value
        WHERE IsTag='y'
        AND
        IndustryID={}""".format(industry_id)
        tag_dicts = pd.read_sql_query(tag_dicts_query, portal)

        tag_preparation = dict()
             
        for cid in tag_dicts['CID'].unique():
            tag_preparation[int(cid)] = {(x[1], x[2], x[4]): x[3].rstrip(',').replace(' ', '').split(',')
                                         for x in tag_dicts[tag_dicts['CID'] == cid].values}

        print u'Total number of data: {}, batch_size = {}'.format(n, batch)
        
        split_df = [data.iloc[j*batch:min((j+1)*batch, n)] for j in xrange(int(ceil(float(n)/batch)))]        
        cursor = connect.cursor()
        for j in xrange(len(split_df)):
            print '{} Start batch {}'.format(datetime.now(), j+1)
            batch_data = split_df[0]
            
            print '{} Tagging brands ...'.format(datetime.now())

            brand = tagging_ali_brands(batch_data['Attribute'].values,
                                       batch_data['ShopNameTitle'].values,
                                       brand_preparation)
            
            print '{} Tagging features ...'.format(datetime.now())
            label = tagging_ali_items(batch_data, tag_preparation, exclusive_list)
            
            item_ids = map(int, batch_data['ItemID'].values)  
            
            update_items = zip(label, brand, item_ids)

            print u'{} Writing this batch to database ...'.format(datetime.now())
            cursor.executemany(update_sql, update_items)
            connect.commit()
            del split_df[0]
           
        connect.close()
        print u'{} Done!'.format(datetime.now())

    else:
        print u'Data in %s had been tagged!' % table_name
