# -*- coding: utf-8 -*-

import os
import MySQLdb
from tqdm import tqdm
from glob import glob
from datetime import datetime
import pandas as pd
import numpy as np
from math import ceil

from tag_process import tagging_ali_items, tagging_ali_brands

from weighted_jacca import getcut, WJacca
from helper import parser_label
from enums import EXCLUSIVES, IMPORTANT_ATTR_ENUM

from mp_preprocess.settings import host, user, pwd



BASE_DIR = os.path.join(os.path.dirname(__file__), 'dicts')



def process_tag(industry, table_name):

    # 词库文件列表，unix style pathname pattern
    TAGLIST =  BASE_DIR + u'/feature/*.txt'# 标签词库
    BRANDSLIST = BASE_DIR + u'/brand/*.txt'# 品牌词库

    # EXCLUSIVES 互斥属性类型：以商品详情为准的标签类型


    print '{} Connecting DB ...'.format(datetime.now())
    connect = MySQLdb.Connect(host=host, user=user, passwd=pwd, db=industry, charset='utf8')

    # 选取数据,ItemID用于写回数据库对应的行,分行业打,因为要用不同的词库
    query = """
            SELECT ItemID,concat(ItemSubTitle,ItemName) as Title,
            ItemAttrDesc as Attribute,concat(ItemSubTitle,ItemName,ShopName) as ShopNameTitle
            FROM %s WHERE NeedReTag='y';
            """%(table_name)
    
    update_sql = """UPDATE """+table_name+""" SET TaggedItemAttr="%s", NeedReTag='n', TaggedBrandName="%s" WHERE ItemID=%s ;"""
    
    print '{} Loading data ...'.format(datetime.now())
    data = pd.read_sql_query(query, connect) 
    
    n = len(data)
    if n > 0:       
        batch = 10000        
        print u'Total number of data: {}, batch_size = {}'.format(n, batch)
        
        cursor = connect.cursor()
        for j in xrange(int(ceil(float(len(data))/batch))):
            print '{} Start batch {}'.format(datetime.now(), j+1)
            batch_data = data.iloc[j*batch:min((j+1)*batch, n)]
            
            print '{} Tagging brands ...'.format(datetime.now())            
            brand = tagging_ali_brands(batch_data, BRANDSLIST)
            
            print '{} Tagging feature ...'.format(datetime.now())
            label= tagging_ali_items(batch_data, TAGLIST, EXCLUSIVES)# 0-1 label          
            
            
            print u'{} Zipping data ...'.format(datetime.now())
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






def process_annual(industry):

    #连接
    print '{} 正在连接数据库 ...'.format(datetime.now())
    connect_industry = MySQLdb.Connect(host=host, user=user, passwd=pwd, db=industry, charset='utf8')
    connect_portal = MySQLdb.Connect(host=host, user=user, passwd=pwd, db='mp_portal', charset='utf8')

    cursor_industry = connect_industry.cursor()
    cursor_portal = connect_portal.cursor()

    #词库文件,这个词库必须和打标签的词库是一个
    TAGLIST =  BASE_DIR + u'/feature/*.txt'# 标签词库

    #Category
    cursor_portal.execute('SELECT CategoryID,CategoryName,ParentID FROM category;')
    categories = cursor_portal.fetchall()

    category_id_dict = {str(_[0]):{'category_id':str(_[0]), 'category_name':_[1], 'parent_id':_[2] } for _ in categories}



    #定义重要维度
    important_attr = IMPORTANT_ATTR_ENUM[industry]
    dict_imp_name = important_attr['name']
    dict_imp_value = important_attr['value']

    #设定价格段上下浮动百分比;不同等级的属性权重划分;异位同位同类划分阈值
    setprecetage, pvalue, jaccavalue = 0.2,  [0.6,0.4], [0.56,0.66]


    #定义总共的二级维度列表
    fl = [u"感官", u"风格", u"做工工艺", u"厚薄", u"图案", u"扣型", u"版型", u"廓型", u"领型", u"袖型", u"腰型", u"衣长", u"袖长", u"衣门禁", u"穿着方式", u"组合形式", u"面料", u"颜色", u"毛线粗细", u"适用体型", u"裤型", u"裤长", u"裙型", u"裙长", u"fea", u"fun"]

    #读取
    head = [x[len(TAGLIST)-5:-4] for x in glob(TAGLIST)]
    dict_head = {}
    for i in xrange(len(head)):
        dict_head[head[i]] = i

    insert_sql = """
        INSERT INTO itemrelation(SourceItemID,TargetItemID,RelationType,Status,ShopId)
        VALUES
        ('%s',%s,'%s','%s', %s)
        """

    cursor_portal.execute('SELECT ShopID FROM shop;')
    shops = cursor_portal.fetchall()

    all_data = pd.read_sql_query("SELECT TaggedItemAttr as label, ItemID as itemid, ShopId as shopid,DiscountPrice,CategoryID FROM mp_women_clothing.item WHERE  TaggedItemAttr IS NOT NULL;", connect_industry)

    #开始寻找竞品
    for value in shops:

        cursor_industry.execute("delete from itemrelation where shopid = %d"%value)
        connect_industry.commit()

        print u'删除店铺%d数据'%value

        print datetime.now(),u'正在计算ShopID=%d ...'%value

        cursor_industry.execute("SELECT TaggedItemAttr as label, ItemID as itemid, DiscountPrice as price, CategoryID FROM item WHERE ShopID=%d AND TaggedItemAttr IS NOT NULL AND TaggedItemAttr!='';"%value)
        items = cursor_industry.fetchall()

        if len(items)==0: continue

        label = parser_label([_[0] for _ in items], dict_head)

        insert_items = []
        #对每个商品找竞品
        for i, item in enumerate(tqdm(items)):
            item_id, price, category_id = int(item[1]), float(item[2]), str(item[3])

            if price == 0: continue

            #Find important dimension
            def find_important(category_id):
                important = None
                if category_id_dict.has_key(category_id):
                    category_name = category_id_dict[category_id]['category_name']
                    for j, x in enumerate(dict_imp_name):
                        if category_name in x:
                            important = dict_imp_value[j]
                            return important
                    if not important:
                        parent_id = category_id_dict[category_id]['parent_id']
                        if parent_id is not None:
                            important = find_important(str(parent_id))
                return important

            important = find_important(category_id)
            if important is None: continue


            #得到不重要的维度
            unimportant = list(set(fl) ^ set(important))
            cut = getcut(important, unimportant, head)


            minprice = price * (1-setprecetage)
            maxprice = price * (1+setprecetage)

            # #找到所有价格段内的同品类商品
            todo_data = all_data[(all_data.DiscountPrice > minprice) & (all_data.DiscountPrice < maxprice) & (all_data.CategoryID == int(category_id)) & (all_data.shopid != value[0]) ]

            if len(todo_data)==0:continue

            #计算相似度
            todo_id = todo_data['itemid'].values
            todo_label = parser_label(list(todo_data['label']), dict_head)
            for j in xrange(len(todo_id)):
                samilarity = WJacca(label[i], todo_label[j], cut, pvalue)

                if samilarity < jaccavalue[0]: continue
                judge = 2 if samilarity > jaccavalue[1] else 1

                if judge > 0:
                    insert_item = (item_id, todo_id[j], judge, 1, str(value[0]))

                    insert_items.append(insert_item)

        if len(insert_items) > 0:
            cursor_industry.executemany(insert_sql, insert_items)
            connect_industry.commit()
            print 'insert'+str(len(insert_items))


    connect_industry.close()
    print datetime.now()
