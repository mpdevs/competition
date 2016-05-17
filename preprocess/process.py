# -*- coding: utf-8 -*-

import os
import MySQLdb
from tqdm import tqdm
from glob import glob
from datetime import datetime
import pandas as pd
from math import ceil

from tag_process import tagging_ali_items
from tag_process import tagging_ali_brands_preparation, tagging_ali_brands

from helper import parser_label, getcut, WJacca
from enums import DICT_EXCLUSIVES, IMPORTANT_ATTR_ENUM, DICT_FL, DICT_MUST

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
                ItemAttrDesc as Attribute,concat(ItemSubTitle,ItemName,ShopName) as ShopNameTitle
                FROM %s WHERE NeedReTag='y';
                """%(table_name)
        
        update_sql = """UPDATE """+table_name+""" SET TaggedItemAttr=%s, NeedReTag='n', TaggedBrandName=%s WHERE ItemID=%s ;"""
        
    print '{} Loading data ...'.format(datetime.now())
    data = pd.read_sql_query(query, connect) 
    
    n = len(data)
    if n > 0:       
        batch = 10000    
        brand_preparation = tagging_ali_brands_preparation(BRANDSLIST)    
        print u'Total number of data: {}, batch_size = {}'.format(n, batch)
                
        cursor = connect.cursor()
        for j in xrange(int(ceil(float(n)/batch))):
            print '{} Start batch {}'.format(datetime.now(), j+1)
            batch_data = data.iloc[j*batch:min((j+1)*batch, n)]
            
            print '{} Tagging brands ...'.format(datetime.now())            
            brand = tagging_ali_brands(batch_data, brand_preparation)
            
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




def process_annual(industry, table_from, table_to, one_shop=None):
    
    ifmonthly = True
    if table_from.find('monthly') == -1:
        ifmonthly = False
    
    #连接
    print '{} 正在连接数据库{} ...'.format(datetime.now(), host)
    connect_industry = MySQLdb.Connect(host=host, user=user, passwd=pwd, db=industry, charset='utf8')
    connect_portal = MySQLdb.Connect(host=host, user=user, passwd=pwd, db='mp_portal', charset='utf8')
    cursor_industry = connect_industry.cursor()
    cursor_portal = connect_portal.cursor()

    #词库文件,这个词库必须和打标签的词库是一个
    TAGLIST =  BASE_DIR + u'/feature/'+industry+'/*.txt'# 标签词库

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
    fl = DICT_FL[industry]

    #读取
    head = [x[len(TAGLIST)-5:-4] for x in glob(TAGLIST)]
    dict_head = {}
    for i in xrange(len(head)):
        dict_head[head[i]] = i
    
    if ifmonthly is False:
        insert_sql = """
            INSERT INTO """+table_to+"""(SourceItemID,TargetItemID,RelationType,Status,ShopId)
            VALUES
            ('%s',%s,'%s','%s', %s)    
        """
        all_data = pd.read_sql_query("SELECT TaggedItemAttr as label, ItemID as itemid, ShopId as shopid,DiscountPrice,CategoryID FROM "+table_from+" WHERE TaggedItemAttr IS NOT NULL and ((MonthlyOrders>=10 and MonthlySalesQty=0) or MonthlySalesQty>=10);", connect_industry)
    else:
        insert_sql = """
            INSERT INTO """+table_to+"""(SourceItemID,TargetItemID,RelationType,Status,BrandName)
            VALUES
            ('%s',%s,'%s','%s', %s)    
        """       
        all_data = pd.read_sql_query("SELECT TaggedItemAttr as label, ItemID as itemid, TaggedBrandName as Brand,DiscountPrice,CategoryID FROM "+table_from+" WHERE TaggedItemAttr IS NOT NULL and SalesQty>=10;", connect_industry)
        cursor_portal.execute("SELECT ShopID,BrandName FROM shop where IsClient='y';")
        shop2brand = cursor_portal.fetchall()
        shop2brand_dict = {_[0]:_[1] for _ in shop2brand}
        for i in xrange(len(shops)):
            shops[i] = shop2brand_dict[shops[i]]
                   
    if one_shop is None or one_shop=='':
        cursor_portal.execute("SELECT ShopID FROM shop where IsClient='y';")
        shops = [int(_[0]) for _ in cursor_portal.fetchall()]
    else:
        shops = [int(one_shop)]
    cursor_portal.close()           
    #为每个cid计算该品类下计算相似度的切分
    #先计算重要维度
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
            
    CID2CUT = {}
    CID2MUSTCUT = {}
    all_cid = set(all_data['CategoryID'].values)
    for cid in all_cid:
        #计算cid到cut的词典
        cid = str(cid)
        important = find_important(cid)
        if important is None: 
            print u"重要维度缺失:"+cid
            continue
        assert len(set(important).difference(fl)) == 0           
        unimportant = list(set(fl) ^ set(important))
        CID2CUT[cid] = getcut([important, unimportant], head)
        #计算cid到mustequal的词典
        must = []
        for j, _ in enumerate(DICT_MUST[industry]['name']):
            if category_id_dict[cid]['category_name'] in _:
                must = DICT_MUST[industry]['value'][j]
                break
        assert len(set(must).difference(important)) == 0
        CID2MUSTCUT[cid] = getcut([must], head)
    
    
               
    print u"共{}个店铺:".format(len(shops))
    #开始寻找竞品
    for value in shops:
        
        print datetime.now(),u'正在删除店铺%s数据 ...'%value
        if ifmonthly is False:
            cursor_industry.execute("delete from "+table_to+" where shopid = %d"%value)
        else:
            cursor_industry.execute("""delete from """+table_to+""" where BrandName = "%s" """%value)
        connect_industry.commit()
        
        print datetime.now(),u'正在读取店铺%s ...'%value
        if ifmonthly is False:
            cursor_industry.execute("SELECT TaggedItemAttr as label, ItemID as itemid, DiscountPrice as price, CategoryID FROM "+table_from+" WHERE ShopID=%d AND TaggedItemAttr IS NOT NULL AND TaggedItemAttr!='';"%value)
        else:
            cursor_industry.execute("""SELECT TaggedItemAttr as label, ItemID as itemid, DiscountPrice as price, CategoryID FROM """+table_from+""" WHERE TaggedBrandName="%s" AND TaggedItemAttr IS NOT NULL AND TaggedItemAttr!='';"""%value)
        items = cursor_industry.fetchall()

        if not items: continue

        label = parser_label([_[0] for _ in items], dict_head)

        insert_items = []
        print datetime.now(),u'正在计算店铺%s ...'%value
        
        #对每个商品找竞品
        for i, item in enumerate(tqdm(items)):
            item_id, price, category_id = int(item[1]), float(item[2]), str(item[3])
            if price == 0: continue
           
            mustequal = CID2MUSTCUT[category_id][0]            
            cut = CID2CUT[category_id]
            
            minprice = price * (1-setprecetage)
            maxprice = price * (1+setprecetage)

            # #找到所有价格段内的同品类商品
            if ifmonthly is False:
                todo_data = all_data[(all_data.DiscountPrice > minprice) & (all_data.DiscountPrice < maxprice) & (all_data.CategoryID == int(category_id)) & (all_data.shopid != value) ]
            else:
                todo_data = all_data[(all_data.DiscountPrice > minprice) & (all_data.DiscountPrice < maxprice) & (all_data.CategoryID == int(category_id)) & (all_data.Brand != value) ]
            if len(todo_data) == 0:continue
                       
            #计算相似度
            todo_id = todo_data['itemid'].values
            todo_label = parser_label(list(todo_data['label']), dict_head)
            v1 = label[i]
            for j in xrange(len(todo_id)):
                v2 = todo_label[j]
                naflag = False
                for _ in mustequal:
                    if sum(v1[_])+sum(v2[_]) != 0 and (v1[_]-v2[_]).any() != 0:
                        naflag = True
                        break
                
                if naflag:
                    judge = 0
                else:
                    samilarity = WJacca(v1, v2, cut, pvalue)                
                    if samilarity < jaccavalue[0]: 
                        judge = 0
                    elif samilarity > jaccavalue[1]:
                        judge = 2
                    else:
                        judge = 1

                if ifmonthly or judge > 0:
                    insert_item = (item_id, todo_id[j], judge, 1, value)
                    insert_items.append(insert_item)

        if len(insert_items) > 0:
            print '正在插入%d条数据'%len(insert_items)
            cursor_industry.executemany(insert_sql, insert_items)
            connect_industry.commit()
            
    connect_industry.close()
    print datetime.now()
