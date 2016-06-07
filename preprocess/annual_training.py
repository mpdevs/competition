# -*- coding: utf-8 -*-
#基于训练模型
import os
import MySQLdb
from tqdm import tqdm
from glob import glob
from datetime import datetime
import pandas as pd
import numpy as np
from math import ceil
from sklearn.linear_model import Ridge

from enums import DICT_FL, Insert_sql, Select_sql
from helper import parser_label, Jaca, getcut
from mp_preprocess.settings import host, user, pwd



BASE_DIR = os.path.join(os.path.dirname(__file__), 'dicts')

def process_annual(industry, table_from, table_to, one_shop=None):
    
    if table_to.find('monthly'):
        status = 'Monthly'
    elif table_to.find('history'):
        status = 'History'
    else:
        status = 'Normal'
    
    print '{} 正在连接数据库{} ...'.format(datetime.now(), host)
    connect_industry = MySQLdb.Connect(host=host, user=user, passwd=pwd, db=industry, charset='utf8')
    connect_portal = MySQLdb.Connect(host=host, user=user, passwd=pwd, db='mp_portal', charset='utf8')
    cursor_industry = connect_industry.cursor()
    cursor_portal = connect_portal.cursor()

    #词库文件,这个词库必须和打标签的词库是一个
    TAGLIST =  BASE_DIR + u'/feature/'+industry+'/*.txt'# 标签词库

    #Category
    cursor_portal.execute('SELECT CategoryID,CategoryName,ParentID FROM category;')
    cid2name = {int(_[0]): _[1] for _ in cursor_portal.fetchall()}
    
    #设定价格段上下浮动百分比
    setprecetage = 0.2

    #定义总共的二级维度列表
    fl = DICT_FL[industry]

    #读取
    head = [x[len(TAGLIST)-5:-4] for x in glob(TAGLIST)]
    dict_head = dict()
    for i in xrange(len(head)):
        dict_head[head[i]] = i
    
    if one_shop is None or one_shop=='':
        cursor_portal.execute("SELECT ShopID FROM shop where IsClient='y';")
        shops = [int(_[0]) for _ in cursor_portal.fetchall()]
    else:
        shops = [int(one_shop)]


    insert_sql = """INSERT INTO """ + table_to + Insert_sql[status]
            
    all_data = pd.read_sql_query(Select_sql[status] + table_from + " WHERE TaggedItemAttr IS NOT NULL and ((MonthlyOrders>=10 and MonthlySalesQty=0) or MonthlySalesQty>=10);", connect_industry)
    
    label = parser_label(all_data['label'].values, dict_head)
    id2vec = {int(all_data['itemid'][i]): label[i] for i in xrange(len(all_data))}
    cursor_portal.close()

    #模型训练
    print u"{} Training... ".format(datetime.now())
    TRAIN = BASE_DIR + u'/train/'+industry+'/*.txt'
    cut = getcut([fl], head)[0]
    name2model = dict()
    for path in glob(TRAIN):
        X, y = [], []
        for row in np.asarray(np.loadtxt(path), dtype=int):
            try:#标注数据未筛选
                v1, v2 = id2vec[row[0]], id2vec[row[1]]
            except:
                continue
            X.append([Jaca(v1[c], v2[c]) for c in cut])
            y.append([1, 0] if row[2] == 1 else [0, 1])  
        X = np.asarray(X)
        alpha = max(0.05, np.median(np.linalg.eigvals(X.T.dot(X))))
        model = Ridge(alpha=alpha, copy_X=True)
        model.fit(X, y)
        name2model[path[len(BASE_DIR + u'/train/'+industry+'/'):-4]] = model
        
    print u"共{}个店铺:".format(len(shops))
    #开始寻找竞品
    for value in shops:
        print datetime.now(),u'正在删除店铺%s数据 ...'%value
        cursor_industry.execute("delete from "+table_to+" where shopid = %d"%value)       
        print datetime.now(),u'正在读取店铺%s ...'%value
        cursor_industry.execute(Select_sql[status] + table_from + " WHERE ShopID=%d AND TaggedItemAttr IS NOT NULL AND TaggedItemAttr!='';"%value)
        items = cursor_industry.fetchall()

        if not items: continue
        shoplabel = parser_label([_[0] for _ in items], dict_head)

        insert_items = []
        print datetime.now(),u'正在计算店铺%s ...'%value
        
        havemodel = name2model.keys()
        #对每个商品找竞品
        for i, item in enumerate(tqdm(items)):
            item_id, price, category_id = int(item[1]), float(item[3]), int(item[4])
            if price == 0: continue
            
            if cid2name[category_id] not in havemodel: continue
            minprice = price * (1-setprecetage)
            maxprice = price * (1+setprecetage)

            # 找到所有价格段内的同品类商品
            if status == 'Normal':
                todo_data = all_data[(all_data.DiscountPrice > minprice) & (all_data.DiscountPrice < maxprice) & (all_data.CategoryID == category_id) & (all_data.shopid != value) ]
            elif status == 'Monthly':
                todo_data = all_data[(all_data.DiscountPrice > minprice) & (all_data.DiscountPrice < maxprice) & (all_data.CategoryID == category_id) & (all_data.shopid != value) & all_data.DateRange == item[5]]
            if len(todo_data) == 0:continue
                             
            #计算相似度
            v1 = shoplabel[i]
            todo_id = todo_data['itemid'].values
            X = [[Jaca(v1[c], v2[c]) for c in cut] for v2 in [id2vec[id2] for id2 in todo_id]]
            y = name2model[cid2name[category_id]].predict(X)

            insert_items += [(item_id, todo_id[j], 1, 1, value) for j in xrange(len(y)) if y[j][0] > y[j][1]]


        n = len(insert_items)        
        if n > 0:
            batch = 10000
            print '{} 正在插入{}条数据, split = 10000'.format(datetime.now(), n)
            for j in tqdm(xrange(int(ceil(float(n)/batch)))):
                cursor_industry.executemany(insert_sql, insert_items[j*batch:min((j+1)*batch, n)])
                connect_industry.commit()
          
    connect_industry.close()
    print datetime.now()



