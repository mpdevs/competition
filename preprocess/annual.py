# -*- coding: utf-8 -*-
# 基于训练模型
import os
import MySQLdb
from tqdm import tqdm
from glob import glob
from datetime import datetime
import pandas as pd
import numpy as np
from math import ceil
from enums import DICT_FL, Insert_sql, Select_sql
from helper import parser_label, Jaca, getcut
from mp_preprocess.settings import host, user, pwd


# 当前文件夹路径下的 dicts 文件夹
BASE_DIR = os.path.join(os.path.dirname(__file__), 'dicts')


def process_annual(industry, table_from, table_to, one_shop=None):
    
    if table_to.find('monthly') != -1:
        status = 'Monthly'
    elif table_to.find('history') != -1:
        status = 'History'
    else:
        status = 'Normal'
    
    print '{} 正在连接数据库{} ...'.format(datetime.now(), host)
    ####################################################################################################################
    # 数据库相关方法可以优化
    # 方法调用时，数据库开启连接
    ####################################################################################################################
    connect_industry = MySQLdb.Connect(host=host, user=user, passwd=pwd, db=industry, charset='utf8')
    connect_portal = MySQLdb.Connect(host=host, user=user, passwd=pwd, db='mp_portal', charset='utf8')
    cursor_industry = connect_industry.cursor()
    cursor_portal = connect_portal.cursor()

    # 词库文件,这个词库必须和打标签的词库是一个
    tag_list = BASE_DIR + u'/feature/'+industry+'/*.txt'  # 标签词库

    # Category
    cursor_portal.execute('SELECT CategoryID,CategoryName,ParentID FROM category;')
    cid2name = {int(_[0]): _[1] for _ in cursor_portal.fetchall()}
    
    # 设定价格段上下浮动百分比
    setprecetage = 0.2

    # 定义总共的二级维度列表
    fl = DICT_FL[industry]

    # 读取
    head = [x[len(tag_list)-5:-4].replace(' ', '') for x in glob(tag_list)]
    dict_head = {head[i]: i for i in xrange(len(head))}
    
    if one_shop is None or one_shop == '':
        cursor_portal.execute("SELECT ShopID FROM shop where IsClient='y';")
        shops = [int(_[0]) for _ in cursor_portal.fetchall()]
    else:
        shops = map(int, one_shop.split(','))
    cursor_portal.close()

    # insert_sql = """INSERT INTO """ + table_to + Insert_sql[status]
    
    # 模型训练
    training_data = pd.read_sql_query("Select ItemID, TaggedItemAttr from TaggedItemAttr where TaggedItemAttr is not NULL and TaggedItemAttr != ''", connect_industry)
    id2vec = {i: j for i, j in zip(training_data['ItemID'].values, parser_label(training_data['TaggedItemAttr'].values, dict_head))}

    from sklearn.ensemble import GradientBoostingRegressor as gbrt
    import random
    print u"{} Training... ".format(datetime.now())

    train = BASE_DIR + u'/train/'+industry+'/*.txt'
    cut = getcut([fl], head)[0]
    name2model = dict()

    for path in glob(train):
        X, y = [], []
        for row in np.asarray(np.loadtxt(path), dtype=int):
            try:  # 标注数据未筛选
                v1, v2 = id2vec[row[0]], id2vec[row[1]]
            except:
                continue
            X.append([Jaca(v1[c], v2[c]) for c in cut])
            y.append(row[2])
        X, y = np.asarray(X), np.asarray(y)
        t = np.array(range(len(y)))
        t = random.sample(t[y < 0.5], sum(y > 0.5)) + t[y > 0.5].tolist()
        t = random.sample(t, len(t))
        X = X.reshape(-1, 1)
        X, y = X[t], y[t]
        model = gbrt()  
        model.fit(X, y)
        name2model[path[len(BASE_DIR + u'/train/'+industry+'/'):-4]] = model
    
    if status == 'Normal':
        all_data = pd.read_sql_query(Select_sql[status] + table_from + " WHERE TaggedItemAttr IS NOT NULL "
                                                                       "AND ((MonthlyOrders>=10 AND MonthlySalesQty=0) "
                                                                       "0R MonthlySalesQty>=10);", connect_industry)
    elif status == 'Monthly' or status == 'History':
        all_data = pd.read_sql_query(Select_sql[status] + table_from + " WHERE TaggedItemAttr IS NOT NULL;",
                                     connect_industry)
        
    print u"共{}个店铺:".format(len(shops))
    # 开始寻找竞品
    for value in shops:
        print datetime.now(), u'正在删除店铺%s数据 ...' % value
        cursor_industry.execute("delete from " + table_to + " where shopid = %d" % value)
        print datetime.now(), u'正在读取店铺%s ...' % value
        if status == 'Normal' or status == 'Monthly':
            cursor_industry.execute(Select_sql[status] + table_from + " WHERE ShopID=%d "
                                                                      "AND TaggedItemAttr IS NOT NULL "
                                                                      "AND TaggedItemAttr!='';" % value)
        elif status == 'History':
            cursor_industry.execute(Select_sql[status].replace(', DateRange', '') +
                                    'item' +
                                    " WHERE ShopID=%d AND TaggedItemAttr IS NOT NULL AND TaggedItemAttr!='';" % value)
        items = cursor_industry.fetchall()

        if not items:
            continue

        shoplabel = parser_label([_[0] for _ in items], dict_head)
        insert_items = []
        print datetime.now(), u'正在计算店铺%s ...' % value
        
        # 对每个商品找竞品
        for i, item in enumerate(tqdm(items)):
            item_id, price, category_id = int(item[1]), float(item[3]), int(item[4])
            if price == 0:
                continue

            if cid2name[category_id] not in name2model.keys():
                continue

            min_price = price * (1 - setprecetage)
            max_price = price * (1 + setprecetage)

            # 找到所有价格段内的同品类商品
            if status == 'Normal' or status == 'History':
                todo_data = all_data[(all_data.DiscountPrice > min_price) &
                                     (all_data.DiscountPrice < max_price) &
                                     (all_data.CategoryID == category_id) &
                                     (all_data.shopid != value)]
            elif status == 'Monthly':
                todo_data = all_data[(all_data.DiscountPrice > min_price) &
                                     (all_data.DiscountPrice < max_price) &
                                     (all_data.CategoryID == category_id) &
                                     (all_data.shopid != value) &
                                     (all_data.DateRange == item[5])]
                
            if len(todo_data) == 0:
                continue
            if status == 'Monthly' or status == 'History':
                todo_daterange = todo_data['DateRange'].values
               
            # 计算相似度
            v1 = shoplabel[i]
            todo_id = todo_data['itemid'].values
            X = [[Jaca(v1[c], v2[c]) for c in cut] for v2 in parser_label(todo_data['label'].values, dict_head)]
            y = name2model[cid2name[category_id]].predict(X)
            if status == 'Normal':
                insert_items += [(item_id, todo_id[j], 1, 1, value) for j in xrange(len(y)) if y[j] > 0.5]
            elif status == 'History' or status == 'Monthly':
                insert_items += [(item_id, todo_id[j], 1, 1, value, todo_daterange[j]) for j in xrange(len(y)) if y[j] > 0.5]

        n = len(insert_items)      
        if n > 0:
            batch = 10000
            print '{} 正在插入{}条数据, split = 10000'.format(datetime.now(), n)
            for j in tqdm(xrange(int(ceil(float(n)/batch)))):
                cursor_industry.executemany("""INSERT INTO """ + table_to + Insert_sql[status], insert_items[j*batch:min((j+1)*batch, n)])
                connect_industry.commit()
          
    connect_industry.close()
    print datetime.now()

if __name__ == '__main__':
    print "BASE_DIR = %s" % BASE_DIR
    industry_test = 'mp_women_clothing'
    tag_list_test = BASE_DIR + u'/feature/' + industry_test + '/*.txt'  # 标签词库
    print "tag_list = %s" % tag_list_test
    head_test = [x[len(tag_list_test)-5:-4].replace(' ', '') for x in glob(tag_list_test)]
    print "head = %s" % head_test
    dict_head_test = {head_test[i]: i for i in xrange(len(head_test))}
    print "dict_head = %s" % dict_head_test
    print "head[0] = %s" % head_test[0]
    print "dict_head[head[0]] = %s" % dict_head_test[head_test[0]]