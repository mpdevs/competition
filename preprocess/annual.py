# -*- coding: utf-8 -*-
"""
基于训练模型 该版本已废弃
"""
import os
from tqdm import tqdm
from glob import glob
from math import ceil
from datetime import datetime
import pandas as pd
import numpy as np
from enums import Insert_sql, Select_sql, CATEGORY_QUERY, ATTR_META_QUERY, ATTRIBUTES_QUERY, SHOP_QUERY
from helper import tag_to_matrix, jaccard, get_cut, tag_process, tag_to_dict
from mysql_helper import connect_db
base_dir = os.path.join(os.path.dirname(__file__), 'dicts')


def process_annual(industry, table_from, table_to, one_shop=None):
    # region数据准备
    category_filter = "(1623, 121412004, 162104, 50007068, 50011277)"
    limits = ""
    statistic_array = []

    if table_to.find('monthly') != -1:
        status = 'Monthly'
    elif table_to.find('history') != -1:
        status = 'History'
    else:
        status = 'Normal'
    
    print (u'{0} 正在连接数据库 ...'.format(datetime.now()))
    ####################################################################################################################
    # 数据库相关方法可以优化
    # 方法调用时，数据库开启连接
    ####################################################################################################################
    connect_industry, connect_portal = connect_db(industry), connect_db()
    cursor_industry, cursor_portal = connect_industry.cursor(), connect_portal.cursor()

    print (u"{0} 正在抽取标品类数据... ".format(datetime.now()))
    cursor_portal.execute(CATEGORY_QUERY.format(category_filter))
    # cid2name dict {CategoryID: CategoryName}
    cid2name = {int(row[0]): row[1] for row in cursor_portal.fetchall()}
    # 设定价格段上下浮动百分比
    price_percentage = 0.2

    # 设定要比价的店铺
    if one_shop is None or one_shop == '':
        print (u"{0} 正在抽取店铺数据... ".format(datetime.now()))
        cursor_portal.execute(SHOP_QUERY)
        shops = [int(_[0]) for _ in cursor_portal.fetchall()]
    else:
        shops = map(int, one_shop.split(','))
    cursor_portal.close()

    # 用来将商品标签向量化
    print (u"{0} 正在抽取标签元数据... ".format(datetime.now()))
    attr_meta = pd.read_sql_query(ATTR_META_QUERY.format(industry), connect_portal)
    # 竞品计算的核心数据
    print (u"{0} 正在抽取商品的标签数据... ".format(datetime.now()))
    items_attributes = pd.read_sql_query(ATTRIBUTES_QUERY.format("TaggedItemAttr", limits), connect_industry)

    # region 标签处理
    print (u"{0} 正在将商品的标签转换成矩阵... ".format(datetime.now()))
    
    tag_dict = tag_to_dict(df=attr_meta[["CID", "Attrname", "AttrValue"]])
    
    tag_list, tag_dict = tag_process(attr_meta.Attrname.tolist(), attr_meta.AttrValue.tolist())
    label_array = tag_to_matrix(items_attributes.TaggedItemAttr.tolist(), tag_dict)
    id2vec = {i: j for i, j in zip(items_attributes.ItemID.values, label_array)}
    include_tag_list = attr_meta.Attrname.unique().tolist()
    # endregion

    # 销毁对象，释放内存
    del items_attributes
    # endregion

    # region 模型训练
    print u"{} 准备训练模型... ".format(datetime.now())
    from sklearn.ensemble import GradientBoostingRegressor as gbrt
    import random

    train_path = u'{0}/train/{1}/'.format(base_dir, industry)
    train_files = u'{0}*.txt'.format(train_path)

    cut = get_cut([include_tag_list], tag_list)
    count = 0
    for i in range(len(cut)):
        count += len(cut[i])
    print count
    name2model = dict()

    key_error_list = []

    for path in glob(train_files):
        print u"{0} Training... {1}".format(datetime.now(), path)
        X, y = [], []
        for index, row in enumerate(np.asarray(np.loadtxt(path), dtype=long)):
            try:  # 标注数据未筛选
                v1, v2 = id2vec[row[0]], id2vec[row[1]]
            except KeyError as e:
                key_error_list.append(e)
                continue
            # jaccard相似度的值为X的input
            X.append([jaccard(v1[c], v2[c]) for c in cut])
            y.append(row[2])
        print u'一共{0}条数据，其中{1}条标签错误'.format(index, len(key_error_list))  #
        X, y = np.asarray(X), np.asarray(y)
        t = np.array(range(len(y)))
        t = random.sample(t[y < 0.5], sum(y > 0.5)) + t[y > 0.5].tolist()
        t = random.sample(t, len(t))
        if path[len(train_path):-4] == u'半身裙':
            statistic_array.append(np.mean(X, 0))
            statistic_array.append(np.std(X, 0))
            statistic_array = np.asarray(statistic_array)
            statistic_array = statistic_array.transpose()
            np.savetxt('X_train_statistic.txt', statistic_array, delimiter='\t')
        X, y = X[t], y[t]
        print u"X的行数为{0}，维度为{1}".format(X.shape[0], X.shape[1])
        model = gbrt()  
        model.fit(X, y)
        name2model[path[len(train_path):-4]] = model

    print u'name2model.keys()={0}'.format(name2model.keys())
    # endregion

    # region竟品查找
    if status == 'Normal':
        all_data_query = Select_sql[status] + table_from + """ WHERE TaggedItemAttr IS NOT NULL
        AND ((MonthlyOrders>=10 AND MonthlySalesQty=0) 0R MonthlySalesQty>=10);"""
    if status == 'Monthly':
        all_data_query = """SELECT TaggedItemAttr as label, ItemID as itemid, ShopId as shopid, DiscountPrice, CategoryID
        FROM {0} WHERE TaggedItemAttr IS NOT NULL
        AND ((MonthlyOrders>=10 AND MonthlySalesQty=0) OR MonthlySalesQty>=10);""".format(table_from)
    elif status == 'History':
        all_data_query = Select_sql[status] + table_from + " WHERE TaggedItemAttr IS NOT NULL;"

    all_data = pd.read_sql_query(all_data_query, connect_industry)

    print u"共{}个店铺:".format(len(shops))
    # 开始寻找竞品
    for value in shops:
        print datetime.now(), u'正在删除店铺%s数据 ...' % value
        cursor_industry.execute("delete from " + table_to + " where shopid = %d" % value)
        connect_industry.commit()
        print datetime.now(), u'正在读取店铺%s ...' % value
        if status == 'Normal':
            items_query = Select_sql[status] + table_from + \
                          " WHERE ShopID={0} AND TaggedItemAttr IS NOT NULL AND TaggedItemAttr!='';".format(value)
        elif status == 'Monthly':
            items_query = """SELECT TaggedItemAttr as label, ItemID as itemid, ShopId as shopid, DiscountPrice, CategoryID
            FROM {0} WHERE ShopID={1} AND TaggedItemAttr IS NOT NULL AND TaggedItemAttr!='';""".format(table_from, value)
        elif status == 'History':
            items_query = Select_sql[status].replace(', DateRange', '') + \
                          " WHERE ShopID={0} AND TaggedItemAttr IS NOT NULL AND TaggedItemAttr!='';".format(value)

        cursor_industry.execute(items_query)
        items = cursor_industry.fetchall()

        if not items:
            continue

        shoplabel = tag_to_matrix([_[0] for _ in items], tag_dict)
        insert_items = []
        print datetime.now(), u'正在计算店铺%s ...' % value
        
        # 对每个商品找竞品
        shape_list = []
        competitive_feature_list = []
        for i, item in enumerate(tqdm(items)):
            item_id, price, category_id = int(item[1]), float(item[3]), int(item[4])
            if price == 0:
                continue

            try:
                # if cid2name[category_id] not in name2model.keys():
                if cid2name[category_id] != u'半身裙':
                    continue
            except KeyError:
                continue

            min_price = price * (1 - price_percentage)
            max_price = price * (1 + price_percentage)

            # 找到所有价格段内的同品类商品
            if status == 'Normal' or status == 'History':
                todo_data = all_data[
                    (all_data.DiscountPrice > min_price) &
                    (all_data.DiscountPrice < max_price) &
                    (all_data.CategoryID == category_id) &
                    (all_data.shopid != value)
                ]
            elif status == 'Monthly':
                todo_data = all_data[
                    (all_data.DiscountPrice > min_price) &
                    (all_data.DiscountPrice < max_price) &
                    (all_data.CategoryID == category_id) &
                    (all_data.shopid != value)
                    # (all_data.DateRange == item[5])
                ]
                
            if len(todo_data) == 0:
                continue
            # if status == 'Monthly' or status == 'History':
            if status == 'History':
                todo_daterange = todo_data['DateRange'].values

            # 计算相似度
            v1 = shoplabel[i]
            todo_id = todo_data['itemid'].values
            X = [[jaccard(v1[c], v2[c]) for c in cut] for v2 in tag_to_matrix(todo_data['label'].values, tag_dict)]
            """测试"""
            X = np.array(X)
            shape_list.append(str(type(X)))
            competitive_feature_list.append(X)
            y = name2model[cid2name[category_id]].predict(X)

            if status == 'Normal':
                insert_items += [(item_id, todo_id[j], 1, 1, value, y[j]) for j in xrange(len(y)) if y[j] > 0.5]
            elif status == 'Monthly':
                insert_items += [(item_id, todo_id[j], 1, 1, value, y[j]) for j in xrange(len(y)) if y[j] > 0.5]
            elif status == 'History':
                insert_items += [(item_id, todo_id[j], 1, 1, value, y[j], todo_daterange[j]) for j in xrange(len(y)) if y[j] > 0.5]

        n = len(insert_items)
        # insert_query = """INSERT INTO """ + table_to + Insert_sql[status]
        insert_query = """INSERT INTO {0} (SourceItemID,TargetItemID,RelationType,Status,ShopId,Score)
        VALUES(%s ,%s, %s , %s, %s, %s)""".format(table_to)
        competitive_feature_list = np.asarray(competitive_feature_list)
        statistic_array = list()
        statistic_array.append(np.mean(competitive_feature_list[0], 0))
        statistic_array.append(np.std(competitive_feature_list[0], 0))
        statistic_array = np.asarray(statistic_array)
        statistic_array = statistic_array.transpose()
        np.savetxt('X_prediction_statistic.txt', statistic_array, delimiter='\t')

        # region更新入库
        if n > 0:
            batch = 10000
            print '{} 正在插入{}条数据, split = 10000'.format(datetime.now(), n)
            for j in tqdm(xrange(int(ceil(float(n)/batch)))):
                cursor_industry.executemany(insert_query, insert_items[j * batch: min((j + 1) * batch, n)])
                connect_industry.commit()
        # endregion
    connect_industry.close()
    # endregion
    print datetime.now()

if __name__ == '__main__':
    _industry = 'mp_women_clothing'
    _table_from = 'item_dev'
    _table_to = 'itemmonthlyrelation_2016'
    process_annual(industry=_industry, table_from=_table_from, table_to=_table_to, one_shop=None)
