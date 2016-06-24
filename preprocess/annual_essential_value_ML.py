# -*- coding: utf-8 -*-
"""
基于训练模型和必要维度值
"""
import os
import MySQLdb
from tqdm import tqdm
from glob import glob
from datetime import datetime
import pandas as pd
import numpy as np
from math import ceil
from enums import DICT_FL, Insert_sql, Select_sql
from helper import parser_label, Jaca, getcut, string_or_unicode_to_list, db_json_to_python_json
from mp_preprocess.settings import host, user, pwd
from mysql_conn import MySQLDB
import json

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
    training_data = pd.read_sql_query("Select ItemID, TaggedItemAttr "
                                      "FROM TaggedItemAttr "
                                      "WHERE TaggedItemAttr IS NOT NULL AND TaggedItemAttr != ''", connect_industry)

    id2vec = {i: j for i, j in zip(training_data['ItemID'].values,
                                    parser_label(training_data['TaggedItemAttr'].values, dict_head))}

    from sklearn.ensemble import GradientBoostingRegressor as gbrt
    import random
    print u"{} Training... ".format(datetime.now())

    train = BASE_DIR + u'/train/' + industry + '/*.txt'
    if os.path.exists(BASE_DIR + u'/train/' + industry + '_false'):
        train_false = BASE_DIR + u'/train/' + industry + '_false/*.txt'
    else:
        train_false = None

    cut = getcut([fl], head)[0]
    name2model = dict()
    train
    for path in glob():
        """
        同位数据由上一步的训练结果np.savetxt方法导出
        异位数据在这里由 train_false 路径下
        由同位数据量去抓取等数量的随机序列的异位数据
        """
        X, y = [], []
        for row in np.asarray(np.loadtxt(path)):
            try:  # 标注数据未筛选
                v1, v2 = id2vec[long(row[0])], id2vec[long(row[1])]
            except:
                continue
            X.append([Jaca(v1[c], v2[c]) for c in cut])
            y.append(float(row[2]))
        X, y = np.asarray(X), np.asarray(y)

    # 文件标识flag
    mixed_set = False

    # 判断文件夹以及文件是否存在，如果不存在，用老方法即可
    file_name = os.path.splitext(os.path.basename(path))[0]
    if train_false:
        for false_file_path in glob(train_false):
            if file_name in false_file_path:
                X_false, y_false = [], []
                for row_false in np.asarray(np.loadtxt(false_file_path)):
                    try:  # 标注数据未筛选
                        v1_false, v2_false = id2vec[long(row_false[0])], id2vec[long(row_false[1])]
                    except:
                        continue
                X_false.append([Jaca(v1_false[c], v2_false[c]) for c in cut])
                y_false.append(float(row_false[2]))
                X_false, y_false = np.asarray(X_false), np.asarray(y_false)
                mixed_set = True

    t = np.array(range(len(y)))
    X_true, y_true = X[t[y > 0.5]], y[y > 0.5]
    # 新老方法结合点
    if mixed_set:
        t = np.array(range(len(y_false)))
        # 随机获取和同位数量等值的异位数据
        t = random.sample(t, sum(y > 0.5))
        X_false, y_false = X_false[t], y_false[t]
        print u'异位数位%s' % len(y_false)
    else:
        t = np.array(range(len(y_false)))
        t = random.sample(t[y < 0.5], sum(y > 0.5))
        t = random.sample(t, len(t))
        X_false, y_false = X[t], y[t]
        print u'异位数位%s' % len(y_false)

    X = np.vstack((X_true, X_false))
    y = np.append((y_true, y_false))
    model = gbrt()
    model.fit(X, y)
    name2model[path[len(BASE_DIR + u'/train/'+industry+'/'):-4]] = model
    
    if status == 'Normal':
        all_data = pd.read_sql_query(Select_sql[status] + table_from + " WHERE TaggedItemAttr IS NOT NULL "
                                                                       "AND ((MonthlyOrders>=10 AND MonthlySalesQty=0) "
                                                                       "0R MonthlySalesQty>=10);", connect_industry)
    elif status == 'Monthly' or status == 'History':
        all_data = pd.read_sql_query(Select_sql[status] + table_from + " WHERE TaggedItemAttr IS NOT NULL"
                                                                       " AND TaggedItemAttr <> '';",
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

        """
        定义必要维度字典，在竞品选择的过程中 商品P1和P2和字典D做比较
        """
        essential_dist = get_essential_dict()

        # 对每个商品找竞品
        for i, item in enumerate(tqdm(items)):
            item_id, price, category_id = int(item[1]), float(item[3]), int(item[4])

            category_name = find_category_name_by_item_id(item_id)

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
            ############################################################################################################
            """
            JP001 如果P1的P2的维度的交，再交D的维度(set(P1.key) & set(P2.key)) & set(D.key)得到集合I
            JP002 对I的元素进行遍历, 求出P1下对应的维度值和P2下对应的维度值，查看他们交集是否等于并集
            如果是，则为同位，否则为异位
            set(P1.value) & set(P2.value) & set(D.value) == set(P1.value) | set(P2.value) & set(D.value)
            JP003 如果是同位，y则记为1否则为0
            """
            # 获取Label的字符串并从json解析成dict
            P1 = db_json_to_python_json(item[0])

            """竞品的标签"""
            # JP001
            for index, row in enumerate(all_data.values):
                # 针对每一个竞品，计算其key值集合
                P2 = db_json_to_python_json(row[0])
                # 引入字典做过滤器
                D = essential_dist[category_name]
                I = set(P1.keys()) & set(P2.keys()) & set(D.keys())
                # JP002
                if I:
                    # 如果有共同维度，要比较其对应的维度值是否一致
                    for key in I:
                        # 如果交集和并集一致，说明是同位，否则是异位
                        if (set(string_or_unicode_to_list(P1[key])) & set(string_or_unicode_to_list(P2[key])) & set(
                                D[key])) == (
                            set(string_or_unicode_to_list(P1[key])) | set(string_or_unicode_to_list(P2[key])) & set(
                                D[key])):
                            pass
                        else:
                            y[index] = 0
                else:
                    # 如果无共同维度，不用管它
                    pass

            """2016/6/20 - 废弃
            for i, row in enumerate(training_data.loc[:, ['ItemID', 'TaggedItemAttr']].values):
                item_id = row[0]
                tagged_item_attr = row[1]
                j_set = label_rebuilt_to_set(tagged_item_attr)
                if category_name not in essential_dist:
                    training_data = training_data[training_data.ItemID != item_id]
                else:
                    e_set = transform_essential_sub_dict_to_essential_set(essential_dist[category_name])
                    if e_set & j_set:
                        continue
                    else:
                        training_data = training_data[training_data.ItemID != item_id]
            """
            ############################################################################################################
            if status == 'Normal':
                insert_items += [(item_id, todo_id[j], 1, 1, value, y[j]) for j in xrange(len(y)) if y[j] > 0.5]
            elif status == 'History' or status == 'Monthly':
                insert_items += [(item_id, todo_id[j], 1, 1, value, todo_daterange[j], y[j]) for j in xrange(len(y)) if y[j] > 0.5]

        n = len(insert_items)      
        if n > 0:
            batch = 10000
            print '{} 正在插入{}条数据, split = 10000'.format(datetime.now(), n)
            for j in tqdm(xrange(int(ceil(float(n)/batch)))):
                cursor_industry.executemany("""INSERT INTO """ + table_to + Insert_sql[status], insert_items[j*batch:min((j+1)*batch, n)])
                connect_industry.commit()
          
    connect_industry.close()
    print datetime.now()
    return


def get_essential_dict(file_name= u'essentialvalue.csv'):
    """
    :param file_name:
    :return: essential_dict  dict(dict(list))
    """
    file_name_test = file_name
    f = open(file_name_test, 'rb')
    content = f.readlines()[1:]
    essential_dict = {}
    for row in content:

        _row = row.replace('\r', '').replace('\n', '').split(',')
        level1 = _row[0].decode('utf-8')
        level2 = _row[1].decode('utf-8')
        level3 = _row[2].decode('utf-8')

        if float(_row[-2]) >= 0.7 and int(_row[-1]) >= 6:
            #for i in _row:
                #print i
            pass
        else:
            continue

        if level1 in essential_dict:
            if level2 in essential_dict[level1]:
                if level3 in essential_dict[level1][level2]:
                    pass
                else:
                    essential_dict[level1][level2].append(level3)
            else:
                essential_dict[level1][level2] = level3.split('\t')
        else:
            essential_dict[level1] = {level2 : level3.split('\t')}
    f.close()
    return essential_dict


def find_category_name_by_item_id(item_id):
    db = MySQLDB()
    sql = 'SELECT c.CategoryName ' \
          'FROM mp_women_clothing.item i ' \
          'LEFT JOIN mp_portal.category c ' \
          'ON i.CategoryID = c.CategoryID ' \
          'WHERE i.ItemID = %s' % item_id
    ret = db.query(sql, )
    if ret:
        category_name = ret[0]['CategoryName']
    else:
        category_name = None
    return category_name


def transform_essential_sub_dict_to_essential_set(essential_sub_dict):
    e_list = []
    for k, v in essential_sub_dict.iteritems():
        if isinstance(v, list):
            for i in v:
                e_list.append(k + i)
        else:
            print 'k = %s, v = %s' % (k, v)
            e_list.append(k + v)
    return set(e_list)


if __name__ == '__main__':
    #print "BASE_DIR = %s" % BASE_DIR
    #industry_test = 'mp_women_clothing'
    #tag_list_test = BASE_DIR + u'/feature/' + industry_test + '/*.txt'  # 标签词库
    #print "tag_list = %s" % tag_list_test
    #head_test = [x[len(tag_list_test)-5:-4].replace(' ', '') for x in glob(tag_list_test)]
    #print "head = %s" % head_test
    #dict_head_test = {head_test[i]: i for i in xrange(len(head_test))}
    #print "dict_head = %s" % dict_head_test
    #print "head[0] = %s" % head_test[0]
    # print "dict_head[head[0]] = %s" % dict_head_test[head_test[0]]
    essential_dict = get_essential_dict()
    for e in essential_dict[u'打底裤'][u'att-做工工艺']:
        print e
    print '-' * 80
    #for k, v in essential_dict.iteritems():
    #   print k, v
    print transform_essential_sub_dict_to_essential_set(essential_dict[u'背心吊带'])
    test_string_none = "{'att-图案': '花式', " \
                  "'att-适用季节': '秋季', " \
                  "'att-色系': '多色', 'app-感官': ['优雅', '淑女', '时尚'], " \
                  "'att-面料': '蕾丝', 'att-做工工艺': '拼接', " \
                  "'att-款式-裙长': '短裙', 'fun': '打底', " \
                  "'att-做工工艺-流行元素': ['镂空', '蕾丝拼接', '印花', '纱网'], " \
                  "'att-款式-裙型': 'A字裙', " \
                  "'app-风格': ['通勤', '百搭', '韩风']}"

    j_set_test = label_rebuilt_to_set(test_string_none)
    e_set_test = transform_essential_sub_dict_to_essential_set(essential_dict[u'半身裙'])
    i = j_set_test & e_set_test
    print i
    test_string_have = "{'app-感官': '时尚', 'att-厚薄': '常规', 'att-适用季节': '春季', 'att-色系': ['中间色', '冷色系'], 'fea': ['舒适', '弹性', '亲肤'], 'att-填充物': ['丝绵', '棉'], 'att-面料': ['牛仔布', '棉', '化纤类'], 'att-做工工艺': ['水洗', '做旧'], 'att-款式-裤长': '九分裤', 'att-适用年龄': '18~24周岁', 'att-款式-版型': ['修身', '显瘦', '紧身'], 'att-款式-腰型': '自然腰', 'att-洗涤方式': '水洗', 'att-做工工艺-流行元素': '破洞', 'att-款式-裤型': '铅笔小脚', 'app-风格': ['韩风', '简约'], 'att-款式-裤门襟': '纽扣开襟'}"
    j_set_test = label_rebuilt_to_set(test_string_have)
    i = j_set_test & e_set_test
    print '-' * 80
    print i
    print '-' * 80
    for j in j_set_test:
        print j
    print '-' * 80
    for e in e_set_test:
        print e
