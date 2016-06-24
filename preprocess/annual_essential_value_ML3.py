# -*- coding: utf-8 -*-
"""
基于训练模型和必要维度值
John 重构版
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
from helper import parser_label, Jaca, getcut, string_or_unicode_to_list, db_json_to_python_json, debug
from mp_preprocess.settings import host, user, pwd
from mysql_conn import MySQLDB


# 当前文件夹路径下的 dicts 文件夹
BASE_DIR = os.path.join(os.path.dirname(__file__), 'dicts')


def process_annual(industry, table_from, table_to, one_shop=None, setprecetage=0.2):
    """
    :param industry: 行业
    :param table_from: 数据源
    :param table_to: 数据目的地
    :param one_shop: 是否只有一家店
    :param setprecetage: 设定价格段上下浮动百分比
    :return:
    """
    status = table_type_extract(table_to)
    debug('{} status = {}'.format(datetime.now(), status))

    # DB连接
    print '{} 正在连接数据库{} ...'.format(datetime.now(), host)
    db_industry = MySQLDB(host=host, user=user, passwd=pwd, db=industry)
    db_portal = MySQLDB(host=host, user=user, passwd=pwd, db='mp_portal')
    connect_industry = MySQLdb.Connect(host=host, user=user, passwd=pwd, db=industry, charset='utf8')

    # 词库文件,这个词库必须和打标签的词库是一个
    tag_list = BASE_DIR + u'/feature/' + industry + '/*.txt'  # 标签词库
    debug(u'{} tag_list = {}'.format(datetime.now(), tag_list))

    # Category获取
    print u'{} 正在查询Category表 ...'.format(datetime.now())
    ret = db_portal.query('SELECT CategoryID,CategoryName,ParentID FROM category;', )
    print u'{} Category表查询完成 ...'.format(datetime.now())
    cid2name = {int(row['CategoryID']): row['CategoryName'] for row in ret}

    # 定义总共的二级维度列表
    fl = DICT_FL[industry]

    # 读取
    head = [x[len(tag_list)-5:-4].replace(' ', '') for x in glob(tag_list)]
    dict_head = {head[i]: i for i in xrange(len(head))}

    # 获取所需要处理的店铺
    print u'{} 正在获取店铺信息 ...'.format(datetime.now())
    if one_shop is None or one_shop == '':
        ret = db_portal.query("SELECT ShopID FROM shop where IsClient='y';", )
        shops = [int(row['ShopID']) for row in ret]
    else:
        shops = map(int, one_shop.split(','))
    print u"{} 共{}个店铺:".format(datetime.now(), len(shops))

    # 模型训练
    print u'{} 正在获取训练数据 ...'.format(datetime.now())
    training_data = pd.read_sql_query("Select ItemID, TaggedItemAttr "
                                      "FROM TaggedItemAttr "
                                      "WHERE TaggedItemAttr IS NOT NULL AND TaggedItemAttr != ''", connect_industry)
    print u'{} 训练数据获取完成，一共 {}条记录  ...'.format(datetime.now(), len(training_data))
    id2vec = {i: j for i, j in zip(training_data['ItemID'].values,
                                    parser_label(training_data['TaggedItemAttr'].values, dict_head))}
    print u'{} 训练数据向量化完成  ...'.format(datetime.now())

    from sklearn.ensemble import GradientBoostingRegressor as gbrt
    import random
    print u"{} 开始训练数据... ".format(datetime.now())

    #train = BASE_DIR + u'/train/' + industry + '/*.txt'

    if os.path.exists(BASE_DIR + u'/train/' + industry + '_false'):
        train_false = BASE_DIR + u'/train/' + industry + '_false/*.txt'
    else:
        train_false = None

    cut = getcut([fl], head)[0]
    # 模型会按照类目进行区分，一个类目就会有一个
    name2model = dict()

    # 5个负例的文本放在这里
    train = [BASE_DIR + u'/train/' + industry + u'/半身裙.txt',
            BASE_DIR + u'/train/' + industry + u'/衬衫.txt',
            BASE_DIR + u'/train/' + industry + u'/风衣.txt',
            BASE_DIR + u'/train/' + industry + u'/连衣裙.txt',
            BASE_DIR + u'/train/' + industry + u'/毛针织衫.txt']
    #for path in glob(train):
    for path in train:
        print u"{} 正在训练数据 {}... ".format(datetime.now(), path)
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

        X_false, y_false = [], []
        if train_false:
            for false_file_path in glob(train_false):
                  if file_name in false_file_path:
                    for row_false in np.asarray(np.loadtxt(false_file_path)):
                        try:  # 标注数据未   筛选
                            v1_false, v2_false = id2vec[long(row_false[0])], id2vec[long(row_false[1])]
                        except:
                            continue
                        X_false.append([Jaca(v1_false[c], v2_false[c]) for c in cut])
                        y_false.append(float(row_false[2]))
                    X_false, y_false = np.asarray(X_false), np.asarray(y_false)
                    mixed_set = True

        t = np.array(range(len(y)))
        X_true, y_true = X[t[y > 0.5]], y[y > 0.5]
        print np.shape(t), sum(y>0.5), len(y_false)
        # 新老方法结合点
        if mixed_set:
            t = np.array(range(len(y_false)))
            # 随机获取和同位数量等值的异位数据
            t = random.sample(t, sum(y > 0.5))
            X_false, y_false = X_false[t], y_false[t]
            print u'异位数为%s' % len(y_false)
        # 老方法
        else:
            t = np.array(range(len(y_false)))
            t = random.sample(t[y < 0.5], sum(y > 0.5))
            t = random.sample(t, len(t))
            X_false, y_false = X[t], y[t]
            print u'异位数为%s' % len(y_false)

        X = np.vstack((X_true, X_false))
        y = np.append(y_true, y_false)
        model = gbrt()
        model.fit(X, y)
        name2model[path[len(BASE_DIR + u'/train/'+industry+'/'):-4]] = model

    print name2model

    print u"{} 正在获取 {} 表的数据 :".format(datetime.now(), table_from)
    if status == 'Normal':
        all_data = pd.read_sql_query(Select_sql[status] + table_from + " WHERE TaggedItemAttr IS NOT NULL "
                                                                       "AND ((MonthlyOrders>=10 AND MonthlySalesQty=0) "
                                                                       "0R MonthlySalesQty>=10);", connect_industry)
    elif status == 'Monthly' or status == 'History':
        all_data = pd.read_sql_query(Select_sql[status] + table_from + " WHERE TaggedItemAttr IS NOT NULL"
                                                                       " AND TaggedItemAttr <> '';",connect_industry)

    print u"{} {} 表的数据获取完成，一共 {}条数据 :".format(datetime.now(), table_from, len(all_data))

    # 开始寻找竞品
    for value in shops:
        print datetime.now(), u'正在删除店铺%s数据 ...' % value
        db_industry.execute("DELETE FROM " + table_to + " WHERE shopid = %d" % value)
        print datetime.now(), u'正在读取店铺%s ...' % value

        if status == 'Normal' or status == 'Monthly':
            items = db_industry.query(Select_sql[status] + table_from + " WHERE ShopID=%d "
                                                                      "AND TaggedItemAttr IS NOT NULL "
                                                                      "AND TaggedItemAttr!='';" % value)
        elif status == 'History':
            items = db_industry.query(Select_sql[status].replace(', DateRange', '') +
                                    " item" +
                                    " WHERE ShopID=%d AND TaggedItemAttr IS NOT NULL AND TaggedItemAttr!='';" % value)

        if items:
            pass
        else:
            continue

        item_label = parser_label([row['label'] for row in items], dict_head)
        insert_items = []
        print u'{} 正在计算店铺%s ...'.format(datetime.now(), value)

        """
        定义必要维度字典，在竞品选择的过程中 商品P1和P2和字典D做比较
        """
        essential_dist = get_essential_dict()

        # 对每个商品找竞品
        for i, item in enumerate(tqdm(items)):
            item_id, price, category_id = long(item['itemid']), float(item['DiscountPrice']), int(item['CategoryID'])

            category_name = cid2name[category_id]

            if price == 0:
                continue
            else:
                pass

            if cid2name[category_id] in name2model.keys():
                pass
            else:
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
                                     (all_data.DateRange == item['DateRange'])]
            else:
                pass

            if len(todo_data) == 0:
                continue
            else:
                pass

            if status == 'Monthly' or status == 'History':
                todo_daterange = todo_data['DateRange'].values
            else:
                pass

            # 计算相似度
            v1 = item_label[i]
            todo_id = todo_data['itemid'].values
            X = [[Jaca(v1[c], v2[c]) for c in cut] for v2 in parser_label(todo_data['label'].values, dict_head)]
            y = name2model[cid2name[category_id]].predict(X)
            print '{} X的数量为{} all_data数量为{} Y的数量为{}'.format(
                datetime.now(), np.shape(X)[0], len(all_data),np.shape(y))
            ########################################### 必要维度法 #####################################################
            """
            JP001 如果P1的P2的维度的交，再交D的维度(set(P1.key) & set(P2.key)) & set(D.key)得到集合I
            JP002 对I的元素进行遍历, 求出P1下对应的维度值和P2下对应的维度值，查看他们交集是否等于并集
            如果是，则为同位，否则为异位
            set(P1.value) & set(P2.value) & set(D.value) == set(P1.value) | set(P2.value) & set(D.value)
            JP003 如果是同位，y则记为1否则为0
            """
            # 获取Label的字符串并从json解析成dict
            P1 = db_json_to_python_json(item['label'])
            D = essential_dist[category_name]
            """竞品的标签"""
            # JP001
            for index, row in enumerate(todo_data.values):
                # 针对每一个竞品，计算其key值集合
                P2 = db_json_to_python_json(row[0])
                if essential_attribute_disturb_dict_key_is_list(P1, P2, D):
                    pass
                else:
                    y[index - 1] = 0
            ############################################################################################################
            if status == 'Normal':
                insert_items += [(item_id, todo_id[j], 1, 1, value, y[j]) for j in xrange(len(y)) if y[j] > 0.5]
            elif status == 'History' or status == 'Monthly':
                insert_items += [(item_id, todo_id[j], 1, 1, value, todo_daterange[j], y[j]) for j in xrange(len(y)) if y[j] > 0.5]

        n = len(insert_items)
        print u'{} 店铺 {}有 {}条待插入数据...'.format(datetime.now(), value, n)
        if n> 0:
            batch = 10000
            print '{} 正在插入{}条数据, split = 10000'.format(datetime.now(), n)
            for j in tqdm(xrange(int(ceil(float(n)/batch)))):
                db_industry.execute_many("""INSERT INTO """ + table_to + Insert_sql[status], insert_items[j*batch:min((j+1)*batch, n)])
                connect_industry.commit()

        print u'{} 店铺 {} 计算完毕...'.format(datetime.now(), value)
    connect_industry.close()
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

        if float(_row[-2]) >= 0.9 and int(_row[-1]) >= 6:
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


def table_type_extract(table_name):
    if table_name.find('monthly') != -1:
        status = 'Monthly'
    elif table_name.find('history') != -1:
        status = 'History'
    else:
        status = 'Norma l'
    return status


def essential_attribute_disturb_dict_key_is_list(P1, P2, D):
    """
    P1 dict[list]
    P2 dict[list]
    P3 dict[list]
    """
    P1P2 = set(P1.keys()) & set(P2.keys())
    P1D = set(P1.keys()) & set(D.keys())
    P2D = set(P2.keys()) & set(D.keys())
    I = P1D & P2D

    # debug("P1P2 = %s") % P1P2
    # debug("P1D = %s") % P1D
    # debug("P2D = %s") % P2D
    # debug("I = %s") % I
    # print P1P2

    # 1. 商品之间有维度相交
    if P1P2:
        # 1.1 都和必要维度无交集
        if not P1D and not P2D:
            print u'商品之间有维度相交, 都必要维度无交集, 我不管'
            return True
        # 1.2 只有一个商品有必要维度相交
        elif (P1D and not P2D) or (P2D and not P1D):
            print u'商品之间有维度相交, 只有一个商品有必要维度相交, 异位'
            return False
        # 两个商品都有必要维度
        else:
            # 1.3 两个商品的必要维度相等
            if P1D == P2D:
                for key in I:
                    # print "str(P1[key])=%s, str(P2[key])=%s" % (str(P1[key]), str(P2[key]))
                    # 1.30 判断每个属性下面是否所有值都相等
                    if set(P1[key]) == set(P2[key]):
                        continue
                    # 1.31 同维度下有不同的维度值
                    else:
                        print u'商品之间有维度相交, 同维度下有不同的维度值, 异位'
                        return False
                # 1.32 所有维度值相等
                print u'商品之间有维度相交, 两个商品的所有维度值相等, 我不管'
                return True
                # 1.32 判断每个属性下面是否所有值都相等
            # 1.4 两个商品的必要维度不等
            else:
                print u'商品之间有维度相交, 两个商品的必要维度不等, 异位'
                return False

    # 2. 商品之间没有维度相交
    else:
        # 2.1 都没有必要维度
        if not P1D and not P2D:
            print u'商品之间没有维度相交, 都没有必要维度, 我不管'
            return True
        # 2.2 有一个有必要维度
        elif (P1D and not P2D) or (P2D and not P1D):
            print u'商品之间没有维度相交, 有一个有必要维度, 异位'
            return False
        # 2.3 都有必要维度
        else:
            print u'商品之间没有维度相交, 都有必要维度, 异位'
            return False
    return





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
    #essential_dict = get_essential_dict()
    #for e in essential_dict[u'打底裤'][u'att-做工工艺']:
    #    print e
    #print '-' * 80
    ##for k, v in essential_dict.iteritems():
    ##   print k, v
    #print transform_essential_sub_dict_to_essential_set(essential_dict[u'背心吊带'])
    #test_string_none = "{'att-图案': '花式', " \
    #              "'att-适用季节': '秋季', " \
    #              "'a#tt-色系': '多色', 'app-感官': ['优雅', '淑女', '时尚'], " \
    #              "'att-面料': '蕾丝', 'att-做工工艺': '拼接', " \
    #              "'att-款式-裙长': '短裙', 'fun': '打底', " \
    #              "'att-做工工艺-流行元素': ['镂空', '蕾丝拼接', '印花', '纱网'], " \
    #              "'att-款式-裙型': 'A字裙', " \
    #              "'app-风格': ['通勤', '百搭', '韩风']}"
#
    #j_set_test = label_rebuilt_to_set(test_string_none)
    #e_set_test = transform_essential_sub_dict_to_essential_set(essential_dict[u'半身裙'])
    #i = j_set_test & e_set_test
    #print i
    #test_string_have = "{'app-感官': '时尚', 'att-厚薄': '常规', 'att-适用季节': '春季', 'att-色系': ['中间色', '冷色系'], 'fea': ['舒适', '弹性', '亲肤'], 'att-填充物': ['丝绵', '棉'], 'att-面料': ['牛仔布', '棉', '化纤类'], 'att-做工工艺': ['水洗', '做旧'], 'att-款式-裤长': '九分裤', 'att-适用年龄': '18~24周岁', 'att-款式-版型': ['修身', '显瘦', '紧身'], 'att-款式-腰型': '自然腰', 'att-洗涤方式': '水洗', 'att-做工工艺-流行元素': '破洞', 'att-款式-裤型': '铅笔小脚', 'app-风格': ['韩风', '简约'], 'att-款式-裤门襟': '纽扣开襟'}"
    #j_set_test = label_rebuilt_to_set(test_string_have)
    #i = j_set_test & e_set_test
    #print '-' * 80
    #print i
    #print '-' * 80
    #for j in j_set_test:
    #    print j
    #print '-' * 80
    #for e in e_set_test:
    #    print e
    process_annual(industry='mp_women_clothing', table_from='itemmonthlysales2016', table_to='itemmonthlyrelation_2016',
                   one_shop='66098091')
