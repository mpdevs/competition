# coding: utf-8
# __author__ = 'John'
import os
import pandas as pd
import numpy as np
from tqdm import tqdm
from math import ceil
from datetime import datetime
from enums import CATEGORY_QUERY, ATTR_META_QUERY, ATTRIBUTES_QUERY, TRAINING_DATA_QUERY, GET_MAX_DATE_RANGE_QUERY
from enums import CUSTOMER_ITEM_QUERY, COMPETITIVE_ITEM_QUERY, SET_SCORES_QUERY, DELETE_SCORES_QUERY
from mysql_helper import connect_db, MySQLDBPackage


def get_categories():
    db = MySQLDBPackage()
    category_filter = "WHERE CategoryID IN (1623, 121412004, 162104, 50007068, 50011277)"
    ret = db.query(sql=CATEGORY_QUERY.format(category_filter))
    return ret


def get_attribute_meta(db="mp_women_clothing"):
    return pd.read_sql_query(ATTR_META_QUERY.format(db), connect_db())


def get_item_attributes(db="mp_women_clothing", limits=""):
    return pd.read_sql_query(ATTRIBUTES_QUERY.format("TaggedItemAttr", limits), connect_db(db))


def get_training_data(cid, db="mp_women_clothing"):
    return pd.read_sql_query(TRAINING_DATA_QUERY.format(db, cid), connect_db())


def get_customer_shop_items(db, table, shop_id, date_range):
    return pd.read_sql_query(CUSTOMER_ITEM_QUERY.format(db, table, shop_id, date_range), connect_db())


def get_competitor_shop_items(db, table, shop_id, category_id, date_range):
    df = pd.read_sql_query(COMPETITIVE_ITEM_QUERY.format(db, table, shop_id, category_id, date_range),
                           connect_db())
    return df


def delete_score(db, table, shop_id, category_id, date_range):
    db_connection = MySQLDBPackage()
    print u"{0} 开始删除店铺ID={1},品类ID=<{2}>,DateRange=<{3}>的竞品数据...".format(
        datetime.now(), shop_id, category_id, date_range)
    db_connection.execute(DELETE_SCORES_QUERY.format(db, table, shop_id, category_id, date_range))
    return


def set_scores(db, table, args):
    """
    :param db:
    :param table:
    :param args:
    :return:
    """
    # print SET_SCORES_QUERY.format(db, table) % args[0]
    db_connection = MySQLDBPackage()
    row_count = len(args)
    batch = int(ceil(float(row_count) / 100))
    for size in tqdm(range(batch)):
        start_index = size * 100
        end_index = min((size + 1) * 100, row_count-1)
        data = args[start_index: end_index]
        db_connection.execute_many(sql=SET_SCORES_QUERY.format(db, table), args=data)
    # db_connection = MySQLDBPackage()
    # for row in tqdm(args):
    #     db_connection.execute(sql=SET_SCORES_QUERY.format(db, table) % row)
    return


def get_training_data_from_txt(db="mp_women_clothing", category_name=u"半身裙"):
    """
    旧的训练数据存放在文本文件中，现已废弃
    :param db:
    :param category_name:
    :return:
    """
    base_dir = os.path.join(os.path.dirname(__file__), 'dicts')
    train_path = u'{0}/train/{1}/'.format(base_dir, db)
    train_file = u'{0}{1}.txt'.format(train_path, category_name)
    ret = []
    for index, row in enumerate(np.asarray(np.loadtxt(train_file), dtype=long)):
        ret.append((row[0], row[1]))
    return ret


def get_max_date_range(db, table):
    """
    因为数据源和数据目的地都有年月控制的字段，所以需要对数据进行删选，同月份数据有可比性，不同月份则没有
    :param db:
    :param table:
    :return:
    """
    return pd.read_sql_query(GET_MAX_DATE_RANGE_QUERY.format(db, table), connect_db()).values[0][0]


if __name__ == "__main__":
    print u"{0} start testing get_categorys".format(datetime.now())
    r = get_categories()
    for i in r:
        print i

    print u"{0} start testing get_attribute_meta".format(datetime.now())
    r = get_attribute_meta()
    for i in range(min(len(r.values), 5)):
        print r.values[i]

    print u"{0} start testing get_item_attributes".format(datetime.now())
    r = get_item_attributes()
    for i in range(min(len(r.values), 5)):
        print r.values[i]

    print u"{0} start testing get_training_data".format(datetime.now())
    r = get_training_data(1623)
    for i in range(min(len(r.values), 5)):
        print r.values[i]

    print u"{0} start testing get_customer_shop_items".format(datetime.now())
    r = get_customer_shop_items(db='mp_women_clothing', table='itemmonthlysales2015', shop_id=66098091,
                                date_range="2015-12-01")
    for i in range(min(len(r.values), 5)):
        print r.values[i]
    print r.DiscountPrice.values

    print u"{0} start testing get_competitor_shop_items".format(datetime.now())
    r = get_competitor_shop_items(db='mp_women_clothing', table='itemmonthlysales2015', shop_id=66098091,
                                  category_id=1623, date_range="2015-12-01")
    print u"{0} get_competitor_shop_items length={1}".format(datetime.now(), len(r.values))
    for i in range(min(len(r.values), 5)):
        print r.values[i]

    print u"{0} start testing set_scores".format(datetime.now())
    batch_data = [(1, 1, 1, 0, datetime.now().strftime("%Y-%m-%d")),
                  (0, 0, 0, 1, datetime.now().strftime("%Y-%m-%d"))]

    set_scores(db='mp_women_clothing', table='itemmonthlyrelation_2016', args=batch_data)

    print u"{0} start testing get_max_date_range".format(datetime.now())
    dr = get_max_date_range(db='mp_women_clothing', table='itemmonthlysales2015')
    print "date_range={0}".format(dr)
