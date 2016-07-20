# coding: utf-8
# __author__ = "John"
from sql_constant import *
import os
import pandas as pd
import numpy as np
from tqdm import tqdm
from math import ceil
from datetime import datetime
from os import path, sys
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from common.mysql_helper import connect_db, MySQLDBPackage
from common.debug_helper import debug


def get_categories(db="mp_women_clothing", category_id_list=[1623, 121412004, 162104, 50007068, 50011277]):
    if category_id_list:
        category_filter = u"AND CategoryID IN ("
        for category_id in category_id_list:
            category_filter += u"{0},".format(category_id)
        category_filter = category_filter[0:-1] + u")"
    else:
        category_filter = u""
    return pd.read_sql_query(CATEGORY_QUERY.format(db, category_filter), connect_db()).values


def get_attribute_meta(db="mp_women_clothing"):
    return pd.read_sql_query(ATTR_META_QUERY.format(db), connect_db(db))


def get_item_attributes(db="mp_women_clothing", limits=""):
    return pd.read_sql_query(ATTRIBUTES_QUERY.format("TaggedItemAttr", limits), connect_db(db))


def get_training_data(cid, db="mp_women_clothing"):
    return pd.read_sql_query(TRAINING_DATA_QUERY.format(cid), connect_db(db))


def get_customer_shop_items(db, table, category_id, date_range, shop_id=66098091):
    return pd.read_sql_query(CUSTOMER_ITEM_QUERY.format(table, shop_id, category_id, date_range), connect_db(db))


def get_competitor_shop_items(db, table, category_id, date_range, shop_id=66098091):
    return pd.read_sql_query(COMPETITIVE_ITEM_QUERY.format(table, shop_id, category_id, date_range), connect_db(db))


def delete_score(db, table, category_id, date_range, shop_id=66098091):
    db_connection = MySQLDBPackage()
    db_connection.execute(DELETE_SCORES_QUERY.format(db, table, shop_id, category_id, date_range))
    return


def set_scores(db, table, args):
    """
    :param db:
    :param table:
    :param args:
    :return:
    """
    db_connection = MySQLDBPackage()
    row_count = len(args)
    batch = int(ceil(float(row_count) / 100))
    for size in tqdm(range(batch)):
        start_index = size * 100
        end_index = min((size + 1) * 100, row_count)
        data = args[start_index: end_index]
        db_connection.execute_many(sql=SET_SCORES_QUERY.format(db, table), args=data)
    return


def get_training_data_from_txt(db="mp_women_clothing", category_name=u"半身裙"):
    """
    旧的训练数据存放在文本文件中，现已废弃
    :param db:
    :param category_name:
    :return:
    """
    base_dir = os.path.join(os.path.dirname(__file__), "dicts")
    train_path = u"{0}/train/{1}/".format(base_dir, db)
    train_file = u"{0}{1}.txt".format(train_path, category_name)
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
    return pd.read_sql_query(MAX_DATE_RANGE_QUERY.format(db, table), connect_db()).values[0][0]


def get_essential_dimensions(db="mp_women_clothing"):
    """
    必要维度法需要的数据， 基本把潜在竞品筛完
    :param db:
    :return: OrderedDict
    """
    threshold = 0.75
    confidence = 6
    return pd.read_sql_query(ESSENTIAL_DIMENSIONS_QUERY.format(threshold, confidence), connect_db(db))


def get_items_attr_data(db, table, category_id=1623):
    """
    获取某行业某个表某个品类下面所有的商品的属性
    :param db:
    :param table:
    :param category_id:
    :return:
    """
    debug(ITEMS_ATTR_DESC_QUERY.format(table, category_id))
    return pd.read_sql_query(ITEMS_ATTR_DESC_QUERY.format(table, category_id), connect_db(db))


def set_tag(db, table, args):
    db_connection = MySQLDBPackage()
    row_count = len(args)
    batch = int(ceil(float(row_count) / 100))
    for size in tqdm(range(batch)):
        start_index = size * 100
        end_index = min((size + 1) * 100, row_count-1)
        data = args[start_index: end_index]
        db_connection.execute_many(sql=SET_SCORES_QUERY.format(db, table), args=data)


def get_competitive_item_pair_info(item1_id, item2_id, db=u"mp_women_clothing", source_table=u"itemmonthlysales2015",
                                   date_range=u"2015-12-01"):
    return pd.read_sql_query(PREDICT_PAIR_INFO_QUERY.format(
        source_table, item1_id, item2_id, date_range), connect_db(db))


def get_train_item_pair_info(item1_id, item2_id, db=u"mp_women_clothing"):
    return pd.read_sql_query(TRAIN_PAIR_INFO_QUERY.format(item1_id, item2_id), connect_db(db))


def get_category_id(table, item_id, db=u"mp_women_clothing"):
    return pd.read_sql_query(GET_CATEGORY_ID_QUERY.format(table, item_id), connect_db(db))

if __name__ == u"__main__":
    _industry = u"mp_women_clothing"
    _source_table = u"itemmonthlysales2015"
    _target_table = u"itemmonthlyrelation_2015"
    _shop_id = 66098091
    _date_range = u"2015-12-01"
    _category_id = 1623
    print u"{0} start testing get_categorys".format(datetime.now())
    r = get_categories()
    print u"get_categorys row count={0}".format(len(r))
    print u"{0} start testing get_categorys".format(datetime.now())
    r = get_items_attr_data(db=u"mp_women_clothing", category_id=_category_id, table=_source_table)
    print u"get_categorys row count={0}".format(len(r))
