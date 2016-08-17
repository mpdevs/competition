# coding: utf-8
# __author__ = "John"
import pandas as pd
from tqdm import tqdm
from math import ceil
from datetime import datetime
from sql_constant import *
from common.mysql_helper import connect_db, MySQLDBPackage as MySQL


def get_item_attributes(db=u"mp_women_clothing", limits=u""):
    return pd.read_sql_query(ATTRIBUTES_QUERY.format(u"TaggedItemAttr", limits), connect_db(db))


def get_training_data(cid, db=u"mp_women_clothing"):
    return pd.read_sql_query(TRAINING_DATA_QUERY.format(cid), connect_db(db))


def get_customer_shop_items(db, table, category_id, date_range, shop_id=66098091):
    return pd.read_sql_query(CUSTOMER_ITEM_QUERY.format(table, shop_id, category_id, date_range), connect_db(db))


def get_competitor_shop_items(db, table, category_id, date_range, shop_id=66098091):
    return pd.read_sql_query(COMPETITIVE_ITEM_QUERY.format(table, shop_id, category_id, date_range), connect_db(db))


def delete_score(db, table, category_id, date_range, shop_id=66098091):
    db_connection = MySQL()
    db_connection.execute(DELETE_SCORES_QUERY.format(db, table, shop_id, category_id, date_range))
    return


def set_scores(db, table, args):
    """
    :param db:
    :param table:
    :param args:
    :return:
    """
    db_connection = MySQL()
    row_count = len(args)
    batch = int(ceil(float(row_count) / 100))
    for size in tqdm(range(batch)):
        start_index = size * 100
        end_index = min((size + 1) * 100, row_count)
        data = args[start_index: end_index]
        db_connection.execute_many(sql=SET_SCORES_QUERY.format(db, table), args=data)
    return

# region 文本处理，暂存
# def get_training_data_from_txt(db="mp_women_clothing", category_name=u"半身裙"):
#     """
#     旧的训练数据存放在文本文件中，现已废弃
#     :param db:
#     :param category_name:
#     :return:
#     """
#     import os
#     base_dir = os.path.join(os.path.dirname(__file__), "dicts")
#     train_path = u"{0}/train/{1}/".format(base_dir, db)
#     train_file = u"{0}{1}.txt".format(train_path, category_name)
#     ret = []
#     for index, row in enumerate(np.asarray(np.loadtxt(train_file), dtype=long)):
#         ret.append((row[0], row[1]))
#     return ret
# endregion


def get_max_date_range(db, table):
    """
    因为数据源和数据目的地都有年月控制的字段，所以需要对数据进行删选，同月份数据有可比性，不同月份则没有
    :param db:
    :param table:
    :return:
    """
    return pd.read_sql_query(MAX_DATE_RANGE_QUERY.format(db, table), connect_db()).values[0][0]


def get_essential_dimensions(db=u"mp_women_clothing"):
    """
    必要维度法需要的数据， 基本把潜在竞品筛完
    :param db:
    :return: OrderedDict
    """
    threshold = 0.75
    confidence = 6
    return pd.read_sql_query(ESSENTIAL_DIMENSIONS_QUERY.format(threshold, confidence), connect_db(db))


if __name__ == u"__main__":
    _industry = u"mp_women_clothing"
    _source_table = u"itemmonthlysales2015"
    _target_table = u"itemmonthlyrelation_2015"
    _shop_id = 66098091
    _date_range = u"2015-12-01"
    _category_id = 1623

    print u"{0} start testing get_training_data".format(datetime.now())
    r = get_training_data(1623)
    print u"get_training_data row count={0}".format(r.values.shape[0])

    print u"{0} start testing get_customer_shop_items".format(datetime.now())
    r = get_customer_shop_items(db=_industry, table=_source_table, shop_id=66098091, date_range=_date_range,
                                category_id=_category_id)
    print u"get_customer_shop_items row count={0}".format(r.values.shape[0])

    print u"{0} start testing get_competitor_shop_items".format(datetime.now())
    r = get_competitor_shop_items(db=_industry, table=_source_table, shop_id=_shop_id, category_id=1623,
                                  date_range=_date_range)
    print u"get_competitor_shop_items row count={0}".format(r.values.shape[0])

    # print u"{0} start testing set_scores".format(datetime.now())
    # batch_data = [(1, 1, 1, 0, datetime.now().strftime("%Y-%m-%d")),
    #               (0, 0, 0, 1, datetime.now().strftime("%Y-%m-%d"))]
    # set_scores(db="mp_women_clothing", table="itemmonthlyrelation_2016", args=batch_data)

    print u"{0} start testing get_max_date_range".format(datetime.now())
    dr = get_max_date_range(db=_industry, table=_source_table)
    print u"date_range={0}".format(dr)

    print u"{0} start testing get_essential_dimensions".format(datetime.now())
    r = get_essential_dimensions(db=_industry)
    print u"get_essential_dimensions row count={0}".format(r.values.shape[0])
