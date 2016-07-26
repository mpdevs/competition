# coding: utf-8
# __author__ = "John"
from sql_constant import *
import pandas as pd
from tqdm import tqdm
from math import ceil
from datetime import datetime
from os import path, sys
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from common.mysql_helper import connect_db, MySQLDBPackage
from common.debug_helper import debug


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


def get_items_no_attr_data(db, table, category_id=1623):
    """
    获取某行业某个表某个品类下面所有的商品的属性
    :param db:
    :param table:
    :param category_id:
    :return:
    """
    debug(ITEMS_ATTR_OTHER_QUERY.format(table, category_id))
    return pd.read_sql_query(ITEMS_ATTR_OTHER_QUERY.format(table, category_id), connect_db(db))


def get_brand(db=u"mp_women_clothing"):
    debug(BRAND_QUERY.format(db))
    return pd.read_sql_query(BRAND_QUERY.format(db), connect_db(db))


def set_tag(args):
    db_connection = MySQLDBPackage()
    row_count = len(args)
    batch = int(ceil(float(row_count) / 100))
    for size in tqdm(range(batch)):
        start_index = size * 100
        end_index = min((size + 1) * 100, row_count-1)
        data = args[start_index: end_index]
        db_connection.execute_many(sql=u"something to replace", args=data)


if __name__ == u"__main__":
    _industry = u"mp_women_clothing"
    _source_table = u"itemmonthlysales2015"
    _target_table = u"itemmonthlyrelation_2015"
    _shop_id = 66098091
    _date_range = u"2015-12-01"
    _category_id = 1623
    print u"{0} start testing get_categorys".format(datetime.now())
    r = get_items_attr_data(db=u"mp_women_clothing", category_id=_category_id, table=_source_table)
    print u"get_categorys row count={0}".format(len(r))
