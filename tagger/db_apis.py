# coding: utf-8
# __author__ = "John"
from sql_constant import *
import pandas as pd
from math import ceil
from datetime import datetime
from os import path, sys
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from common.mysql_helper import connect_db, MySQLDBPackage
from common.debug_helper import debug


def get_tag_list(category_id):
    return pd.read_sql_query(TAG_DICT_QUERY.format(category_id), connect_db())


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


def set_tag(db, table, column_name, args):
    db_connection = MySQLDBPackage()
    row_count = len(args)
    batch = int(ceil(float(row_count) / 100))
    for size in xrange(batch):
        start_index = size * 100
        end_index = min((size + 1) * 100, row_count-1)
        data = args[start_index: end_index]
        db_connection.execute_many(sql=SET_ATTR_QUERY.format(db, table, column_name), args=data)


def get_color():
    return pd.read_sql_query(COLOR_QUERY, con=connect_db())


if __name__ == u"__main__":
    _industry = u"mp_women_clothing"
    _table = u"TaggedItemAttr"
    _category_id = 1623
    print u"{0} start testing get_tag_list".format(datetime.now())
    r = get_tag_list(category_id=_category_id)
    print u"get_tag_list row count={0}".format(len(r))
    print u"{0} start testing get_items_attr_data".format(datetime.now())
    r = get_items_attr_data(db=_industry, category_id=_category_id, table=_table)
    print u"get_items_attr_data row count={0}".format(len(r))
    print u"{0} start testing get_items_no_attr_data".format(datetime.now())
    r = get_items_no_attr_data(db=_industry, category_id=_category_id, table=_table)
    print u"get_items_no_attr_data row count={0}".format(len(r))
    print u"{0} start testing get_brand".format(datetime.now())
    r = get_brand(db=_industry)
    print u"get_brand row count={0}".format(len(r))
    print u"{0} start testing get_color".format(datetime.now())
    r = get_color()
    print u"get_color row count={0}".format(len(r))
    print r.values.tolist()[0][0], r.values.tolist()[0][1], r.values.tolist()[0][2], r.values.tolist()[0][3]
    if not r.values.tolist()[0][3]:
        print u"No blurred color"

