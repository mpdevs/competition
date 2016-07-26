# coding: utf-8
# __author__: "John"
from sql_constant import *
from mysql_helper import *
import pandas as pd


def get_attribute_meta(db=u"mp_women_clothing"):
    return pd.read_sql_query(ATTR_META_QUERY.format(db), connect_db(db))


def get_categories(db=u"mp_women_clothing", category_id_list=[1623, 121412004, 162104, 50007068, 50011277]):
    if category_id_list:
        category_filter = u"AND CategoryID IN ("
        for category_id in category_id_list:
            category_filter += u"{0},".format(category_id)
        category_filter = category_filter[0:-1] + u")"
    else:
        category_filter = u""
    return pd.read_sql_query(CATEGORY_QUERY.format(db, category_filter), connect_db()).values

