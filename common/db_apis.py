# coding: utf-8
# __author__: "John"
from sql_constant import *
from mysql_helper import *
from debug_helper import *
import pandas as pd


def get_attribute_meta(db=u"mp_women_clothing"):
    debug(ATTR_META_QUERY.format(db))
    return pd.read_sql_query(ATTR_META_QUERY.format(db), connect_db(db))


def get_categories(db=u"mp_women_clothing", category_id_list=[121412004]):
    # categories that have essential tag: 1623, 162104, 50007068, 50011277
    # all categories under women_clothing: [162116, 1623, 162104, 50000671, 162103, 50008901, 50011277]
    # Phase 3 new categories: [50010850, 50000697, 50008898, 162201]
    if category_id_list:
        category_filter = u"AND CategoryID IN ("
        for category_id in category_id_list:
            category_filter += u"{0},".format(category_id)
        category_filter = category_filter[0:-1] + u")"
    else:
        category_filter = u""
    debug(CATEGORY_QUERY.format(db, category_filter))
    return pd.read_sql_query(CATEGORY_QUERY.format(db, category_filter), connect_db()).values


def get_date_ranges_list(db, table):
    debug(GET_DATE_RANGES_QUERY.format(table))
    df = pd.read_sql_query(GET_DATE_RANGES_QUERY.format(table), connect_db(db))
    return [line[0] for line in df.values.tolist()]


if __name__ == u"__main__":
    # _db = u"mp_women_clothing"
    # _table = u"TaggedItemAttr"
    # r = get_date_ranges_list(db=_db, table=_table)
    # for i in r:
    #     print i

    _db = u'mp_portal'
    _table = u'attr_value'
    ATTR_DICT_QUERY = u"""SELECT DisplayName, AttrValue FROM mp_portal.`attr_value`;"""
    attr_dict = pd.read_sql_query(ATTR_DICT_QUERY, connect_db(_db))
    print attr_dict
    attr_dict.to_csv('Attr_dict.csv', encoding='utf8')

