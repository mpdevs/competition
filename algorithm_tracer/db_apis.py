# coding: utf-8
# __author__ "John"
import pandas as pd
from datetime import datetime
from sql_constant import *
from os import path, sys
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from common.mysql_helper import connect_db


def get_competitive_item_pair_info(item1_id, item2_id, db=u"mp_women_clothing", source_table=u"itemmonthlysales2015",
                                   date_range=u"2015-12-01"):
    return pd.read_sql_query(PREDICT_PAIR_INFO_QUERY.format(
        source_table, item1_id, item2_id, date_range), connect_db(db))


def get_train_item_pair_info(item1_id, item2_id, db=u"mp_women_clothing"):
    return pd.read_sql_query(TRAIN_PAIR_INFO_QUERY.format(item1_id, item2_id), connect_db(db))


def get_category_id(table, item_id, db=u"mp_women_clothing"):
    return pd.read_sql_query(GET_CATEGORY_ID_QUERY.format(table, item_id), connect_db(db))


def get_tagged_item_info(item_id, db=u"mp_women_clothing"):
    return pd.read_sql_query(GET_TAGGED_ITEM_INFO.format(item_id), connect_db(db))


def get_category_displayname(category_id):
    return pd.read_sql_query(GET_CATEGORY_DISPLAYNAME_QUERY.format(category_id), connect_db())


if __name__ == u"__main__":
    _industry = u"mp_women_clothing"
    _source_table = u"itemmonthlysales2015"
    _target_table = u"itemmonthlyrelation_2015"
    _shop_id = 66098091
    _date_range = u"2015-12-01"
    _category_id = 1623
    print u"{0} start testing get_competitive_item_pair_info".format(datetime.now())
    r = get_competitive_item_pair_info(db=_industry, item1_id=528056085087, item2_id=528558659600,
                                       date_range=_date_range, source_table=_source_table)
    print u"get_competitive_item_pair_info row count={0}".format(r.values.shape[0])
    r = r[u"TaggedItemAttr"].values
    if r:
        print u"got data"
        print u"shape is {}".format(r.shape)
        print u"type[0] is {}".format(type(r[0]))
        print u"[0] value is {}".format(r[0])
    else:
        print u"no data"

    print u"{0} start testing get_train_item_pair_info".format(datetime.now())
    r = get_train_item_pair_info(item1_id=528056085087, item2_id=528558659600)
    print u"get_train_item_pair_info row count={0}".format(r.values.shape[0])

    print u"{0} start testing get_category_id".format(datetime.now())
    r = get_category_id(table=_source_table, db=_industry, item_id=525723538258)
    try:
        print u"category_id = {0}".format(r.values[0][0])
        print u"get_category_id row count={0}".format(r.values.shape[0])
    except IndexError:
        print u"no data"

    print u"{0} start testing get_tagged_item_info".format(datetime.now())
    r = get_tagged_item_info(item_id=40575063265)
    print u"get_tagged_item_info content is {0}".format(r.values.tolist()[0])

    print u"{0} start testing get_category_displayname".format(datetime.now())
    r = get_category_displayname(category_id=1623)
    print u"get_category_displayname content is {0}".format(r.values.tolist()[0])

    print u"".join(r.values.tolist()[0])


