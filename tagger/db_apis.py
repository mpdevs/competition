# coding: utf-8
# __author__ = "John"
from sql_constant import *
import pandas as pd
from math import ceil
from datetime import datetime
from common.mysql_helper import connect_db, MySQLDBPackage
from common.debug_helper import debug
from common.pickle_helper import *
from common.settings import NAME_ATTRIBUTE_PICKLE


def get_tag_list(category_id):
    """
    根据品类获取属性的维度和值
    :param category_id:
    :return:
    """
    return pd.read_sql_query(TAG_DICT_QUERY.format(category_id), connect_db())


def get_items_attr_data(db, table, category_id=1623, retag=u" AND NeedReTag = 'y'"):
    """
    获取某行业某个表某个品类下面所有的商品的属性，这部分是
    :param db:
    :param table:
    :param category_id:
    :param retag:
    :return:
    """
    return pd.read_sql_query(ITEMS_ATTR_DESC_QUERY.format(table, category_id, retag), connect_db(db))


def get_items_no_attr_data(db, table, category_id=1623):
    """
    获取某行业某个表某个品类下面所有的商品的属性
    :param db:
    :param table:
    :param category_id:
    :return:
    """
    return pd.read_sql_query(ITEMS_ATTR_OTHER_QUERY.format(table, category_id), connect_db(db))


def get_items_attr(db, table, category_id, date_range=None, incremental=True):
    """
    提取所有相关的AttrDescription
    :param db: 行业
    :param table: 数据源
    :param category_id: 当前品类
    :param incremental: 是否增量
    :param date_range: 年度表需要该字典
    :return: DataFrame[[ItemID, CategoryID, Attribute, HasDescription]]
    """
    if incremental:
        retag = u" AND NeedReTag = 'y'"
    else:
        retag = u""
    if date_range:
        date_range_filter = u" AND DateRange = '{0}'".format(date_range)
    else:
        date_range_filter = u""
    debug(ITEMS_ATTR_QUERY.format(table, category_id, date_range_filter, retag))
    return pd.read_sql_query(ITEMS_ATTR_QUERY.format(table, category_id, date_range_filter, retag), connect_db(db))


def get_item_attr(db, table, item_id, date_range=None):
    """
    提取一条相关的AttrDescription
    :param db: 行业
    :param table: 数据源
    :param item_id: 当前商品ID
    :param date_range: 年度表需要该字典
    :return: DataFrame[[ItemID, CategoryID, Attribute, HasDescription]]
    """
    if date_range:
        date_range_filter = u" AND DateRange = '{0}'".format(date_range)
    else:
        date_range_filter = u""
    debug(ITEM_ATTR_QUERY.format(table, item_id, date_range_filter))
    return pd.read_sql_query(ITEM_ATTR_QUERY.format(table, item_id, date_range_filter), connect_db(db))


def get_brand(db=u"mp_women_clothing"):
    """
    根据行业获取品牌列表
    :param db:
    :return:
    """
    return pd.read_sql_query(BRAND_QUERY.format(db), connect_db(db))


def update_tag(db, table, column_name, args):
    """
    将计算好的标签更新到指定的列
    :param db:
    :param table:
    :param column_name:
    :param args:
    :return:
    """
    db_connection = MySQLDBPackage()
    row_count = len(args)
    batch = int(ceil(float(row_count) / 100))
    debug(SET_ATTR_QUERY.format(db, table, column_name) % args[0])
    for size in xrange(batch):
        start_index = size * 100
        end_index = min((size + 1) * 100, row_count)
        data = args[start_index: end_index]
        db_connection.execute_many(sql=SET_ATTR_QUERY.format(db, table, column_name), args=data)
    return


def get_color():
    """
    获取颜色组的列表
    :return:
    """
    return pd.read_sql_query(COLOR_QUERY, con=connect_db())


def get_simplified_color():
    """
    在颜色组获取的时候就处理好所需要的格式，提高效率
    :return:
    """
    df = pd.read_sql_query(COLOR_SIMPLIFIED_QUERY, con=connect_db())
    return df if not df.empty else []


def get_item_attr_data(db, table, item_id):
    """
    获取某行业某个表某个品类下面所有的商品的属性
    :param db:
    :param table:
    :param item_id:
    :return:
    """
    return pd.read_sql_query(ITEM_ATTR_DESC_QUERY.format(table, item_id), connect_db(db))


def get_item_no_attr_data(db, table, item_id):
    """
    获取某行业某个表某个品类下面所有的商品的属性
    :param db:
    :param table:
    :param item_id:
    :return:
    """
    return pd.read_sql_query(ITEMS_ATTR_OTHER_QUERY.format(table, item_id), connect_db(db))


def get_category_by_item_id(db, table, item_id):
    """
    根据商品ID获取其对应的品类ID
    :param db:
    :param table:
    :param item_id:
    :return: int
    """
    debug(CATEGORY_BY_ITEM_QUERY.format(db, table, item_id))
    df = pd.read_sql_query(CATEGORY_BY_ITEM_QUERY.format(table, item_id), connect_db(db))
    return df.values[0][0] if not df.empty else None


def get_tag_attribute_meta(db, category_id):
    """
    获取属性维度值列表
    :param db:
    :param category_id:
    :return:
    """
    if category_id:
        category_filter = u" AND a.CID = {0}".format(category_id)
    else:
        category_filter = u""
    return pd.read_sql_query(TAG_ATTR_META_QUERY.format(db, category_filter), connect_db(db))


def make_ambiguous_attr_value():
    """
    生成歧义维度值列表，并导出成pickle文件
    :return: 
    """
    xlsx = pd.ExcelFile(u"name_dimension_value.xlsx")
    df = xlsx.parse(xlsx.sheet_names[0])
    pickle_dump(file_name=NAME_ATTRIBUTE_PICKLE, dump_object=df)
    return


def get_ambiguous_attr_value(category_id):
    """
    获取歧义维度值列表，从pickle文件转换成DataFrame
    :param category_id:
    :return:  DataFrame
    """
    df = pickle_load(file_name=NAME_ATTRIBUTE_PICKLE)
    df = df[[u"CategoryID", u"CategoryName", u"AttrName", u"AttrValue", u"Flag"]]
    return df[df.CategoryID == category_id]


def attr_value_reorder():
    """
    一次性的函数，将属性值的顺序按照长度倒序排列
    :return:
    """
    db = MySQLDBPackage()
    data = db.query(REORDER_QUERY)
    args = []
    for i, row in enumerate(data):
        new_row = row[0].split(u",")
        new_row = sorted(new_row, key=len)
        new_row = new_row[::-1]
        new_string = u",".join(new_row)
        args.append((new_string, row[1]))
    db.execute_many(UPDATE_REORDER_QUERY, args)
    return


if __name__ == u"__main__":
    _industry = u"mp_women_clothing"
    _table = u"TaggedItemAttr"
    _category_id = 1623
    _item_id = 526270140664
    # print u"{0} start testing get_tag_list".format(datetime.now())
    # r = get_tag_list(category_id=_category_id)
    # print u"get_tag_list row count={0}".format(len(r))
    # print u"{0} start testing get_items_attr_data".format(datetime.now())
    # r = get_items_attr_data(db=_industry, category_id=_category_id, table=_table)
    # print u"get_items_attr_data row count={0}".format(len(r))
    # print u"{0} start testing get_items_no_attr_data".format(datetime.now())
    # r = get_items_no_attr_data(db=_industry, category_id=_category_id, table=_table)
    # print u"get_items_no_attr_data row count={0}".format(len(r))
    print u"{0} start testing get_brand".format(datetime.now())
    r = get_brand(db=_industry)
    print u"get_brand row count={0}".format(len(r))

    # print u"{0} start testing get_color".format(datetime.now())
    # r = get_color()
    # print u"get_color row count={0}".format(len(r))
    # print r.values.tolist()[0][0], r.values.tolist()[0][1], r.values.tolist()[0][2], r.values.tolist()[0][3]
    # if not r.values.tolist()[0][3]:
    #     print u"No blurred color"
    print u"{0} start testing get_item_attr_data".format(datetime.now())
    r = get_item_attr_data(db=_industry, table=_table, item_id=526270140664)
    print u"get_item_attr_data row count={0}".format(len(r))
    print u"{0} start testing get_item_no_attr_data".format(datetime.now())
    r = get_item_no_attr_data(db=_industry, table=_table, item_id=526270140664)
    print u"get_item_no_attr_data row count={0}".format(len(r))
    make_ambiguous_attr_value()
    print u"{0} start testing get_ambiguous_attr_value_dict".format(datetime.now())
    r = get_ambiguous_attr_value(category_id=_category_id)
    print u"get_ambiguous_attr_value_dict row count={0}".format(len(r))
    # attr_value_reorder()
    print u"{0} start testing get_items_attr".format(datetime.now())
    r = get_items_attr(db=_industry, table=_table, category_id=_category_id, date_range=u"2015-12-01")
    print u"get_items_attr row count={0}".format(len(r))
    print r
    if not r.empty:
        print u"has data"
    else:
        print u"no data"
    print u"{0} start testing get_tag_attribute_meta".format(datetime.now())
    r = get_tag_attribute_meta(db=_industry, category_id=_category_id)
    print u"get_tag_attribute_meta row count={0}".format(len(r))








