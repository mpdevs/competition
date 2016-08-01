# coding: utf-8
# __author__: u"John"
from os import path, sys
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from common.mysql_helper import *
from common.sql_constant import CATEGORY_QUERY
import pandas as pd
from tagger.helper import unicode_decoder
from tqdm import tqdm
from datetime import datetime
from openpyxl import load_workbook


def attr_desc_parser(data):
    """
    将爬虫获取的字符串数据转换成字典的格式
    :return: items_attr: list(dict(key=维度,value=维度值)), error_info: list, error_items: list
    """
    error_info = []
    error_items = []
    items_attr = []
    for item in tqdm(data):
        item[0] = str(item[0])
        attr_dict = dict()
        # 结尾逗号去除
        try:
            if item[1][-1] == u",":
                item[1] = item[1][0:-1]
            else:
                pass
        except IndexError:
            error_info.append(unicode(e))
            error_items.append(item[0])
        attr_desc = item[1].split(u",")

        string = u""

        for dimension_value in attr_desc:
            key_pair = dimension_value.split(u":")
            try:
                key = unicode_decoder(key_pair[0])
                value = unicode_decoder(key_pair[1])
                if key == u"材质成分":
                    string = u"{0}:{1}".format(key, value)
                    continue
            except IndexError as e:
                error_info.append(unicode(e))
                error_items.append(item[0])
                continue
        items_attr.append((item[0], string))
    return items_attr, zip(error_items, error_info)

db = u"mp_women_clothing"

SQL = CATEGORY_QUERY
r = pd.read_sql_query(sql=SQL.format(db, u""), con=connect_db()).values.tolist()
category_dict = {row[0]: row[1] for row in r}

# SQL = u"SELECT DISTINCT DateRange FROM itemmonthlysales2015"
# date_ranges = pd.read_sql_query(sql=SQL, con=connect_db(db)).values.tolist()
month = []
m = u"0%s"
for i in xrange(10):
    month.append(m % i)
month.append(u"10")
month.append(u"11")
month.append(u"12")
date_ranges = []
mm = u"2015-%s-01"
for i in month:
    date_ranges.append(mm % i)

for category in category_dict.keys():
    name = category_dict[category].replace(u"/", u"")
    file_name = u"{0}.xlsx".format(name)
    writer = pd.ExcelWriter(file_name)
    for date_range in date_ranges:
        print u"{0} 正在处理品类<{1}>, DateRange<{2}>".format(datetime.now(), name, date_range)
        SQL = u"""SELECT ItemID, ItemAttrDesc FROM itemmonthlysales2015
        WHERE (ItemAttrDesc IS NOT NULL OR ItemAttrDesc != '') AND CategoryID = {0} AND DateRange = '{1}';"""
        r = pd.read_sql_query(sql=SQL.format(category, date_range), con=connect_db(db)).values.tolist()
        print len(r)
        if len(r) == 0:
            continue
        result, error = attr_desc_parser(data=r)
        r = pd.DataFrame(data=result, columns=[u"ItemID", u"Material"])
        r.to_excel(excel_writer=writer, sheet_name=date_range, encoding=u"utf-8")
    writer.save()















