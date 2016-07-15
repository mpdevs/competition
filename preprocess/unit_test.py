# coding: utf-8
import MySQLdb
import pandas as pd
from datetime import datetime
from enums import DICT_EXCLUSIVES
from tag_process import tagging_items, tagging_material
from helper import tag_to_matrix

host = '192.168.1.120'
user = 'dev'
pwd = 'Dev_123123'

industry = 'mp_women_clothing'
table_name = 'item_dev'

exclusive_list = DICT_EXCLUSIVES[industry]
connect = MySQLdb.Connect(host=host, user=user, passwd=pwd, db=industry, charset='utf8')
portal = MySQLdb.Connect(host=host, user=user, passwd=pwd, db='mp_portal', charset='utf8')

item_info_query = """SELECT ItemID, CategoryID, concat_ws(' ',ItemSubTitle,ItemName) AS Title,
ItemAttrDesc AS Attribute, concat_ws(' ',ShopName,ItemSubTitle,ItemName) AS ShopNameTitle
FROM {0}
; """.format(table_name)
# WHERE ItemID = '2847830213'

tag_dicts_query = """SELECT a.CID, a.Attrname, a.DisplayName, a.AttrValue, a.Flag
FROM mp_portal.attr_value AS a
JOIN mp_portal.industry AS i
ON a.IndustryID = i.IndustryID
WHERE a.IsTag='y'
AND i.DBName ='{0}'
""".format(industry)


def tagging_items_test():
    print '{0} start tagging_items_test()...'.format(datetime.now())
    print '{0} start item_info_query...'.format(datetime.now())
    data = pd.read_sql_query(item_info_query, connect)
    print '{0} start tag_dicts_query...'.format(datetime.now())
    tag_dicts = pd.read_sql_query(tag_dicts_query, portal)

    tag_preparation = dict()
    for cid in tag_dicts['CID'].unique():
        tag_preparation[int(cid)] = {(x[1], x[2], x[4]): x[3].rstrip(',').replace(' ', '').split(',')
                                     # type(x) = ndarray
                                     for x in tag_dicts[tag_dicts['CID'] == cid].values}

    print '{0} start tagging_items...'.format(datetime.now())
    ret = tagging_items(data, tag_preparation, exclusive_list)
    for i in range(min(len(ret), 5)):
        print ret[i]
    print '{0} start testing "," value...'.format(datetime.now())
    count = 0
    for i in ret:
        if i == u",":
            count += 1
    if count:
        print '{0} "," value count:{1}...'.format(datetime.now(), count)


def tagging_material_test():
    print '{0} start tagging_material_test()...'.format(datetime.now())
    print '{0} start item_info_query...'.format(datetime.now())
    data = pd.read_sql_query(item_info_query, connect)
    print '{0} start tag_dicts_query...'.format(datetime.now())
    tag_dicts = pd.read_sql_query(tag_dicts_query, portal)

    tag_preparation = dict()
    for cid in tag_dicts['CID'].unique():
        tag_preparation[int(cid)] = {(x[1], x[2], x[4]): x[3].rstrip(',').replace(' ', '').split(',')
                                     # type(x) = ndarray
                                     for x in tag_dicts[tag_dicts['CID'] == cid].values}

    print '{0} start tagging_material...'.format(datetime.now())
    ret = tagging_material(data, tag_preparation)
    count = 0
    for i in range(min(len(ret), 5)):
        if count <= 5 and ret[i]:
            count += 1
            print ret[i]
    print '{0} start testing "," value...'.format(datetime.now())
    count = 0
    for i in ret:
        if i == u",":
            count += 1
    if count:
        print '{0} "," value count:{1}...'.format(datetime.now(), count)


def parse_label_for_string_test():

    print '{0} start training_data_query...'.format(datetime.now())
    training_data_sql = """SELECT CategoryId AS CID, ItemAttrDesc, TaggedItemAttr
    FROM item_dev WHERE ItemID IN ('522856633815', '529417252828');"""
    training_data = pd.read_sql_query(training_data_sql, connect)

    print '{0} start tag_dicts_query...'.format(datetime.now())
    tag_dicts = pd.read_sql_query(tag_dicts_query, portal)

    dimensions = tag_dicts[u'Attrname'].tolist()
    values_str = tag_dicts[u'AttrValue'].tolist()
    head_raw = zip(dimensions, values_str)
    head_list = []
    for row in head_raw:
        for attr in row[1].split(u','):
            head = u'{0}-{1}'.format(row[0], attr)
            head_list.append(head)
    head_list = list(set(head_list))
    head_dict = {head_list[i]: i for i in xrange(len(head_list))}
    label_string_list = training_data['TaggedItemAttr'].tolist()
    result = tag_to_matrix(label_string_list, head_dict)
    print 'over'


if __name__ == '__main__':
    parse_label_for_string_test()
