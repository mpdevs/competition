# coding: utf-8
# __author__: "John"
import MySQLdb
import pandas as pd
from datetime import datetime
from helper import tag_to_matrix

host = '192.168.1.120'
user = 'dev'
pwd = 'Dev_123123'

industry = 'mp_women_clothing'
table_name = 'item_dev'

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
