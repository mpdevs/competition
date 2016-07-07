# -*- coding: utf-8 -*
# __author__: huang_yanhua
"""
打标签主程序
留坑:
爬虫程序会对取回来的数据进行逗号替换，英文逗号会替换成中文逗号
"""
import os
import MySQLdb
import pandas as pd
from datetime import datetime
from math import ceil
from tag_process import tagging_ali_brands_preparation, tagging_ali_brands, tagging_items, tagging_material
from enums import DICT_EXCLUSIVES
# product
host = '192.168.1.120'
user = 'dev'
pwd = 'Dev_123123'


BASE_DIR = os.path.join(os.path.dirname(__file__), 'dicts')


def process_tag(industry, table_name):
    """
    打标签需要打两个不同的字段： 1. 品牌 2. 属性
    :param industry:
    :param table_name:
    :return:
    """
    # 词库文件列表，unix style pathname pattern
    # 词库来源： 坚果云/词库/行业/行业品牌词库(最新版)
    brand_list = BASE_DIR + u'/brand/'+industry+'/*.txt'  # 品牌词库

    # exclusive_list 互斥属性类型：以商品详情为准的标签类型
    exclusive_list = DICT_EXCLUSIVES[industry]

    print '{} Connecting DB{} ...'.format(datetime.now(), host)
    connect = MySQLdb.Connect(host=host, user=user, passwd=pwd, db=industry, charset='utf8')
    portal = MySQLdb.Connect(host=host, user=user, passwd=pwd, db='mp_portal', charset='utf8')

    # 选取商品数据
    # 商品在打完标签后会对NeedReTag做更新
    # 目前NeedReTag只对item表有效, sales表以后要加字段
    # 全更新去数据库更新NeedReTag字段
    item_info_query = """SELECT ItemID, CategoryID, concat_ws(' ',ItemSubTitle,ItemName) AS Title,
    ItemAttrDesc AS Attribute, concat_ws(' ',ShopName,ItemSubTitle,ItemName) AS ShopNameTitle
    FROM {0} WHERE NeedReTag='y';""".format(table_name)

    update_sql = """UPDATE {0}
    SET TaggedItemAttr=%s, NeedReTag='n', TaggedBrandName=%s, TaggedMaterial=%s
    WHERE ItemID=%s ;""".format(table_name)

    print '{} Loading data ...'.format(datetime.now())
    data = pd.read_sql_query(item_info_query, connect)

    n = len(data)
    if n > 0:
        print '{} Preprocess ...'.format(datetime.now())
        batch = 20000  # 20000的整数倍
        # 商店名去空格
        # 天猫前段输入框只支持空格和其他标点符号，不能输入\t \r \n
        data['ShopNameTitle'] = data['ShopNameTitle'].str.replace(' ', '')
        # 属性去空格
        data['Attribute'] = data['Attribute'].str.replace(' ', '')

        # 标签准备
        # brand_preparation 格式 {"正则表达式", "品牌"}
        brand_preparation = tagging_ali_brands_preparation(brand_list)

        # region 合并成一个Query
        # industry_id_query = "SELECT IndustryID FROM industry WHERE DBName='{}'".format(industry)
        # industry_id = pd.read_sql_query(industry_id_query, portal)['IndustryID'].values[0]
        #
        # tag_dicts_query = """SELECT CID, Attrname, DisplayName, AttrValue, Flag
        # FROM attr_value
        # WHERE IsTag='y'
        # AND
        # IndustryID={}""".format(industry_id)
        tag_dicts_query = """SELECT a.CID, a.Attrname, a.DisplayName, a.AttrValue, a.Flag
        FROM mp_portal.attr_value AS a
        JOIN mp_portal.industry AS i
        ON a.IndustryID = i.IndustryID
        WHERE a.IsTag='y'
        AND i.DBName ='{0}'
        """.format(industry)
        # endregion

        tag_dicts = pd.read_sql_query(tag_dicts_query, portal)

        # tag_preparation 打标签用和 brand_preparation类似
        tag_preparation = dict()

        # 每个类别的商品单独处理
        for cid in tag_dicts['CID'].unique():
            # {"CID": {("attr_name", "display_name", "attr_flag"): list}}
            # dict(dict[tuple](list))
            # 抓取的数据会有截尾逗号和空格
            # x[0] = CID
            # x[1] = Attrname
            # x[2] = DisplayName
            # x[3] = AttrValue
            # x[4] = Flag
            tag_preparation[int(cid)] = {(x[1], x[2], x[4]): x[3].rstrip(',').replace(' ', '').split(',')
                                         # type(x) = ndarray
                                         for x in tag_dicts[tag_dicts['CID'] == cid].values}

        print u'Total number of data: {}, batch_size = {}'.format(n, batch)

        # 切分数据
        # split_df = map(pd.DataFrame(), np.array_split(data.values, n/batch))
        split_df = [data.iloc[j * batch: min((j + 1) * batch, n)] for j in xrange(int(ceil(float(n) / batch)))]
        cursor = connect.cursor()
        for j in xrange(len(split_df)):
            print '{} Start batch {}'.format(datetime.now(), j + 1)
            batch_data = split_df[0]

            print '{} Tagging brands ...'.format(datetime.now())
            # Attribute 是商品详情，通过爬虫直接存好
            # 打标签的同时把品牌名加入到属性的json里
            # type(brand) = list
            brand = tagging_ali_brands(batch_data['Attribute'].values,
                                       batch_data['ShopNameTitle'].values,
                                       brand_preparation)

            print '{} Tagging features ...'.format(datetime.now())
            label = tagging_items(batch_data, tag_preparation, exclusive_list)
            # 面料标签
            material_label = tagging_material(batch_data, tag_preparation)
            item_ids = map(int, batch_data['ItemID'].values)

            update_items = zip(label, brand, material_label, item_ids)

            print u'{} Writing this batch to database ...'.format(datetime.now())
            cursor.executemany(update_sql, update_items)
            connect.commit()
            del split_df[0]

        connect.close()
        print u'{} Done!'.format(datetime.now())

    else:
        print u'Data in {} had been tagged!'.format(table_name)


if __name__ == '__main__':
    process_tag('mp_women_clothing', 'item_dev')
