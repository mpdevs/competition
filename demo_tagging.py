# -*- coding: utf-8 -*-

import MySQLdb
from tqdm import tqdm
from datetime import datetime
import pandas as pd
from tagging import tagging_ali_items, tagging_ali_brands


if __name__ == '__main__':

    ###########################################################################

    # 词库文件列表，unix style pathname pattern
    TAGLIST = u'dicts/feature/*.txt'# 标签词库
    BRANDSLIST = u'dicts/brand/*.txt'# 品牌词库

    # 互斥属性类型：以商品详情为准的标签类型
    EXCLUSIVES = [u'领型',u'面料',u'袖款',u'袖长',u'腰型',u'厚薄',u"扣型"]

    print '{} Connecting DB ...'.format(datetime.now())
    conn = MySQLdb.Connect(host='192.168.1.31', user='mpdev', passwd='mpdev@2016', db='mp_women_clothing', charset='utf8')
    # conn = MySQLdb.Connect(host='localhost', user='root', passwd='123', db='mp_women_clothing', charset='utf8')

    ###########################################################################

    # 选取数据,ItemID用于写回数据库对应的行,分行业打,因为要用不同的词库
    query = "SELECT ItemID,concat(ItemSubTitle,ItemName) as Title,ItemAttrDesc as Attribute,concat(ItemSubTitle,ItemName,ShopName) as ShopNameTitle FROM item WHERE NeedReTag='y';"

    print '{} Loading data ...'.format(datetime.now())
    data = pd.read_sql_query(query, conn)
    ID = data['ItemID']

    print '{} Start tagging brands ...'.format(datetime.now())
    data = tagging_ali_brands(data, BRANDSLIST)
    brand = data.iloc[:,4]# 存的是品牌
    #data.to_csv('output.csv', encoding='utf-8', index=False, float_format='%f')

    print '{} Start tagging feature ...'.format(datetime.now())
    data = tagging_ali_items(data, TAGLIST, EXCLUSIVES)
    label = data.iloc[:,5:]# 存的是0-1标签
    feature = label.columns# 存的是0-1标签对应的列名

    print u'{} 开始写入...'.format(datetime.now())
    cursor = conn.cursor()

    print u'共%d条数据 ...'%len(data.index)
    for i in tqdm(xrange(len(data.index))):

        sql = """UPDATE item SET TaggedItemAttr="%s",NeedReTag='n',TaggedBrandName="%s" WHERE ItemID=%d ; """%(','.join(feature[label.iloc[i].values==1]), brand[i], int(ID[i]))#更新0-1标签和品牌

        '''
        sql = """
            UPDATE item SET TaggedBrandName="%s" WHERE ItemID=%d ;
        """%(brand[i], int(ID[i]))#只更新品牌
        '''
        try:
            cursor.execute(sql)
        except Exception, e:
            print 'Get an error where Line = %d ItemID = %d'%(i,int(ID[i]))
        # if i/1000 == i/1000.0 and i>0:
        #     print u"已处理%d条..."%i

    conn.commit()
    conn.close()
    print u'{} 写入完成!'.format(datetime.now())


