# -*- coding: utf-8 -*-

import MySQLdb
from datetime import datetime
import pandas as pd
from tagging import tagging_ali_items


if __name__ == '__main__':
    
    ###########################################################################

    # 词库文件列表，unix style pathname pattern
    TAGLIST = u'dicts/women/*'

    # 互斥属性类型：以商品详情为准的标签类型 
    EXCLUSIVES = [u'领型',u'面料',u'袖款',u'袖长',u'腰型',u'厚薄',u"扣型"]
    
    print '{} Connecting DB ...'.format(datetime.now()) 
    conn = MySQLdb.Connect(host='192.168.1.31', user='mpdev', passwd='mpdev@2016', db='mp_women_clothing', charset='utf8')
    
    ###########################################################################

    query = "SELECT ItemID,concat(ItemSubTitle,ItemName) as Title,ItemAttrDesc as Attribute FROM item WHERE NeedReTag='y';"  
    
    print '{} Loading data ...'.format(datetime.now())     
    data = pd.read_sql_query(query, conn)
    
    print '{} Start tagging ...'.format(datetime.now())
    data = tagging_ali_items(data, TAGLIST, EXCLUSIVES)
    
    label = data.iloc[:,4:]
    ID = data['ItemID']
    feature = label.columns
    
    print u'{} 开始写入...'.format(datetime.now())
    cursor = conn.cursor()
       
    n = len(data.index)
    print "共%d条数据"%n
    for i in xrange(n):
        #x = "UPDATE item SET TaggedItemAttr='%s' "%(label.iloc[i].to_json(force_ascii=False))
        
        sql = "UPDATE item SET TaggedItemAttr='%s',NeedReTag='n' WHERE ItemID=%s ; ".format(','.join([feature[j] for j in xrange(len(feature)) if label.iloc[i].values[j]==1]), str(int(ID[i])))
        cursor.execute(sql)  
        
        if i/1000.0 == i/1000 and i>0: 
            print u"已写入%d条..."%i


    conn.commit()
    conn.close()
    print u'{} 写入完成!'.format(datetime.now())
       
    


