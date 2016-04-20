# -*- coding: utf-8 -*-

import os
import MySQLdb
from tqdm import tqdm
from glob import glob
from datetime import datetime
import pandas as pd

from tag_process import tagging_ali_items, tagging_ali_brands

from weighted_jacca import getcut, WJacca
from helper import parser_label
from enums import EXCLUSIVES, IMPORTANT_ATTR_ENUM

from mp_preprocess.settings import host, user, pwd



BASE_DIR = os.path.join(os.path.dirname(__file__), 'dicts')



def process_tag(industry, table_name):

    # 词库文件列表，unix style pathname pattern
    TAGLIST =  BASE_DIR + u'/feature/*.txt'# 标签词库
    BRANDSLIST = BASE_DIR + u'/brand/*.txt'# 品牌词库

    # EXCLUSIVES 互斥属性类型：以商品详情为准的标签类型


    print '{} Connecting DB ...'.format(datetime.now())
    connect = MySQLdb.Connect(host=host, user=user, passwd=pwd, db=industry, charset='utf8')

    # 选取数据,ItemID用于写回数据库对应的行,分行业打,因为要用不同的词库
    query = """
            SELECT ItemID,concat(ItemSubTitle,ItemName) as Title,
            ItemAttrDesc as Attribute,concat(ItemSubTitle,ItemName,ShopName) as ShopNameTitle
            FROM %s WHERE NeedReTag='y';
            """%(table_name)

    print '{} Loading data ...'.format(datetime.now())
    data = pd.read_sql_query(query, connect)

    if len(data) > 0:

        print '{} Start tagging brands ...'.format(datetime.now())
        data = tagging_ali_brands(data, BRANDSLIST)
        brand = data.iloc[:,4]             # 存的是品牌


        print '{} Start tagging feature ...'.format(datetime.now())
        data = tagging_ali_items(data, TAGLIST, EXCLUSIVES)
        label = data.iloc[:,5:]            # 存的是0-1标签
        feature = label.columns            # 存的是0-1标签对应的列名

        print u'{} 开始写入...'.format(datetime.now())
        cursor = connect.cursor()

        print u'共%d条数据 ...'%len(data.index)
        ID = data['ItemID']
        for i in tqdm(xrange(len(data.index))):
            #更新0-1标签和品牌
            update_sql = """
                UPDATE %s SET TaggedItemAttr="%s",
                NeedReTag='n',TaggedBrandName="%s"
                WHERE ItemID=%d ;
                """%(table_name, ','.join(feature[label.iloc[i].values==1]), brand[i], int(ID[i]))
            try:
                cursor.execute(update_sql)
            except Exception, e:
                print 'Get an error where Line = %d ItemID = %d'%(i,int(ID[i]))


        connect.commit()
        connect.close()
        print u'{} 写入完成!'.format(datetime.now())

    else:
        print u'%s数据已全部打标签!'%table_name



def process_annual(industry):
    # print 'begin'
    # dd1 = datetime.now()
    # connect_industry = MySQLdb.Connect(host=host, user=user, passwd=pwd, db=industry, charset='utf8')
    # sql = """
    # select  * from  mp_women_clothing.itemmonthlysales_201501;
    # """
    # df = pd.read_sql_query(sql, connect_industry)
    # print len(df)
    # dd2= datetime.now()
    # print dd2-dd1
    # return




    #连接
    print '{} 正在连接数据库 ...'.format(datetime.now())
    connect_industry = MySQLdb.Connect(host=host, user=user, passwd=pwd, db=industry, charset='utf8')
    connect_portal = MySQLdb.Connect(host=host, user=user, passwd=pwd, db='mp_portal', charset='utf8')

    cursor_industry = connect_industry.cursor()
    cursor_portal = connect_portal.cursor()

    #词库文件,这个词库必须和打标签的词库是一个
    TAGLIST =  BASE_DIR + u'/feature/*.txt'# 标签词库

    #Category
    cursor_portal.execute('SELECT CategoryID,CategoryName,ParentID FROM category;')
    categories = cursor_portal.fetchall()

    dict_cid = [int(x[0]) for x in categories]
    dict_cname = [x[1] for x in categories]
    dict_par = [x[2] for x in categories]

    #定义重要维度
    important_attr = IMPORTANT_ATTR_ENUM[industry]
    dict_imp_name = important_attr['name']
    dict_imp_value = important_attr['value']

    #设定价格段上下浮动百分比
    setprecetage = 0.2

    #不同等级的属性权重划分
    pvalue = [0.6,0.4]

    #异位同位同类划分阈值
    jaccavalue = [0.56,0.66]

    #定义总共的二级维度列表
    fl = [u"感官", u"风格", u"做工工艺", u"厚薄", u"图案", u"扣型", u"版型", u"廓型", u"领型", u"袖型", u"腰型", u"衣长", u"袖长", u"衣门禁", u"穿着方式", u"组合形式", u"面料", u"颜色", u"毛线粗细", u"适用体型", u"裤型", u"裤长", u"裙型", u"裙长", u"fea", u"fun"]

    #读取
    head = [x[len(TAGLIST)-5:-4] for x in glob(TAGLIST)]
    dict_head = {}
    for i in xrange(len(head)):
        dict_head[head[i]] = i

    cursor_portal.execute('SELECT ShopID FROM shop;')
    shops = cursor_portal.fetchall()

    error_category = []
    s1, m1 = 0, 0
    s2, m2 = 0, 0
    s3, m3 = 0, 0
    s4, m4 = 0, 0

    insert_sql = """
        INSERT INTO itemrelation(SourceItemID,TargetItemID,RelationType,Status)
        VALUES
        ('%s','%s','%s','%s')
        """

    #开始寻找竞品
    for value in shops:

        if str(value[0]) != '60014692':
            print 'continue'
            continue
        print datetime.now(),u'正在计算ShopID=%d ...'%value

        d1 = datetime.now()
        cursor_industry.execute("SELECT TaggedItemAttr as label, ItemID as itemid, DiscountPrice as price, CategoryID FROM item WHERE ShopID=%d AND TaggedItemAttr IS NOT NULL AND TaggedItemAttr!='';"%value)
        items = cursor_industry.fetchall()
        d2 = datetime.now()
        s1 += (d2-d1).seconds
        m1 += (d2-d1).microseconds

        if len(items)==0: continue

        label = parser_label([x[0] for x in items], dict_head)
        itemid = [int(x[1]) for x in items]
        price = [float(x[2]) for x in items]
        CategoryID = [int(x[3]) for x in items]



        insert_items = []
        #对每个商品找竞品
        for i in tqdm(xrange(len(itemid))):

            if price[i] == 0: continue

            cid = CategoryID[i]

            #Find important dimension
            cidcid = cid
            flag = 1
            flagflag = 0
            while flag:
                try:
                    tempindex = dict_cid.index(cidcid)
                except:
                    error_category.append(cid)
                    flagflag = 1
                    break
                cname = dict_cname[tempindex]
                #print cname
                for j, x in enumerate(dict_imp_name):
                    if cname in x:
                        important = dict_imp_value[j]
                        flag = 0
                        break
                if flag == 1:
                    try:
                        cidcid = dict_cid.index(dict_par[tempindex])
                    except:
                        error_category.append(cid)
                        flagflag = 1
                        flag = 0




            if flagflag:
                continue


            #得到不重要的维度
            unimportant = list(set(fl) ^ set(important))
            cut = getcut(important, unimportant, head)

            minprice = price[i] * (1-setprecetage)
            maxprice = price[i] * (1+setprecetage)


            d5 = datetime.now()
            # #找到所有价格段内的同品类商品
            cursor_industry.execute("SELECT TaggedItemAttr as label, ItemID as itemid FROM mp_women_clothing.item WHERE ShopID!={} AND CategoryID={} AND DiscountPrice>{} AND DiscountPrice<{} AND TaggedItemAttr IS NOT NULL;".format(value[0], cid, minprice, maxprice))
            tododata = cursor_industry.fetchall()

            d6 = datetime.now()
            s3 += (d6-d5).seconds
            m3 += (d6-d5).microseconds


            if len(tododata)==0:continue


            todoid = [int(x[1]) for x in tododata]
            todolabel = parser_label([x[0] for x in tododata], dict_head)



            #计算相似度
            for j in xrange(len(todoid)):
                d3 = datetime.now()
                #print label.iloc[i],'-----------------------------------',todolabel.iloc[j]
                samilarity = WJacca(label[i], todolabel[j], cut, pvalue)
                if samilarity > jaccavalue[1]:
                    judge = 2
                elif samilarity < jaccavalue[0]:
                    judge = 0
                else:
                    judge = 1
                d4 = datetime.now()
                s2 += (d4-d3).seconds
                m2 += (d4-d3).microseconds
                #写入到数据库
                if judge>0:
                    insert_item = (itemid[i], todoid[j], judge, 1)
                    insert_items.append(insert_item)
                    # cursor_industry.execute("INSERT INTO itemrelation(SourceItemID,TargetItemID,RelationType,Status) VALUES('%d','%d','%d','1')"%(itemid[i], todoid[j], judge))

        if len(insert_items) > 0:
            d7 = datetime.now()
            cursor_industry.executemany(insert_sql, insert_items)
            connect_industry.commit()
            d8 = datetime.now()
            s4 += (d8-d7).seconds
            m4 += (d8-d7).microseconds
            print 'insert'+str(len(insert_items))





    # print u"以下品类不在重要属性字典中:"
    # for x in error_category:
    #     catid = dict_cid.index(x)
    #     catename = dict_cname[catid] if catid > -1 else None
    #     if catename:
    #         print catename
    connect_industry.close()
    print '找本店商品耗时{0},计算相似度耗时{1},找竞品耗时{2},入库耗时{3}'.format(str(s1+float(m1)/1000000), str(s2+float(m2)/1000000), str(s3+float(m3)/1000000), str(s4+float(m4)/1000000))
    print datetime.now()


def process_annual_test(industry):
    # print 'begin'
    # dd1 = datetime.now()
    # connect_industry = MySQLdb.Connect(host=host, user=user, passwd=pwd, db=industry, charset='utf8')
    # sql = """
    # select  * from  mp_women_clothing.itemmonthlysales_201501;
    # """
    # df = pd.read_sql_query(sql, connect_industry)
    # print len(df)
    # dd2= datetime.now()
    # print dd2-dd1
    # return




    #连接
    print '{} 正在连接数据库 ...'.format(datetime.now())
    connect_industry = MySQLdb.Connect(host=host, user=user, passwd=pwd, db=industry, charset='utf8')
    connect_portal = MySQLdb.Connect(host=host, user=user, passwd=pwd, db='mp_portal', charset='utf8')

    cursor_industry = connect_industry.cursor()
    cursor_portal = connect_portal.cursor()

    #词库文件,这个词库必须和打标签的词库是一个
    TAGLIST =  BASE_DIR + u'/feature/*.txt'# 标签词库

    #Category
    cursor_portal.execute('SELECT CategoryID,CategoryName,ParentID FROM category;')
    categories = cursor_portal.fetchall()

    dict_cid = [int(x[0]) for x in categories]
    dict_cname = [x[1] for x in categories]
    dict_par = [x[2] for x in categories]

    #定义重要维度
    important_attr = IMPORTANT_ATTR_ENUM[industry]
    dict_imp_name = important_attr['name']
    dict_imp_value = important_attr['value']

    #设定价格段上下浮动百分比
    setprecetage = 0.2

    #不同等级的属性权重划分
    pvalue = [0.6,0.4]

    #异位同位同类划分阈值
    jaccavalue = [0.56,0.66]

    #定义总共的二级维度列表
    fl = [u"感官", u"风格", u"做工工艺", u"厚薄", u"图案", u"扣型", u"版型", u"廓型", u"领型", u"袖型", u"腰型", u"衣长", u"袖长", u"衣门禁", u"穿着方式", u"组合形式", u"面料", u"颜色", u"毛线粗细", u"适用体型", u"裤型", u"裤长", u"裙型", u"裙长", u"fea", u"fun"]

    #读取
    head = [x[len(TAGLIST)-5:-4] for x in glob(TAGLIST)]
    dict_head = {}
    for i in xrange(len(head)):
        dict_head[head[i]] = i

    cursor_portal.execute('SELECT ShopID FROM shop;')
    shops = cursor_portal.fetchall()

    error_category = []
    s1, m1 = 0, 0
    s2, m2 = 0, 0
    s3, m3 = 0, 0
    s4, m4 = 0, 0

    insert_sql = """
        INSERT INTO itemrelation(SourceItemID,TargetItemID,RelationType,Status)
        VALUES
        ('%s','%s','%s','%s')
        """

    all_data = pd.read_sql_query("SELECT TaggedItemAttr as label, ItemID as itemid FROM mp_women_clothing.item WHERE ShopID!={} AND TaggedItemAttr IS NOT NULL;".format(value[0]), connect_industry)
    
    #开始寻找竞品
    for value in shops:

        if str(value[0]) != '60014692':
            print 'continue'
            continue
        print datetime.now(),u'正在计算ShopID=%d ...'%value

        d1 = datetime.now()
        cursor_industry.execute("SELECT TaggedItemAttr as label, ItemID as itemid, DiscountPrice as price, CategoryID FROM item WHERE ShopID=%d AND TaggedItemAttr IS NOT NULL AND TaggedItemAttr!='';"%value)
        items = cursor_industry.fetchall()
        d2 = datetime.now()
        s1 += (d2-d1).seconds
        m1 += (d2-d1).microseconds

        if len(items)==0: continue

        label = parser_label([x[0] for x in items], dict_head)
        itemid = [int(x[1]) for x in items]
        price = [float(x[2]) for x in items]
        CategoryID = [int(x[3]) for x in items]



        insert_items = []
        #对每个商品找竞品
        for i in tqdm(xrange(len(itemid))):

            if price[i] == 0: continue

            cid = CategoryID[i]

            #Find important dimension
            cidcid = cid
            flag = 1
            flagflag = 0
            while flag:
                try:
                    tempindex = dict_cid.index(cidcid)
                except:
                    error_category.append(cid)
                    flagflag = 1
                    break
                cname = dict_cname[tempindex]
                #print cname
                for j, x in enumerate(dict_imp_name):
                    if cname in x:
                        important = dict_imp_value[j]
                        flag = 0
                        break
                if flag == 1:
                    try:
                        cidcid = dict_cid.index(dict_par[tempindex])
                    except:
                        error_category.append(cid)
                        flagflag = 1
                        flag = 0




            if flagflag:
                continue


            #得到不重要的维度
            unimportant = list(set(fl) ^ set(important))
            cut = getcut(important, unimportant, head)

            minprice = price[i] * (1-setprecetage)
            maxprice = price[i] * (1+setprecetage)


            d5 = datetime.now()
            # #找到所有价格段内的同品类商品
            tododata = all_data[(all_data.DiscountPrice > minprice) & (all_data.DiscountPrice < maxprice) & (all_data.CategoryID == cid) ]

            d6 = datetime.now()
            s3 += (d6-d5).seconds
            m3 += (d6-d5).microseconds


            if len(tododata)==0:continue


            todoid = todata.itemid
            todolabel = parser_label(tododata.label, dict_head)



            #计算相似度
            for j in xrange(len(todoid)):
                d3 = datetime.now()
                #print label.iloc[i],'-----------------------------------',todolabel.iloc[j]
                samilarity = WJacca(label[i], todolabel[j], cut, pvalue)
                if samilarity > jaccavalue[1]:
                    judge = 2
                elif samilarity < jaccavalue[0]:
                    judge = 0
                else:
                    judge = 1
                d4 = datetime.now()
                s2 += (d4-d3).seconds
                m2 += (d4-d3).microseconds
                #写入到数据库
                if judge>0:
                    insert_item = (itemid[i], todoid[j], judge, 1)
                    insert_items.append(insert_item)
                    # cursor_industry.execute("INSERT INTO itemrelation(SourceItemID,TargetItemID,RelationType,Status) VALUES('%d','%d','%d','1')"%(itemid[i], todoid[j], judge))

        if len(insert_items) > 0:
            d7 = datetime.now()
            cursor_industry.executemany(insert_sql, insert_items)
            connect_industry.commit()
            d8 = datetime.now()
            s4 += (d8-d7).seconds
            m4 += (d8-d7).microseconds
            print 'insert'+str(len(insert_items))





    # print u"以下品类不在重要属性字典中:"
    # for x in error_category:
    #     catid = dict_cid.index(x)
    #     catename = dict_cname[catid] if catid > -1 else None
    #     if catename:
    #         print catename
    connect_industry.close()
    print '找本店商品耗时{0},计算相似度耗时{1},找竞品耗时{2},入库耗时{3}'.format(str(s1+float(m1)/1000000), str(s2+float(m2)/1000000), str(s3+float(m3)/1000000), str(s4+float(m4)/1000000))
    print datetime.now()
