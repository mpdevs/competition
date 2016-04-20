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

    #设定价格段上下浮动百分比;不同等级的属性权重划分;异位同位同类划分阈值
    setprecetage, pvalue, jaccavalue = 0.2,  [0.6,0.4], [0.56,0.66]


    #定义总共的二级维度列表
    fl = [u"感官", u"风格", u"做工工艺", u"厚薄", u"图案", u"扣型", u"版型", u"廓型", u"领型", u"袖型", u"腰型", u"衣长", u"袖长", u"衣门禁", u"穿着方式", u"组合形式", u"面料", u"颜色", u"毛线粗细", u"适用体型", u"裤型", u"裤长", u"裙型", u"裙长", u"fea", u"fun"]

    #读取
    head = [x[len(TAGLIST)-5:-4] for x in glob(TAGLIST)]
    dict_head = {}
    for i in xrange(len(head)):
        dict_head[head[i]] = i

    error_category = []

    insert_sql = """
        INSERT INTO itemrelation(SourceItemID,TargetItemID,RelationType,Status)
        VALUES
        ('%s',%s,'%s','%s')
        """

    cursor_portal.execute('SELECT ShopID FROM shop;')
    shops = cursor_portal.fetchall()

    all_data = pd.read_sql_query("SELECT TaggedItemAttr as label, ItemID as itemid, ShopId as shopid,DiscountPrice,CategoryID FROM mp_women_clothing.item WHERE  TaggedItemAttr IS NOT NULL;", connect_industry)
    
    #开始寻找竞品
    for value in shops:


        print datetime.now(),u'正在计算ShopID=%d ...'%value

        cursor_industry.execute("SELECT TaggedItemAttr as label, ItemID as itemid, DiscountPrice as price, CategoryID FROM item WHERE ShopID=%d AND TaggedItemAttr IS NOT NULL AND TaggedItemAttr!='';"%value)
        items = cursor_industry.fetchall()

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

            # #找到所有价格段内的同品类商品
            tododata = all_data[(all_data.DiscountPrice > minprice) & (all_data.DiscountPrice < maxprice) & (all_data.CategoryID == cid) & (all_data.shopid != value[0]) ]


            if len(tododata)==0:continue

            todoid = tododata['itemid']
            todolabel = parser_label(list(tododata['label']), dict_head)

            #计算相似度
            for j in xrange(len(todoid)):
                samilarity = WJacca(label[i], todolabel[j], cut, pvalue)
                if samilarity > jaccavalue[1]: judge = 2
                elif samilarity < jaccavalue[0]:judge = 0
                else: judge = 1

                if judge>0:
                    insert_item = (itemid[i], todoid.iloc[j], judge, 1)
                    insert_items.append(insert_item)

        if len(insert_items) > 0:
            cursor_industry.executemany(insert_sql, insert_items)
            connect_industry.commit()
            print 'insert'+str(len(insert_items))


    connect_industry.close()
    print datetime.now()
