# -*- coding: utf-8 -*-

from glob import glob
import MySQLdb
from tqdm import tqdm
from Weighted_Jacca import getcut,WJacca
from datetime import datetime
from parser import parser_label


#连接
print '{} 正在连接数据库 ...'.format(datetime.now()) 
conn1 = MySQLdb.Connect(host='192.168.1.31', user='mpdev', passwd='mpdev@2016', db='mp_women_clothing', charset='utf8')
conn2 = MySQLdb.Connect(host='192.168.1.31', user='mpdev', passwd='mpdev@2016', db='mp_portal', charset='utf8')
cur1 = conn1.cursor()
cur2 = conn2.cursor()

#词库文件,这个词库必须和打标签的词库是一个
TAGLIST = u'dicts/feature/*.txt'

#Category
cur2.execute('SELECT CategoryID,CategoryName,ParentID FROM category;')
temp = cur2.fetchall()
dict_cid = [int(x[0]) for x in temp]
dict_cname = [x[1] for x in temp]
dict_par = [x[2] for x in temp]

#定义重要维度
dict_imp_name = [[u"羽绒服",u"棉衣/棉服"],[u"休闲裤",u"西装裤/正装裤",u"打底裤"],[u"牛仔裤"],[u"风衣",u"毛呢外套",u"短外套",u"皮草",u"西装",u"皮衣",u"西装套装",u"时尚套装"],[u"T恤",u"长袖",u"衬衫",u"背心吊带",u"蕾丝衫/雪纺衫",u"POLO衫",u"马夹"],[u"卫衣/绒衫",u"毛衣",u"毛针织衫"],[u"短裙",u"连衣裙",u"长裙",u"半身裙"]]
dict_imp_valu = [[u"版型",u"廓形",u"衣门襟",u"图案",u"衣长",u"袖型",u"腰型",u"fun"],[u"版型", u"裤型", u"面料", u"裤长", u"腰型"],[u"版型", u"裤型", u"面料", u"裤长", u"腰型", u"做工工艺"],[u"版型",u"廓形",u"面料",u"图案",u"衣长",u"袖型",u"腰型"],[u"版型",u"廓形",u"面料",u"图案",u"风格",u"颜色"],[u"版型",u"廓形",u"面料",u"图案",u"袖型",u"风格",u"颜色"],[u"版型",u"廓形",u"面料",u"图案",u"裙型",u"袖型"]]

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
cur2.execute('SELECT ShopID FROM mp_portal.shop;')
match = cur2.fetchall()

errorcategory = []

#开始寻找竞品
for value in match:
    print datetime.now(),u'正在计算ShopID=%d ...'%value

    cur1.execute("SELECT TaggedItemAttr as label, ItemID as itemid, DiscountPrice as price, CategoryID FROM mp_women_clothing.item WHERE ShopID=%d AND TaggedItemAttr IS NOT NULL AND TaggedItemAttr!='';"%value)
    temp = cur1.fetchall()
    if len(temp)==0: continue
    label = parser_label([x[0] for x in temp], head)
    itemid = [int(x[1]) for x in temp]
    price = [float(x[2]) for x in temp]
    CategoryID = [int(x[3]) for x in temp]
    
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
                errorcategory.append(cid)
                flagflag = 1
                break
            cname = dict_cname[tempindex]
            #print cname
            for j, x in enumerate(dict_imp_name):
                if cname in x:
                    important = dict_imp_valu[j]
                    flag = 0
                    break
            if flag == 1:
                try:
                    cidcid = dict_cname[dict_cid.index(dict_par[tempindex])]      
                except:
                    errorcategory.append(cid)
                    flagflag = 1
                    flag = 0
                    
        if flagflag:
            continue               
        #得到不重要的维度
        unimportant = list(set(fl) ^ set(important))       
        cut = getcut(important, unimportant, head)
        
        minprice = price[i] * (1-setprecetage)
        maxprice = price[i] * (1+setprecetage)
        
        #找到所有价格段内的同品类商品
        cur1.execute("SELECT TaggedItemAttr as label, ItemID as itemid FROM mp_women_clothing.item WHERE ShopID!={} AND CategoryID={} AND DiscountPrice>{} AND DiscountPrice<{} AND TaggedItemAttr IS NOT NULL;".format(value[0], cid, minprice, maxprice))
        tododata = cur1.fetchall()
        if len(tododata)==0:continue
        todoid = [int(x[1]) for x in tododata]
        todolabel = parser_label([x[0] for x in tododata], head)
        
        #计算相似度
        for j in xrange(len(todoid)):
            #print label.iloc[i],'-----------------------------------',todolabel.iloc[j]
            samilarity = WJacca(label.iloc[i], todolabel.iloc[j], cut, pvalue)
            if samilarity > jaccavalue[1]:
                judge = 2
            elif samilarity < jaccavalue[0]:
                judge = 0
            else:
                judge = 1
            #写入到数据库
            if judge>0:
                cur1.execute("INSERT INTO itemrelation(SourceItemID,TargetItemID,RelationType,Status) VALUES('%d','%d','%d','1')"%(itemid[i], todoid[j], judge))
        conn1.commit()           

print u"以下品类不在重要属性字典中:"
for x in errorcategory:
    print dict_cname[dict_cid.index(x)]
conn1.commit()
conn1.close()
print datetime.now()

