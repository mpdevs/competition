# -*- coding: utf-8 -*-

from glob import glob
import pandas as pd
import numpy as np
import MySQLdb
from tqdm import tqdm
from Weighted_Jacca import getcut, WJacca
from datetime import datetime


#连接
print '{} 正在连接数据库 ...'.format(datetime.now())
conn1 = MySQLdb.Connect(host='192.168.1.31', user='mpdev', passwd='mpdev@2016', db='mp_women_clothing', charset='utf8')
conn2 = MySQLdb.Connect(host='192.168.1.31', user='mpdev', passwd='mpdev@2016', db='mp_portal', charset='utf8')

# conn1 = MySQLdb.Connect(host='localhost', user='root', passwd='123', db='mp_women_clothing', charset='utf8')
# conn2 = MySQLdb.Connect(host='localhost', user='root', passwd='123', db='mp_portal', charset='utf8')
cur1 = conn1.cursor()
cur2 = conn2.cursor()


#词库文件,这个词库必须和打标签的词库是一个
TAGLIST = u'dicts/feature/*.txt'

#定义重要维度
#羽绒服/棉衣
#important = [u"版型",u"廓形",u"衣门襟",u"图案",u"衣长",u"袖型",u"腰型",u"fun"]
#休闲裤/西裤/打底裤
#important = [u"版型", u"裤型", u"面料", u"裤长", u"腰型"]
#牛仔裤
#important = [u"版型", u"裤型", u"面料", u"裤长", u"腰型", u"做工工艺"]
#风衣/毛呢外套/短外套/皮草/西装/皮衣/西装套装
#important = [u"版型",u"廓形",u"面料",u"图案",u"衣长",u"袖型",u"腰型"]
#T袖/长袖/衬衫/吊带背心/雪纺衫/POPLO衫
#important = [u"版型",u"廓形",u"面料",u"图案",u"风格",u"颜色"]
#(绒衫/卫衣)/毛衣/针织衫
#important = [u"版型",u"廓形",u"面料",u"图案",u"袖型",u"风格",u"颜色"]
#短裙/连衣裙/长裙/半身裙
important = [u"版型",u"廓形",u"面料",u"图案",u"裙型",u"袖型"]
#设定价格段上下浮动百分比
setprecetage = 0.2

#不同等级的属性权重划分
pvalue = [0.6,0.4]

#异位同位同类划分阈值
jaccavalue = [0.56,0.66]

#定义总共的二级维度列表
fl = [u"感官", u"风格", u"做工工艺", u"厚薄", u"图案", u"扣型", u"版型", u"廓型", u"领型", u"袖型", u"腰型", u"衣长", u"袖长", u"衣门禁", u"穿着方式", u"组合形式", u"面料", u"颜色", u"毛线粗细", u"适用体型", u"裤型", u"裤长", u"裙型", u"裙长", u"fea", u"fun"]

#限制
lim = ' WHERE TaggedItemAttr IS NOT NULL AND ShopID IS NOT NULL ;'

#查询
query = 'SELECT TaggedItemAttr as label, ItemID as itemid, MonthlyOrders as amount, DiscountPrice as price, ShopID as others FROM mp_women_clothing.item'
query_match = 'SELECT ShopID FROM mp_portal.shop;'

#得到不重要的维度
unimportant = list(set(fl) ^ set(important))

#读取
print '{} Loading data ...'.format(datetime.now())
head = [x[len(TAGLIST)-5:-4] for x in glob(TAGLIST)]
cur1.execute(query+lim)
data = cur1.fetchall()
cur2.execute(query_match)
match = cur2.fetchall()

label = [x[0] for x in data]
itemid = [int(x[1]) for x in data]
amount = np.array([int(x[2]) for x in data])
price = np.array([int(x[3]) for x in data])
others = [int(x[4]) for x in data]
match = list(match)
print "共有数据%d条"%len(label)

#解析0-1矩阵
print "{} 正在解析标签 ...".format(datetime.now())
df_label = pd.DataFrame(np.zeros((len(label), len(head))))
df_label.columns = head
for i in range(len(label)):
    #df_label[label[i].split(',')][i] = 1#maybe a good change
    for x in label[i].split(','):
        if x:
            df_label[x][i] = 1

#处理0-1矩阵
sparsem = df_label.values
temp = sum(sparsem)>5
featurelist = []
for i,x in enumerate(df_label.columns):
    if temp[i]: featurelist.append(x)
sparsem = sparsem[:,temp]#删除出现特征少的列

#计算销售额
sales = amount*price

#计算cut为计算相似度做准备
cut = getcut(important, unimportant, featurelist)

print "{} 开始寻找竞品 ...".format(datetime.now())
#对目标循环
for value in match:
    #找到目标所在的行
    aimindex = []
    for i,x in enumerate(others):
        if x == int(value[0]):
            aimindex.append(i)
    print u'正在计算ShopID=%d ...'%value

    if len(aimindex) == 0:
        print u'ShopID=%d 暂无竞品...'%value
        continue
    #对每个商品找竞品

    for i in tqdm(aimindex):
        if price[i]==0: continue
        minprice = price[i] * (1-setprecetage)
        maxprice = price[i] * (1+setprecetage)

        #找到所有价格段内的商品
        todoindex = []
        for j,y in enumerate(price):
            if y < maxprice and y > minprice and j!=i:
                todoindex.append(j)

        #计算相似度
        for j in todoindex:
            samilarity = WJacca(sparsem[i], sparsem[j], cut, pvalue)
            if samilarity > jaccavalue[1]:
                judge = 2
            elif samilarity < jaccavalue[0]:
                judge = 0
            else:
                judge = 1
            #写入到数据库
            if judge>0:
                cur1.execute("INSERT INTO itemrelation(SourceItemID,TargetItemID,RelationType,Status) VALUES('%d','%d','%d','1')"%(itemid[i], itemid[j], judge))
    conn1.commit()
conn1.close()
print datetime.now()




