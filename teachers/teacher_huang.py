# -*- coding: utf-8 -*-
import pandas as pd
from glob import glob
import numpy as np
from enums import DICT_FL


########################################################################################################################
# 在shell里执行python yyyyy.py > execution_yyyyy.log 会将所有这本程序的print结果输出到execution_yyyyy.log里,
# 下面是碰到的问题和解决办法
# python 在此操作系统上的默认字符集是 ascii 在向系统输出执行结果的时候会报错
# 所以在这里采取 reload(sys) + setdefaultencoding 的办法来输出文件
# 这是一个临时办法, reload(sys)会导致不可预估的问题
########################################################################################################################
import sys
reload(sys)
sys.setdefaultencoding(u"utf-8")


class Process(object):
    """
    The methods who start with a capital will change the attribute, so their reference is in __init__ or themselves. 
    """
    def __init__(self, industry):
        self.data = dict()    # {ilabel: dfi}
        self.ps = set()       # persons
        self.industry = industry
        # columns = ["id1", "id2", "attr1", "attr2", "tag1", "tag2"]
        self.col = [u"id1", u"id2"]    # common columns of DataFrames
        self.filtered = None  # merge the data in a df based on consistent label
        self.sim = None       # the similarity of each pair of products in every dim
        
    def load(self, paths):
        """
        load data from no-title $FileNum_PersonName.txt splited by "\t".
        In $FileNum_PersonName.txt : id1, id2, attr1, attr2, tag1, tag2, label
        """
        for path in glob(paths):
            num, person = tuple(path.split(u"/")[-1][:-4].split(u"_"))[:2]
            person = person.replace(u" ", u"")
            self.ps.add(person)
            with open(path) as f:
                info = [line.strip(u"\n\r").replace(u" ", u"").decode(u"utf8").split(u"\t") for line in f]
            
            t = pd.DataFrame(info, columns=self.col+[person])
            self.data[num] = pd.merge(self.data[num], t, on=self.col) if num in self.data.keys() else t
        self.__digitizing_data()
    
    def __digitizing_data(self):
        """
        Digitizate data (binary label and swift id).
        """
        import copy
        data = copy.deepcopy(self.data)
        for i in data.iterkeys():
            data[i][u"id1"] = data[i][u"id1"].map(float)
            data[i][u"id2"] = data[i][u"id2"].map(float)
            for p in self.ps.intersection(set(data[i].columns)): 
                # 0 出错(图片丢失)
                # 1 非常相似
                # 2 相似
                # 3 不相似
                data[i][p].replace([u"1", u"2"], 1, inplace=True)
                data[i][p].replace([u"3", u"0"], 0, inplace=True)
        self.data = data
                      
    def consistency(self):    
        """
        Print consistency of every person.
        """
        cy = dict().fromkeys(self.ps)
        for i, j in self.data.iteritems():
            ps = list(self.ps.intersection(set(j.columns)))
            if len(ps) == 2:
                p1, p2 = ps[0], ps[1]
                x = [(sum(j[p1] == j[p2]), len(j))]
                cy[p1] = cy[p1] + x if cy[p1] else x 
                cy[p2] = cy[p2] + x if cy[p2] else x 
            else:
                t = [1 if j.loc[index, ps].values.tolist().count(1) > len(ps) / 2.0 else 0 for index in j.index]
                for p in ps:                    
                    x = [(sum(t == j[p]), len(j))]
                    cy[p] = cy[p] + x if cy[p] else x
                
        print u"一致性"    
        for i in cy.iterkeys():
            num, n = 0.0, 0
            for x, y in cy[i]:
                num += x
                n += y
            print i, num / n if n != 0 else 0
        print u"\n"
    
    def filter(self):
        """
        Data label must be binary!
        """
        self.filtered = pd.DataFrame(columns=self.col + [u"Std"])
        for i, j in self.data.iteritems():

            ps = list(self.ps.intersection(set(j.columns)))
            t = j[self.col]
            if len(ps) == 2:
                t = j[j[ps[0]] == j[ps[1]]].rename(columns={ps[0]: u"Std"})
                del t[ps[1]]
            else:
                t = j[self.col]               
                t[u"Std"] = pd.Series([sum(j.loc[index, ps] == 1) / float(len(ps)) for index in j.index])
                            
            self.filtered = pd.concat([self.filtered, t])
        
        self.filtered.index = range(len(self.filtered))      
        # print len(self.filtered)
        # print sum(self.filtered["Std"] == 1)
        # print sum(self.filtered["Std"] == 1) / float(len(self.filtered))
        # print u"正例:负例 {}:{}".format(sum(self.filtered["Std"] == 1), sum(self.filtered["Std"] == 0) )
    
    # Calculate feature matrix                    
    def sim(self):
        import MySQLdb
        from helper import getcut, parser_label, Jaca
        # 取数据库存在的标签json和本地的txt标签文件做比较
        connect_industry = MySQLdb.Connect(host=u"192.168.1.120", user=u"dev", passwd=u"Dev_123123", db=self.industry, charset=u"utf8")
        FL = DICT_FL[self.industry]
        # 获取的是当前文件夹下某行业的文件夹下的所有文件名
        HEAD = [x.split(u"/")[-1][:-4].replace(u" ", u"") for x in glob(self.industry + u"/*.txt")]
        dict_head = {HEAD[i]: i for i in xrange(len(HEAD))}
        cut = getcut([FL], HEAD)[0]
        # print cut,raw_input()
        all_data = pd.read_sql(u"select ItemID as itemid, TaggedItemAttr as label "
                               u"from TaggedItemAttr where TaggedItemAttr is not NULL", connect_industry)
        # print "%%%%%%%%%%%",len(all_data)
        tag1 = []
        tag2 = []
        tag = []
        ID1 = []
        ID2 = []
        for i in xrange(len(self.filtered)):
            id1 = self.filtered[u"id1"][i]
            id2 = self.filtered[u"id2"][i]
            try:
                # find id1"s tag
                tag1.append(all_data[u"label"][all_data[u"itemid"] == int(id1)].values[0])
                # find id2"s tag
                tag2.append(all_data[u"label"][all_data[u"itemid"] == int(id2)].values[0])
                # label
                tag.append(self.filtered[u"Std"][i])
                ID1.append(id1)
                ID2.append(id2)
            except:
                continue
        
        v1 = parser_label(tag1, dict_head)
        v2 = parser_label(tag2, dict_head)
        sim = [[ID1[i], ID2[i], tag[i]] + [Jaca(v1[i][c], v2[i][c]) for c in cut] for i in xrange(len(tag))]
        # print len(ID1),len(ID2),len(v1),len(v2),len(tag)
        sb = u""
        for i in dict_head:
            sb += i+u"\t"
        # print sb[:-1]
        for i in range(len(tag)):
            print ID1[i], u"\t", ID2[i], u"\t", tag1[i], u"\t", tag2[i], u"\t", tag[i]
        self.sim = pd.DataFrame(sim, columns=[u"id1", u"id2", u"Std"] + FL)

    def regression(self):
        if self.sim is None:
            self.sim()
        from sklearn import cross_validation
        # from sklearn.linear_model import Ridge, Lasso, ElasticNet
        from sklearn.ensemble import GradientBoostingRegressor as gbrt  # , GradientBoostingClassifier as gbdt
        from sklearn.metrics import classification_report
        # from sklearn.svm import SVC
        
        FL = DICT_FL[self.industry]
        
        X = self.sim[FL].values
        y = self.sim[u"Std"].values
        
        print u"总量" + u"\t" + u"一致同位数量"
        print str(np.shape(y)) + u"\t" + str(len(y[y > 0.5]))
        
        import random
        # 均衡各类数量
        t = random.sample(np.array(range(len(y)))[y < 0.5], sum(y > 0.5)) + np.array(range(len(y)))[y > 0.5].tolist()
        t = random.sample(t, len(t))
        X = X[t]
        y = y[t]
               
        X_train, X_test, y_train, y_test = cross_validation.train_test_split(X, y, test_size=0.5, random_state=42)
        # print "type(y) %s" % type(y) # type 是 ndarray
        # print np.shape(y) # y是1维向量
        
        model = gbrt()   
        model.fit(X_train, y_train)  # gbrt fit
        print u"GBRT, mse", np.sqrt(sum((model.predict(X_test) - y_test) ** 2) / len(y_test))
        print u"GBRT, precision, recall"
        print classification_report([1 if i > 0.5 else 0 for i in y_test],
                                    [1 if i > 0.5 else 0 for i in model.predict(X_test)])
        
        """
        model = Ridge(alpha=0.005) 
        model.fit(X_train, y_train)  #Ridge fit
        print "Ridge alpha=0.005, mse", np.sqrt(sum((model.predict(X_test) - y_test) ** 2) / len(y_test))
        print "Ridge alpha=0.005, precision, recall"
        print classification_report([1 if i > 0.5 else 0 for i in y_test], [1 if i > 0.5 else 0 for i in model.predict(X_test)])
        
        model = Ridge(alpha=0.005, copy_X=True) 
        model.fit(X_train, y_train)  #Ridge fit
        print "Ridge alpha=0.005, mse", np.sqrt(sum((model.predict(X_test) - y_test) ** 2) / len(y_test))
        print "Ridge alpha=0.005, precision, recall"
        print classification_report([1 if i > 0.5 else 0 for i in y_test], [1 if i > 0.5 else 0 for i in model.predict(X_test)])
        
        model = Lasso(alpha=0.005, copy_X=True) 
        model.fit(X_train, [1 if t > 0.5 else 0 for t in y_train]) # lasso fit
        print "Lasso alpha=0.005, mse", np.sqrt(sum((model.predict(X_test) - y_test) ** 2) / len(y_test))
        print "Lasso alpha=0.005, precision, recall"
        print classification_report([1 if i > 0.5 else 0 for i in y_test], [1 if i > 0.5 else 0 for i in model.predict(X_test)])

        model = SVC(kernel="poly")
        model.fit(X_train, [1 if t == 1 else 0 for t in y_train]) # svc fit
        print "SVC kernel=poly, mse", np.sqrt(sum((model.predict(X_test) - y_test) ** 2) / len(y_test))
        print "SVC kernel=poly, precision, recall"
        print classification_report([1 if i > 0.5 else 0 for i in y_test], [1 if i > 0.5 else 0 for i in model.predict(X_test)])
                   
        model = gbdt()   # gbdt fit
        model.fit(X_train, [1 if t == 1 else 0 for t in y_train])
        print "gbdt, mse", np.sqrt(sum((model.predict(X_test) - y_test) ** 2) / len(y_test))
        print "gbdt, precision, recall"
        print classification_report([1 if i > 0.5 else 0 for i in y_test], [1 if i > 0.5 else 0 for i in model.predict(X_test)])
        """
   
        

    """
    # use feature matrix and std (as label) to train models
    # should add 10-fold cross validation
    # models: ridge and gbdt
    # precision and recall are used as evaluation matrics
    def classification(self):
        if self.sim is None:
            self.sim()
        from sklearn import cross_validation
        from sklearn.svm import SVC
        from sklearn.linear_model import Ridge, Lasso, ElasticNet
        from sklearn.metrics import classification_report
        from sklearn.ensemble import GradientBoostingClassifier as gbdt
        FL = DICT_FL[self.industry]

        X = self.sim[FL].values
        y = [[1, 0] if i else [0, 1] for i in self.sim["Std"].values]
        
        print "all data count",len(X)
        print "positive percent",y.count([1,0])*1.0/len(X)
        pos=y.count([1,0])
        cnt=0
        # output feature and label        
        f=file("a.txt","w")
        f.write(str(zip(X.tolist(),self.sim["Std"].values.tolist())))
        f.close()
        print type(self.sim["Std"].values)#,raw_input()
              
        X_train, X_test, y_train, y_test = cross_validation.train_test_split(X, y, test_size=0.5, random_state=42)
        #print y_train,raw_input()
        if True:
            if True:
                print "Ridge"
                model = Ridge(alpha=0.005, copy_X=True)
                model.fit(X_train, y_train)
                #print model.predict(X_test)
                y_ = [[1, 0] if t[0] > t[1] else [0, 1] for t in model.predict(X_test)] 
                print classification_report([1 if x[0] == 1 else 0 for x in y_test], [1 if x[0] == 1 else 0 for x in y_])
                
                #y_ = [1 if t>0.4 else 0 for t in model.predict(X_test)]
                #y_=model.predict(X_test)
                #print classification_report(y_test,y_).replace("   "," ").replace("  "," ").replace("  "," ").replace("  "," ").replace(" ","\t").replace("\n\n","\n").replace("\n\t","\n").replace("avg	/	total","avg/total")
                
                #print [i for i in zip(list(y_),y_test)]
                                              
                print "Lasso"
                model = Lasso(alpha=0.005, copy_X=True)
                model.fit(X_train, [1 if t[0] == 1 else 0 for t in y_train])
                y_ = [1 if t > 0.5 else 0 for t in model.predict(X_test)] 
                print classification_report([1 if x[0] == 1 else 0 for x in y_test], y_).replace("   "," ").replace("  "," ").replace("  "," ").replace("  "," ").replace(" ","\t").replace("\n\n","\n").replace("\n\t","\n").replace("avg	/	total","avg/total")
                
                print "ElasticNet"
                model = ElasticNet(alpha=0.005, l1_ratio=0.2, copy_X=True)
                model.fit(X_train, [1 if t[0] == 1 else 0 for t in y_train])
                y_ = [1 if t > 0.5 else 0 for t in model.predict(X_test)] 
                print classification_report([1 if x[0] == 1 else 0 for x in y_test], y_).replace("   "," ").replace("  "," ").replace("  "," ").replace("  "," ").replace(" ","\t").replace("\n\n","\n").replace("\n\t","\n").replace("avg	/	total","avg/total")
                
                print "SVM"
                model = SVC(kernel="poly")
                model.fit(X_train, [1 if t[0] == 1 else 0 for t in y_train])
                y_ = model.predict(X_test)
                print classification_report([1 if x[0] == 1 else 0 for x in y_test], y_).replace("   "," ").replace("  "," ").replace("  "," ").replace("  "," ").replace(" ","\t").replace("\n\n","\n").replace("\n\t","\n").replace("avg	/	total","avg/total")
                
                print "GBDT"               
                model = gbdt()   
                model.fit(X_train, [1 if x[0] == 1 else 0 for x in y_train])
                y_ = model.predict(X_test)
                print classification_report([1 if x[0] == 1 else 0 for x in y_test], y_).replace("   "," ").replace("  "," ").replace("  "," ").replace("  "," ").replace(" ","\t").replace("\n\n","\n").replace("\n\t","\n").replace("avg	/	total","avg/total")
        """              


if __name__ == u"__main__":
    from os import path
    base_path = path.join(path.dirname(__file__), u"biaozhu")
    for path in glob(u"{0}/女装/*".format(base_path)):
        print u"path = {0}".format(path)
        print u"###\t", path.split(u"/")[-1]
        file_path = path + u"/*.txt"
        p = Process(u"mp_women_clothing")
        p.load(file_path)
        p.filter()
        # p.consistency()
        p.sim()
        # p.regression()
        np.savetxt(p+u".txt", p.filtered[[u"id1", u"id2", u"Std"]].values, delimiter=u"\t")
