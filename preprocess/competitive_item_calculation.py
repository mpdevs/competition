# -*- coding: utf-8 -*-
"""
基于训练模型
"""
from collections import OrderedDict
from datetime import datetime
from sklearn.ensemble import GradientBoostingRegressor
from helper import tag_to_dict, construct_train_feature, sample_balance, construct_prediction_feature
from db_apis import get_categories, get_attribute_meta, get_training_data, get_customer_shop_items
from db_apis import get_competitor_shop_items, set_scores, delete_score, get_max_date_range


class CalculateCompetitiveItems(object):
    # region 内部所有属性的初始化
    def __init__(self, industry="mp_women_clothing", source_table="itemmonthlysales2015",
                 target_table="itemmonthlyrelation_2015", shop_id=66098091, cid=1623):
        print (u'{0} 正在连接数据库 ...'.format(datetime.now()))
        # region 必要
        self.industry = industry
        self.source_table = source_table
        self.target_table = target_table
        self.customer_shop_id = shop_id
        self.date_range = get_max_date_range(db=self.industry, table=self.source_table)
        print (u"{0} 正在获取品类信息... ".format(datetime.now()))
        self.cid_cname = {int(row[0]): row[1] for row in get_categories()}
        self.cid = cid
        # 用来将商品标签向量化
        print (u"{0} 正在抽取标签元数据... ".format(datetime.now()))
        self.attribute_meta = get_attribute_meta(db=self.industry)
        # self.items_attributes = None
        # key=CategoryID value=CategoryName
        # CID: CNAME
        print (u"{0} 正在生成商品的标签dict... ".format(datetime.now()))
        self.tag_dict = tag_to_dict(df=self.attribute_meta[["CID", "Attrname", "AttrValue"]])
        self.training_data = None
        self.train_x = None
        self.train_y = None
        # key=category value=model
        self.model = OrderedDict()
        self.predict_x = None
        self.predict_y = None
        self.message = None
        # TaggedItemAttr, item_id, shop_id DataFrame
        print (u"{0} 正在获取客户商品信息... ".format(datetime.now()))
        self.customer_shop_items = get_customer_shop_items(
            db=self.industry, table=self.source_table, shop_id=self.customer_shop_id, date_range=self.date_range)
        self.competitor_items = None
        self.item_pairs = None
        self.data_to_db = None
        # endregion
        # region 可选
        # 必要维度法
        self.essential_tag_dict = None
        self.statistic_info = []
        # endregion

    # endregion

    # region print实例的值
    def __str__(self):
        """
        调试程序
        :return:
        """
        string = u"""industry={0}\ncustomer_shop_id={1}\nCategoryName={2}\n
        """.format(self.industry, self.customer_shop_id, self.cid_cname[self.cid])
        return string
    # endregion

    # region 特征构造
    def build_train_feature(self):
        """
        按需训练
        :return:
        """
        print (u"{0} 正在获取训练数据... ".format(datetime.now()))
        self.training_data = get_training_data(cid=self.cid)
        # 构造训练数据
        # training_data: attr1, attr2, score
        print (u"{0} 正在构造训练数据的特征矩阵... ".format(datetime.now()))
        self.train_x, self.train_y = construct_train_feature(raw_data=self.training_data.values.tolist(),
                                                             tag_dict=self.tag_dict[self.cid])
        self.train_x, self.train_y = sample_balance(train_x=self.train_x, train_y=self.train_y)
        return

    def build_prediction_feature(self):
        # 构造预测数据
        print (u"{0} 正在获取预测数据... ".format(datetime.now()))
        # 获取客户店铺之外所有对应品类下的商品信息
        self.competitor_items = get_competitor_shop_items(
            db=self.industry, table=self.source_table, shop_id=self.customer_shop_id, category_id=self.cid,
            date_range=self.date_range)
        print (u"{0} 正在构造预测用数据的特征矩阵 品类是<{1}>, 月份为<{2}>... ".format(
            datetime.now(), self.cid_cname[self.cid], self.date_range))
        self.predict_x, self.item_pairs = construct_prediction_feature(
            customer_data=self.customer_shop_items.values,
            competitor_data=self.competitor_items.values,
            tag_dict=self.tag_dict[self.cid])
        return
    # endregion

    # region 模型训练和预测
    def train(self):
        model = GradientBoostingRegressor()
        print (u"{0} 正在生成训练模型... ".format(datetime.now()))
        self.model[self.cid_cname[self.cid]] = model.fit(self.train_x, self.train_y)
        return

    def predict(self):
        print u"{0} predict_x 行数为{1}".format(datetime.now(), self.predict_x.shape[0])
        if self.predict_x.shape[0] > 0:
            self.predict_y = self.model[self.cid_cname[self.cid]].predict(self.predict_x)
        # 对客户的每个店铺进行搜索，计算同表内不同商店的商品的竞品相似度
        return
    # endregion

    # region 入库
    def set_data(self):
        """
        将最后的score更新到数据库中, 分数要在0.5以上
        :return:
        """
        if self.predict_x.shape[0] == 0:
            print u"{0} 没有预测用的X，跳过品类{1}".format(datetime.now(), self.cid_cname[self.cid])
            return
        self.data_to_db = []
        for row in zip(self.item_pairs, self.predict_y):
            if row[1] >= 0.5:
                self.data_to_db.append(
                    (self.customer_shop_id, row[0][0], row[0][1], round(row[1], 4), self.date_range, self.cid))
        delete_score(db=self.industry, table=self.target_table, shop_id=self.customer_shop_id,
                     category_id=self.cid, date_range=self.date_range
                     )
        print u"{0} 开始将预测结果写入表{1}... 行数为{2}".format(
            datetime.now(), self.target_table, len(self.data_to_db))
        set_scores(db=self.industry, table=self.target_table, args=self.data_to_db)
        self.statistic_info.append({"CID": self.cid, "DateRange": self.date_range, "rows": len(self.data_to_db)})
        return
    # endregion

    # region 程序入口
    def main(self,
             industry='mp_women_clothing',
             source_table='itemmonthlysales2015',
             target_table='itemmonthlyrelation_2015',
             shop_id=66098091):
        """
        1. 读数据库
        2. 处理不同品类的标签, 构造特征
        3. 训练数据
        4. 预测数据
        5. 写入数据库
        :return:
        """
        self.industry = industry
        self.source_table = source_table
        self.target_table = target_table
        self.customer_shop_id = shop_id
        # cid 1623 半身裙
        for cid in (self.cid_cname.keys()):
            self.cid = cid
            self.build_train_feature()
            self.build_prediction_feature()
            self.train()
            self.predict()
            self.set_data()
        import pandas as pd
        print pd.DataFrame(self.statistic_info)["rows"].sum()
    # endregion

    # region 手动垃圾回收
    def clear_training_data(self):
        self.training_data = None
    # endregion

if __name__ == '__main__':
    c = CalculateCompetitiveItems()
    c.main(industry='mp_women_clothing', source_table='itemmonthlysales2015',
           target_table='itemmonthlyrelation_2015', shop_id=66098091)


# coding: utf-8
# from competitive_item_calculation import CalculateCompetitiveItems as CC
# c, c.cid = CC(), 1623L
# c.build_train_feature()
# c.build_prediction_feature()
