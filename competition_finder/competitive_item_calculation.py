# coding: utf-8
# __author__: u"John"
"""
基于训练模型
"""
from helper import *
from db_apis import *
from collections import OrderedDict
from sklearn.ensemble import GradientBoostingRegressor
from common.db_apis import *
from common.settings import *
from common.pickle_helper import pickle_dump
from datetime import datetime


class CalculateCompetitiveItems(object):
    # region 内部所有属性的初始化
    def __init__(self, industry=u"mp_women_clothing", source_table=u"itemmonthlysales2015",
                 target_table=u"itemmonthlyrelation_2015", shop_id=66098091, cid=1623):
        print (u"{0} 正在连接数据库 ...".format(datetime.now()))
        # region 必要
        self.industry = industry
        self.source_table = source_table
        self.target_table = target_table
        self.customer_shop_id = shop_id
        self.date_range = get_max_date_range(db=self.industry, table=self.source_table)
        self.date_range_list = None
        print (u"{0} 正在获取品类信息... ".format(datetime.now()))
        self.category_dict = {int(row[0]): row[1] for row in get_categories(db=self.industry)}
        self.category_id = cid
        # 用来将商品标签向量化
        print (u"{0} 正在抽取标签元数据... ".format(datetime.now()))
        self.attribute_meta = get_attribute_meta(db=self.industry)
        # self.items_attributes = None
        # key=CategoryID value=CategoryName
        # CID: CNAME
        print (u"{0} 正在生成商品的标签dict... ".format(datetime.now()))
        self.tag_dict = tag_to_dict(df=self.attribute_meta[[u"CID", u"DisplayName", u"AttrValue"]])
        self.training_data = None
        self.train_x = None
        self.train_x_positive = dict()
        self.train_x_negative = dict()
        self.test_x = dict()
        self.train_y = None
        # key=category value=model
        self.model = OrderedDict()
        self.predict_x = None
        self.predict_y = None
        self.message = None
        self.customer_shop_items = None
        self.competitor_items = None
        self.item_pairs = None
        self.data_to_db = None
        # endregion
        # region 可选
        # 必要维度法
        self.use_essential_tag_dict = False
        self.essential_tag_dict = parse_essential_dimension(get_essential_dimensions(db=self.industry)) \
            if self.use_essential_tag_dict else None
        self.statistic_info = []
        # endregion
        # region 用于子类的继承
        self.construct_prediction_feature = construct_prediction_feature
        # endregion
        return
    # endregion

    # region print实例的值
    def __str__(self):
        """
        调试程序
        :return:
        """
        string = u"industry={0}\ncustomer_shop_id={1}\nCategoryName={2}\n".format(
            self.industry, self.customer_shop_id, self.category_dict[self.category_id])
        return string
    # endregion

    # region 特征(feature)构造
    def build_train_feature(self):
        """
        按需训练
        :return:
        """
        print (u"{0} 正在获取训练数据... ".format(datetime.now()))
        self.training_data = get_training_data(cid=self.category_id)
        # 构造训练数据
        # training_data: attr1, attr2, score
        print (u"{0} 正在构造训练数据的特征矩阵... ".format(datetime.now()))
        self.train_x, self.train_y = construct_train_feature(raw_data=self.training_data.values.tolist(),
                                                             tag_dict=self.tag_dict[self.category_id])

        # region 用于echarts展示
        df_x = pd.DataFrame(self.train_x)
        df_y = pd.DataFrame(self.train_y, columns=[u"y"])
        df = pd.concat([df_x, df_y], axis=1, join_axes=[df_x.index])
        self.train_x_positive[self.category_id] = df[df.y > 0.5].values
        self.train_x_negative[self.category_id] = df[df.y <= 0.5].values
        # endregion
        self.train_x, self.train_y = sample_balance(train_x=self.train_x, train_y=self.train_y)
        return

    def build_prediction_feature(self):
        print (u"{0} 正在获取预测数据... ".format(datetime.now()))
        # TaggedItemAttr, item_id, shop_id DataFrame
        print (u"{0} 正在获取客户商品信息... ".format(datetime.now()))
        self.customer_shop_items = get_customer_shop_items(
            db=self.industry, table=self.source_table, shop_id=self.customer_shop_id, date_range=self.date_range,
            category_id=self.category_id)
        # 构造预测数据
        print (u"{0} 正在获取竞争对手数据... ".format(datetime.now()))
        # 获取客户店铺之外所有对应品类下的商品信息
        self.competitor_items = get_competitor_shop_items(
            db=self.industry, table=self.source_table, shop_id=self.customer_shop_id, category_id=self.category_id,
            date_range=self.date_range)
        print (u"{0} 正在构造预测用数据的特征矩阵 品类是<{1}>, 月份为<{2}>... ".format(
            datetime.now(), self.category_id, self.date_range))
        # 如果没有必要维度的品类
        essential_tag_dict = None
        try:
            essential_tag_dict = self.essential_tag_dict[self.category_id]
        except KeyError:
            pass
        except TypeError:
            pass
        self.predict_x, self.item_pairs = construct_prediction_feature(
            customer_data=self.customer_shop_items.values,
            competitor_data=self.competitor_items.values,
            tag_dict=self.tag_dict[self.category_id], essential_tag_dict=essential_tag_dict)
        return
    # endregion

    # region 模型训练和预测
    def train(self):
        model = GradientBoostingRegressor()
        print (u"{0} 正在生成训练模型... ".format(datetime.now()))
        self.model[self.category_dict[self.category_id]] = model.fit(self.train_x, self.train_y)
        return

    def predict(self):
        print u"{0} predict_x 行数为{1}".format(datetime.now(), self.predict_x.shape[0])
        if self.predict_x.shape[0] > 0:
            self.predict_y = self.model[self.category_dict[self.category_id]].predict(self.predict_x)
            df_x = pd.DataFrame(self.predict_x)
            df_y = pd.DataFrame(self.predict_y, columns=[u"y"])
            df = pd.concat([df_x, df_y], axis=1, join_axes=[df_x.index])
            self.test_x[self.category_id] = df.values
        # 对客户的每个店铺进行搜索，计算同表内不同商店的商品的竞品相似度
        return
    # endregion

    # region 入库
    def set_data(self):
        """
        将最后的score更新到数据库中, 分数要在0.5以上
        :return:
        """
        delete_score(db=self.industry, table=self.target_table, shop_id=self.customer_shop_id,
                     category_id=self.category_id, date_range=self.date_range)
        if self.predict_x.shape[0] == 0:
            print u"{0} 没有预测用的X，跳过品类{1}".format(datetime.now(), self.category_id)
            return
        self.data_to_db = []
        for row in zip(self.item_pairs, self.predict_y):
            if row[1] >= 0.5:
                self.data_to_db.append(
                    (self.customer_shop_id, row[0][0], row[0][1], round(row[1], 4), self.date_range, self.category_id))
        print u"{0} 开始删除店铺ID={1},品类为<{2}>,月份为<{3}>的竞品数据...".format(
            datetime.now(), self.customer_shop_id, self.category_id, self.date_range)
        print u"{0} 开始将预测结果写入表{1}... 行数为{2}".format(
            datetime.now(), self.target_table, len(self.data_to_db))
        set_scores(db=self.industry, table=self.target_table, args=self.data_to_db)
        self.statistic_info.append(
            {u"CID": self.category_id, u"DateRange": self.date_range, u"rows": len(self.data_to_db)})
        return
    # endregion

    # region 程序入口
    def main(self):
        """
        1. 读数据库
        2. 处理不同品类的标签, 构造特征
        3. 训练数据
        4. 预测数据
        5. 写入数据库
        6. 数据会进行分割，先按CategoryID，再按DaterRange进行双重循环
        :return:
        """
        # cid 1623 半身裙
        for cid in (self.category_dict.keys()):
            self.category_id = cid
            self.build_train_feature()
            self.build_prediction_feature()
            self.train()
            self.predict()
            self.set_data()
        # region pickle用于运算检验一致性
        pickle_dump(TRAIN_MODEL_PICKLE, self.model)
        pickle_dump(TRAIN_X_POSITIVE_PICKLE, self.train_x_positive)
        pickle_dump(TRAIN_X_NEGATIVE_PICKLE, self.train_x_negative)
        pickle_dump(TEST_X_PICKLE, self.test_x)
        self.tag_dict = tag_to_dict(df=self.attribute_meta[[u"CID", u"DisplayName", u"AttrValue"]])
        pickle_dump(TAG_DICT_PICKLE, self.tag_dict)
        # import pandas as pd
        # return pd.DataFrame(self.statistic_info)["rows"].sum()
        # endregion
        return
    # endregion

    # region 手动垃圾回收
    def clear_training_data(self):
        self.training_data = None
        return
    # endregion

if __name__ == u"__main__":
    c = CalculateCompetitiveItems()
    c.main()
    # result = []
    # for i in range(100):
    #     result.append(c.main(industry="mp_women_clothing", source_table="itemmonthlysales2015", target_table="itemmonthlyrelation_2015", shop_id=66098091))
    # sum_value = 0
    # max_value = result[0]
    # min_value = result[0]
    # for value in result:
    #     sum_value += value
    #     if min_value > value:
    #         min_value = value
    #     if max_value < value:
    #         max_value = value
    # print u"min_value={0}, max_value={1}, avg_value={2}".format(min_value, max_value, sum_value / len(result))

# coding: utf-8
# from competitive_item_calculation import CalculateCompetitiveItems as CC
# c, c.cid = CC(), 1623L
# c.build_train_feature()
# c.build_prediction_feature()
# 100times min_value=10627, max_value=775740, avg_value=396130
