# coding: utf-8
# __author__: "John"
from db_apis import *
from helper import *
import numpy as np
from datetime import datetime
from os import path, sys
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from competition_finder.competitive_item_calculation import CalculateCompetitiveItems as CalC
from common.pickle_helper import pickle_load
from common.settings import train_model_pickle


class VerifyResult(CalC):
    """
    该类用于CalculateCompetitiveItems类的可视化检验，查看竞品的计算过程
    """
    def __init__(self, source_table, target_table, date_range, industry=u"mp_women_clothing"):
        CalC.__init__(self, industry=industry, source_table=source_table, target_table=target_table)
        self.model = pickle_load(train_model_pickle)
        self.competitive_item_pair_data = None
        self.date_range = date_range
        self.feature_vector = None
        self.pair_type = u"predict"
        self.category_id = None
        self.predict_y = None
        self.essential_dimension_conflict = False

    def get_data(self, item1_id, item2_id, category_id=None):
        self.competitive_item_pair_data = get_competitive_item_pair_info(
            db=self.industry, source_table=self.source_table, item1_id=item1_id, item2_id=item2_id, 
            date_range=u"2015-12-01")
        if self.competitive_item_pair_data.values:
            self.category_id = self.competitive_item_pair_data[u"CategoryID"].values[0]
        else:
            self.category_id = category_id
            self.competitive_item_pair_data = get_train_item_pair_info(item1_id=item1_id, item2_id=item2_id)
            self.pair_type = u"train"
            self.essential_tag_dict[self.category_id] = None

    def build_feature(self):
        attr = self.competitive_item_pair_data[u"TaggedItemAttr"].values
        try:
            attr1, attr2 = attributes_to_dict(attr[0]), attributes_to_dict(attr[1])
            if essential_dimension_trick(
                    attr1=attr1, attr2=attr2, essential_tag_dict=self.essential_tag_dict[self.category_id]):
                debug(u"必要维度没有冲突")
                self.feature_vector = make_similarity_feature_demo(attr1, attr2, self.tag_dict[self.category_id])
                return True
            else:
                debug(u"和必要维度冲突")
                self.essential_dimension_conflict = True
                return False
            # self.feature_vector = make_similarity_feature(attr1, attr2, self.tag_dict[self.category_id])
            # return True
        except Exception as e:
            print str(e)
            return False

    def predict(self):
        self.predict_y = self.model[self.category_dict[self.category_id]].predict(np.asarray(self.feature_vector))

    def show_process_and_result(self):
        debug(u"{0} 开始数据比较...".format(datetime.now()))
        items = self.competitive_item_pair_data.values
        item1, item2 = items[0], items[1]
        if self.pair_type == u"predict":
            debug(u"商品1的信息:\nid={0}\n标签={1}\n价格={2}\n品类={3}\n日期={4}\n".format(
                item1[0], item1[1][1:-1], item1[2], item1[3], item1[4]))
            debug(u"商品2的信息:\nid={0}\n标签={1}\n价格={2}\n品类={3}\n日期={4}\n".format(
                item2[0], item2[1][1:-1], item2[2], item2[3], item2[4]))
        else:
            debug(u"商品1的信息:\nid={0}\n标签={1}\n".format(item1[0], item1[1][1:-1]))
            debug(u"商品2的信息:\nid={0}\n标签={1}\n".format(item2[0], item2[1][1:-1]))
        debug(u"两个商品的特征向量是:\n{0}\n相似度回归值为:\n{1}\n特征向量的内容:\n".format(
            self.feature_vector, self.predict_y))
        dimension_list = self.tag_dict[self.category_id].keys()
        for i in range(len(dimension_list)):
            debug(u"维度 {0}:{1}".format(dimension_list[i], self.feature_vector[i]))

    def main(self, item1_id, item2_id, category_id):
        self.get_data(item1_id=item1_id, item2_id=item2_id, category_id=category_id)
        if self.build_feature():
            self.predict()
            self.show_process_and_result()
        else:
            self.predict_y = 0
            self.feature_vector = [0] * len(self.tag_dict[self.category_id])

if __name__ == u"__main__":
    vr = VerifyResult(source_table=u"itemmonthlysales2015", target_table=u"itemmonthlyrelation_2015",
                      date_range=u"2015-12-01")
    vr.main(item1_id=527684776697, item2_id=531834073847, category_id=121412004)

