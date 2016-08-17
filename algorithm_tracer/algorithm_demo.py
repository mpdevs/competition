# coding: utf-8
# __author__: u"John"
from db_apis import *
from competition_finder.helper import *
import numpy as np
from datetime import datetime
from competition_finder.competitive_item_calculation import CalculateCompetitiveItems as CalC
from common.pickle_helper import pickle_load
from common.settings import *
from common.debug_helper import info


class VerifyResult(CalC):
    """
    该类用于CalculateCompetitiveItems类的可视化检验，查看竞品的计算过程
    """
    def __init__(self, source_table, target_table, date_range, industry=u"mp_women_clothing"):
        CalC.__init__(self, industry=industry, source_table=source_table, target_table=target_table)
        self.model = pickle_load(TRAIN_MODEL_PICKLE)
        self.competitive_item_pair_data = None
        self.date_range = date_range
        self.feature_vector = None
        self.feature_vector_demo = None
        self.pair_type = u"predict"
        self.category_id = None
        self.predict_y = None
        self.essential_dimension_conflict = False
        self.item1 = None
        self.item2 = None

    def get_data(self, item1_id, item2_id, category_id=None):
        self.competitive_item_pair_data = get_competitive_item_pair_info(
            db=self.industry, source_table=self.source_table, item1_id=item1_id, item2_id=item2_id, 
            date_range=self.date_range)
        # 如果竞品对能在source_table找到，则就用该表的信息
        if len(self.competitive_item_pair_data.values.tolist()) >= 2:
            self.category_id = self.competitive_item_pair_data.CategoryID.values[0]
        # 否则去训练数据的表获取
        else:
            self.category_id = category_id
            self.competitive_item_pair_data = get_train_item_pair_info(item1_id=item1_id, item2_id=item2_id)
            self.pair_type = u"train"
            if self.use_essential_tag_dict:
                self.essential_tag_dict[self.category_id] = None
            else:
                self.essential_tag_dict = None

    def build_feature(self):
        attr = self.competitive_item_pair_data.TaggedItemAttr.values.tolist()
        try:
            attr1, attr2 = attributes_to_dict(attr[0]), attributes_to_dict(attr[1])
            self.feature_vector = make_similarity_feature(attr1, attr2, self.tag_dict[self.category_id])
            self.feature_vector_demo = make_similarity_feature(attr1, attr2, self.tag_dict[self.category_id], demo=True)
            if self.use_essential_tag_dict:
                pass
            else:
                return True
            try:
                essential_tag_dict = self.essential_tag_dict[self.category_id]
                # 有必要维度字典，并且没有违反必要维度法
                if essential_dimension_trick(attr1=attr1, attr2=attr2, essential_tag_dict=essential_tag_dict):
                    debug(u"必要维度没有冲突")
                    return True
                else:
                    debug(u"和必要维度冲突")
                    self.essential_dimension_conflict = True
                    return False
            except KeyError as e:
                info(u"E 30001 raise exception:{0}".format(e))
                return True
            # self.feature_vector = make_similarity_feature(attr1, attr2, self.tag_dict[self.category_id])
            # return True
        except Exception as e:
            info(u"E3000 raise exception:{0}".format(str(e)))
            return False

    def predict(self):
        self.predict_y = self.model[self.category_dict[self.category_id]].predict(np.asarray(self.feature_vector))

    def show_process_and_result(self):
        debug(u"{0} 开始数据比较...".format(datetime.now()))
        items = self.competitive_item_pair_data.values
        self.item1, self.item2 = items[0], items[1]
        if self.pair_type == u"predict":
            debug(u"商品1的信息:\nid={0}\n标签={1}\n价格={2}\n品类={3}\n日期={4}\n".format(
                self.item1[0], self.item1[1][1:-1], self.item1[2], self.item1[3], self.item1[4]))
            debug(u"商品2的信息:\nid={0}\n标签={1}\n价格={2}\n品类={3}\n日期={4}\n".format(
                self.item2[0], self.item2[1][1:-1], self.item2[2], self.item2[3], self.item2[4]))
        else:
            debug(u"商品1的信息:\nid={0}\n标签={1}\n".format(self.item1[0], self.item1[1][1:-1]))
            debug(u"商品2的信息:\nid={0}\n标签={1}\n".format(self.item2[0], self.item2[1][1:-1]))
        debug(u"两个商品的特征向量是:\n{0}\n相似度回归值为:\n{1}\n特征向量的内容:\n".format(
            self.feature_vector, self.predict_y))
        # dimension_list = self.tag_dict[self.category_id].keys()
        # for i in range(len(dimension_list)):
        #     debug(u"维度 {0}:{1}".format(dimension_list[i], self.feature_vector[i]))

    def main(self, item1_id, item2_id, category_id):
        self.get_data(item1_id=item1_id, item2_id=item2_id, category_id=category_id)
        if self.build_feature():
            self.predict()
            self.show_process_and_result()
        else:
            debug(u"build feature failed")
            self.predict_y = 0
            self.feature_vector = [0] * len(self.tag_dict[self.category_id])

if __name__ == u"__main__":
    vr = VerifyResult(source_table=u"TaggedItemAttr", target_table=u"itemmonthlyrelation_2015",
                      date_range=u"")
    vr.main(item1_id=40590561581, item2_id=523793340966, category_id=121412004)

