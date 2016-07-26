# coding: utf-8
# __author__: "John"
from tornado.web import RequestHandler as BaseHandler
from tornado.escape import url_escape, url_unescape, json_encode, json_decode
import json
from os import sys, path
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from common.pickle_helper import pickle_load
from common.settings import *
from common.db_apis import get_categories
from helper import scatter_adapter, debug



class ChartIndexHandler(BaseHandler):
    def __init__(self, application, request, **kwargs):
        super(ChartIndexHandler, self).__init__(application, request, **kwargs)
        self.json_data = 1

    def get(self, *args, **kwargs):
        self.render(template_name=u"chart_base.html")


class FeatureVectorDistributionHandler(ChartIndexHandler):
    """
    这里用来验证竞品计算的方法数据上是否可行 20160721
    """
    def __init__(self, application, request, **kwargs):
        super(FeatureVectorDistributionHandler, self).__init__(application, request, **kwargs)
        self.train_x_positive = pickle_load(TRAIN_X_POSITIVE_PICKLE)
        self.train_x_negative = pickle_load(TRAIN_X_NEGATIVE_PICKLE)
        self.test_x_positive = pickle_load(TEST_X_PICKLE)
        self.tag_dict = pickle_load(TAG_DICT_PICKLE)
        # for k in self.train_x_positive.keys():
        #     debug(k)
        # debug(len(self.train_x_positive))
        # for k in self.train_x_negative.keys():
        #     debug(k)
        # debug(len(self.train_x_negative))
        # for k in self.test_x_positive.keys():
        #     debug(k)
        # debug(len(self.test_x_positive))
        self.available_category = set(self.train_x_positive.keys()) & set(self.train_x_negative.keys())
        self.available_category = list(self.available_category & set(self.test_x_positive.keys()))
        # for category in self.available_category:
        #     print category
        self.category = get_categories(db=u"mp_women_clothing", category_id_list=self.available_category)
        self.category_id = None

    def get(self, *args, **kwargs):
        category_id = int(self.get_argument(u"category_id", u"1623"))
        self.category_id = category_id
        train_x_positive_data = {}
        train_x_negative_data = {}
        test_x_data = {}
        dimensions = self.tag_dict[self.category_id].keys()
        for i in xrange(len(dimensions)):
            dimension_name = dimensions[i]
            train_x_positive = scatter_adapter(self.train_x_positive[self.category_id], i)
            train_x_negative = scatter_adapter(self.train_x_negative[self.category_id], i)
            test_x = scatter_adapter(self.test_x_positive[self.category_id], i)
            train_x_positive_data[dimension_name] = train_x_positive
            train_x_negative_data[dimension_name] = train_x_negative
            test_x_data[dimension_name] = test_x
        train_x_positive_data = {u"legend_data": train_x_positive_data.keys(), u"series_data": train_x_positive_data}
        train_x_negative_data = {u"legend_data": train_x_negative_data.keys(), u"series_data": train_x_negative_data}
        test_x_data = {u"legend_data": test_x_data.keys(), u"series_data": test_x_data}
        self.render(template_name=u"feature_vector_distribution.html",
                    train_x_positive_data=train_x_positive_data,
                    train_x_negative_data=train_x_negative_data,
                    test_x_data=test_x_data,
                    category=self.category)


