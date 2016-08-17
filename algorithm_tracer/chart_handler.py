# coding: utf-8
# __author__: "John"
from tornado.web import RequestHandler as BaseHandler
from helper import scatter_adapter
from common.pickle_helper import pickle_load
from common.settings import *
from common.db_apis import get_categories


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
        self.txp = pickle_load(TRAIN_X_POSITIVE_PICKLE)
        self.txn = pickle_load(TRAIN_X_NEGATIVE_PICKLE)
        self.tp = pickle_load(TEST_X_PICKLE)
        self.td = pickle_load(TAG_DICT_PICKLE)
        print type(self.td)
        # for k in self.txp.keys():
        #     debug(k)
        # debug(len(self.txp))
        # for k in self.txn.keys():
        #     debug(k)
        # debug(len(self.txn))
        # for k in self.tp.keys():
        #     debug(k)
        # debug(len(self.tp))
        self.ac = set(self.txp.keys()) & set(self.txn.keys())
        self.ac = list(self.ac & set(self.tp.keys()))
        # for category in self.ac:
        #     print category
        self.category = get_categories(db=u"mp_women_clothing", category_id_list=self.ac)
        self.cid = None

    def get(self, *args, **kwargs):
        category_id = int(self.get_argument(u"category_id", u"1623"))
        self.cid = category_id
        train_x_positive_data = {}
        train_x_negative_data = {}
        test_x_data = {}
        dimensions = self.td[self.cid].keys()
        for i in xrange(len(dimensions)):
            dimension_name = dimensions[i]
            train_x_positive = scatter_adapter(self.txp[self.cid], i)
            train_x_negative = scatter_adapter(self.txn[self.cid], i)
            test_x = scatter_adapter(self.tp[self.cid], i)
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


