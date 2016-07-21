# coding: utf-8
# __author__: "John"
from tornado.web import RequestHandler as BaseHandler


class ChartIndexHandler(BaseHandler):
    def __init__(self, application, request, **kwargs):
        super(ChartIndexHandler, self).__init__(application, request, **kwargs)
        self.json_data = 1

    def get(self):
        self.render(template_name=u"chart_base.html")


class FeatureVectorDistributionHandler(ChartIndexHandler):
    def get(self):
        print self.json_data
        self.render(template_name=u"feature_vector_distribution.html")

