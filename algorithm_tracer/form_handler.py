# coding: utf-8
# __author__: "John"
from tornado.web import RequestHandler as BaseHandler
from algorithm_demo import VerifyResult


class IndexHandler(BaseHandler):
    def get(self):
        self.render(u"index.html")


class AlgorithmDemoFormHandler(BaseHandler):
    def get(self):
        self.render(u"demo_form.html")


class AlgorithmDemoHandler(BaseHandler):
    def post(self):
        item1_id = int(self.get_argument(u"item1_id", 0))
        item2_id = int(self.get_argument(u"item2_id", 0))
        category_id = int(self.get_argument(u"category_id", 0))
        vr = VerifyResult(
            source_table=u"itemmonthlysales2015", target_table=u"itemmonthlyrelation_2015", date_range=u"2015-12-01")
        vr.main(item1_id=item1_id, item2_id=item2_id, category_id=category_id)
        source_item = vr.item1
        target_item = vr.item2
        if vr.essential_dimension_conflict:
            self.render(u"demo_error.html", si=source_item, ti=target_item)
        else:
            dimension_list = vr.tag_dict[vr.category_id].keys()
            self.render(u"demo_result.html", si=source_item, ti=target_item, fv=vr.feature_vector_demo, y=vr.predict_y,
                        li=dimension_list)
