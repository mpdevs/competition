# coding: utf-8
# __author__: "John"
from tornado.web import RequestHandler as BaseHandler
import textwrap
from os import path
import sys
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from algorithm_demo import VerifyResult


class IndexHandler(BaseHandler):
    def get(self):
        self.render(u"index.html")


class ReverseHandler(BaseHandler):
    def get(self, word):
        self.write(word[::-1])


class WrapHandler(BaseHandler):
    def post(self):
        text = self.get_argument(u"text")
        width = self.get_argument(u"width", 40)
        self.write(textwrap.fill(text, int(width)))


class AlgorithmDemoFormHandler(BaseHandler):
    def get(self):
        self.render(u"demo_form.html")


class AlgorithmDemoHandler(BaseHandler):
    def post(self):
        item1_id = int(self.get_argument(u"item1_id"))
        item2_id = int(self.get_argument(u"item2_id"))
        category_id = int(self.get_argument(u"category_id"))
        vr = VerifyResult(
            source_table=u"itemmonthlysales2015", target_table=u"itemmonthlyrelation_2015", date_range=u"2015-12-01")
        vr.main(item1_id=item1_id, item2_id=item2_id, category_id=category_id)
        item_pair = vr.competitive_item_pair_data.values
        source_item = item_pair[0]
        target_item = item_pair[1]
        if vr.essential_dimension_conflict:
            self.reder(u"demo_error.html", si=source_item, ti=target_item)
        else:
            dimension_list = vr.tag_dict[vr.category_id].keys()
            self.render(u"demo.html", si=source_item, ti=target_item, fv=vr.feature_vector, y=vr.predict_y,
                        li=dimension_list)
