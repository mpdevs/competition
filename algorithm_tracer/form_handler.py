# coding: utf-8
# __author__: "John"
from tornado.web import RequestHandler as BaseHandler
import textwrap
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
            source_table=u"TaggedItemAttr", target_table=u"itemmonthlyrelation_2015", date_range=u"2015-12-01")
        vr.main(item1_id=item1_id, item2_id=item2_id, category_id=category_id)
        source_item = vr.item1
        target_item = vr.item2
        print source_item[1]
        print target_item[1]
        if vr.essential_dimension_conflict:
            self.render(u"demo_error.html", si=source_item, ti=target_item)
        else:
            dimension_list = vr.tag_dict[vr.category_id].keys()
            self.render(u"demo.html", si=source_item, ti=target_item, fv=vr.feature_vector_demo, y=vr.predict_y,
                        li=dimension_list)
