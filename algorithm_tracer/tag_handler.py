# coding: utf-8
# __author__: u"John"
from tornado.web import RequestHandler as BaseHandler
from db_apis import get_tagged_item_info


class TagCheckFormHandler(BaseHandler):
    def get(self, *args, **kwargs):
        self.render(u"tag_form.html")


class TagCheckHandler(BaseHandler):
    def post(self, *args, **kwargs):
        item_id = int(self.get_argument(u"ItemID", 0))
        ret = get_tagged_item_info(item_id=item_id).values.tolist()[0]
        self.render(u"tag_check.html", item=ret)

