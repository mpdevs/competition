# coding: utf-8
# __author__: u"John"
from tornado.web import RequestHandler as BaseHandler
from db_apis import *
from tagger.tag import OneItemTagger


class TagCheckFormHandler(BaseHandler):
    def get(self, *args, **kwargs):
        self.render(u"tag_form.html")


class TagCheckHandler(BaseHandler):
    def post(self, *args, **kwargs):
        item_id = int(self.get_argument(u"ItemID", 0))
        retag = self.get_argument(u"retag", False)
        if retag:
            oit = OneItemTagger(db=u"mp_women_clothing", table=u"TaggedItemAttr")
            oit.main(item_id=item_id)
        item = get_tagged_item_info(item_id=item_id).values.tolist()[0]
        category_id = item[5]
        attr_dict = get_category_displayname(category_id=category_id).values.tolist()
        item.append(attr_dict[0][1])
        displayname_list = [u"".join(i[0]) for i in attr_dict]
        self.render(u"tag_check.html", item=item, displayname_list=displayname_list)


class RetagFormHandler(BaseHandler):
    def get(self, *args, **kwargs):
        self.render(u"retag_form.html")



