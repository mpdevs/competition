# coding: utf-8
# __author__: "John"
"""
打标签主程序
将数据分成有商品描述和没有商品描述的两个类
"""
from db_apis import *
from helper import *
from datetime import datetime
from tqdm import tqdm
from os import path, sys
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from common.db_apis import *
from common.settings import INFO


class Tagger(object):
    # region init
    def __init__(self, db, table):
        self.db = db
        self.table = table
        self.category_dict = {int(row[0]): row[1] for row in get_categories(db=self.db, category_id_list=[])}
        self.category_id = None
        self.items_data = None
        self.items_no_attr_data = None
        self.brand_list = None
        self.items_attr = None
        self.error_info = None
        self.error_items = None
        if INFO:
            self.attr_keys = set()
        return
    # endregion

    # region Prepare Data
    def prepare_data(self):
        """
        有AttrDesc和没有AttrDesc会同时执行，因为解析方式不一样，所以调用的函数不同
        :return:
        """
        print u"{0} 正在获取品类<{1}>的商品描述和店铺数据...".format(datetime.now(), self.category_id)
        self.items_data = get_items_attr_data(db=self.db, table=self.table, category_id=self.category_id)
        self.items_no_attr_data = get_items_no_attr_data(db=self.db, table=self.table, category_id=self.category_id)
        print u"{0} 正在转换品类<{1}>数据格式...".format(datetime.now(), self.category_id)
        self.items_attr, self.error_items, self.error_info = attr_desc_parser(
            attr_desc_list=self.items_data[[u"ItemID", u"Attribute"]].values)
        print u"{0} 成功转换品类<{1}>的数据:{2}条, 失败:{3}条".format(
            datetime.now(), self.category_id, len(self.items_attr), len(self.error_items))
        if len(self.items_data) != len(self.items_attr):
            print u"品类<{0}>出现数据转换问题,转换前长度:{1},转换后长度{2}".format(
                self.category_id, len(self.items_data), len(self.items_attr))
        if INFO:
            for i in self.items_attr:
                for k in i.keys():
                    self.attr_keys.add(k)
        return
    # endregion

    # region Attribute
    def tag_attr_by_desc(self):
        self.items_attr = tag_setter(self.items_attr)
        return

    def tag_attr_by_name(self):
        return
    # endregion

    # region Brand
    def tag_brand(self):
        """
        天猫的商品描述都会有品牌字段，如果没有品牌字段
        :return:
        """
        self.brand_list = []
        for row in self.items_attr:
            try:
                brand = row[u"品牌"]
                self.brand_list.append(brand)
            except KeyError:
                self.brand_list.append(u"")
        return
    # endregion

    # region Material
    # endregion

    # region Color
    # endregion

    # region DataChunk
    # def items_data_chunk(self):
    #     error_list = []
    #     processed_list = []
    #     for row in tqdm(self.items_data[u"Attribute"].values):
    #         row = row.split(u"，")
    #         print row
    #         d = dict()
    #         for col in row:
    #             spl = col.split(u":")
    #             try:
    #                 key = spl[0]
    #                 value = spl[1]
    #             except IndexError:
    #                 error_list.append(u"index error")
    #                 continue
    #             d[key] = value
    #         processed_list.append(d)
    #     return processed_list, error_list
    # endregion

    def main(self):
        for category_id in tqdm(self.category_dict.keys()):
            self.category_id = category_id
            self.prepare_data()


if __name__ == u"__main__":
    t = Tagger(db=u"mp_women_clothing", table=u"TaggedItemAttr")
    # success, error = tad.items_data_chunk()
    t.main()

