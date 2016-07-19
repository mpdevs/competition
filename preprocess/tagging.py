# -*- coding: utf-8 -*
# __author__: John
"""
打标签主程序
将数据分成有商品描述和没有商品描述的两个类
"""
from datetime import datetime
from tqdm import tqdm
from db_apis import get_items_data, set_tag, get_categories
from helper import parse_raw_desc


class TaggingAttrDesc(object):
    def __init__(self, db, table):
        self.db = db
        self.table = table
        self.category_dict = {int(row[0]): row[1] for row in get_categories(db=self.db, category_id_list=[])}
        self.category_id = None
        self.items_data = None
        self.brand_list = None
        self.items_attr = None
        return

    def start_tag(self):
        print u"{0} 正在获取品类=<{1}>的商品描述数据...".format(datetime.now(), self.category_id)
        self.items_data = get_items_data(db=self.db, table=self.table, category_id=self.category_id)
        print u"{0} 正在转换数据格式...".format(datetime.now())
        self.items_attr = parse_raw_desc(attr_desc_list=self.items_data[u"Attribute"].values)
        if len(self.items_data) == 0:
            print u"{0} 没有数据需要打标签...".format(datetime.now())
            return
        else:
            print u"{0} {1}条数据需要打标签...".format(datetime.now(), len(self.items_data))
            self.items_data[u"ShopNameTitle"] = self.items_data[u"ShopNameTitle"].str.replace(u" ", u"")
            return

    def tag_attribute(self):

        return

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
                self.brand_list.append("")
        return

    def items_data_chunk(self):
        error_list = []
        processed_list = []
        for row in tqdm(self.items_data[u"Attribute"].values):
            row = row.split(u"，")
            print row
            d = dict()
            for col in row:
                spl = col.split(u":")
                try:
                    key = spl[0]
                    value = spl[1]
                except IndexError:
                    error_list.append(u"index error")
                    continue
                d[key] = value
            processed_list.append(d)
        return processed_list, error_list

    def main(self):
        for category_id in self.category_dict.keys():
            self.category_id = category_id
            self.start_tag()


class TaggingNoneAttrDesc(object):
    def __init__(self):
        return

    def start_tag(self):
        return

if __name__ == "__main__":
    tad = TaggingAttrDesc(db="mp_women_clothing", table="item_dev")
    success, error = tad.items_data_chunk()
    import pandas as pd
    df = pd.DataFrame(success)[u"品牌"].values.tolist()
    for i in df:
        print i
    print len(df)

