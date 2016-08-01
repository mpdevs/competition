# coding: utf-8
# __author__: "John"
"""
打标签主程序
将数据分成有商品描述和没有商品描述的两个类
"""
from db_apis import *
from helper import *
from tqdm import tqdm
from os import path, sys
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from common.db_apis import *
from common.settings import INFO


# region TaggedItemAttr字段打标签
class AttrTagger(object):
    # region 初始化
    def __init__(self, db, table):
        self.db = db
        self.table = table
        self.category_dict = {int(row[0]): row[1] for row in get_categories(db=self.db, category_id_list=[])}
        self.tag_list = None
        self.tag_df = get_attribute_meta(db=self.db)
        self.category_id = None
        self.items_data = None
        self.items_no_attr_data = None
        self.brand_list = get_brand(db=self.db).values.tolist()
        self.items_attr = None
        self.current_item_id = None
        self.current_attr_dict = None
        self.tagged_items = None
        self.tagged_brand = None
        self.tagged_color = None
        self.tagged_material = None
        self.error_info = None
        self.has_data = True
        self.color_list = get_color().values.tolist()
        if INFO:
            self.attr_keys = set()
            self.none_attr_value = set()
        else:
            pass
        self.tag_column = u"TaggedItemAttr"
        return
    # endregion

    # region 打标签的对象
    def make_tag_list(self):
        """
        用于重载时候代码复用
        :return:
        """
        tag_list = self.tag_df.DisplayName.values.tolist()
        external_tag_list = [u"品牌"]
        return tag_list + external_tag_list
    # endregion

    # region 数据准备：取数据、解析数据
    def prepare_data(self):
        """
        有AttrDesc和没有AttrDesc会同时执行，因为解析方式不一样，所以调用的函数不同
        :return:
        """
        self.items_data = None
        self.has_data = True
        if INFO:
            self.none_attr_value = set()
        self.tag_list = self.make_tag_list()
        print u"{0} 正在获取品类<{1}>的商品描述和店铺数据...".format(datetime.now(), self.category_id)
        self.items_data = get_items_attr_data(db=self.db, table=self.table, category_id=self.category_id)
        print u"{0} 品类<{1}>数据获取完成，一共{2}条数据...".format(
            datetime.now(), self.category_id, self.items_data.values.shape[0])
        self.items_no_attr_data = get_items_no_attr_data(db=self.db, table=self.table, category_id=self.category_id)

        print u"{0} 正在转换品类<{1}>数据格式...".format(datetime.now(), self.category_id)
        self.tagged_items, self.error_info = self.attr_desc_parser()
        self.items_data = self.items_data.ItemID.values.tolist()

        if len(self.items_data) == 0:
            self.has_data = False
        print u"{0} 成功转换品类<{1}>的数据:{2}条, 失败:{3}条".format(
            datetime.now(), self.category_id, len(self.tagged_items), len(self.error_info))

        if len(self.items_data) != len(self.tagged_items):
            self.has_data = False
            print u"品类<{0}>出现数据转换问题,转换前长度:{1},转换后长度{2}".format(
                self.category_id, len(self.items_data), len(self.tagged_items))

        if INFO:
            for i in self.tagged_items:
                for k in i.keys():
                    self.attr_keys.add(k)
        return
    # endregion

    # region 标签的生成
    def tag_attr_by_desc(self):
        print u"{0} 正在打包<{1}>条数据".format(datetime.now(), len(self.tagged_items))
        for i, value in enumerate(self.tagged_items):
            if isinstance(value, unicode):
                # print i, value
                pass
        self.items_attr = tag_setter(self.tagged_items)
        return

    @staticmethod
    def tag_attr_by_name():
        return True

    # region 数据库格式的解析器
    def attr_desc_parser(self):
        """
        将爬虫获取的字符串数据转换成字典的格式
        :return: items_attr: list(dict(key=维度,value=维度值)), error_info: list, error_items: list
        """
        error_info = []
        items_attr = []
        for item in self.items_data[[u"ItemID", u"Attribute"]].values:
            self.current_attr_dict = dict()
            self.current_item_id = item[0]
            # 结尾逗号去除
            try:
                if item[1][-1] == u",":
                    item[1] = item[1][0:-1]
                else:
                    pass
            except IndexError as e:
                error_info.append((self.current_item_id, unicode(e)))
            attr_desc = item[1].split(u",")
            for dimension_value in attr_desc:
                key_pair = dimension_value.split(u":")
                try:
                    key = unicode_decoder(key_pair[0])
                    value = unicode_decoder(key_pair[1])
                    # 匹配不到的关键词就不需要
                    if is_tag(key, self.tag_list):
                        pass
                    else:
                        continue
                    value = self.tag_value_process(key, value)
                    # 通用维度值处理的时候如果找不到维度值，该维度就不能算作标签
                    if value:
                        pass
                    else:
                        continue
                    value = attr_value_chunk(value)
                except IndexError as e:
                    error_info.append((self.current_item_id, unicode(e)))
                    continue
                try:
                    if key in self.current_attr_dict.keys():
                        self.current_attr_dict[key].append(value)
                    else:
                        self.current_attr_dict[key] = value
                except KeyError as e:
                    error_info.append((self.current_item_id, unicode(e)))
                    continue
            items_attr.append(self.current_attr_dict)
        return items_attr, error_info
    # endregion

    # region 通用维度值处理
    def tag_value_process(self, key, value):
        if len(value) > 512:
            return None
        if key == u"品牌":
            ret = brand_unify(value, self.brand_list)
        elif key == u"材质成分":
            material_dict = dict()
            material_list = []
            # valid_value_list = self.tag_df[self.tag_df.DisplayName == key].AttrValue.values.tolist()[0].split(u",")
            for material_purity in value.split(u" "):
                purity = re.findall(ur"\d+\.?\d*", material_purity)[0]
                material = material_purity.replace(purity, u"").replace(u"%", u"")
                if u"(" in material and u")" in material:
                    material = material[material.find(u"(") + len(u"("): material.find(u")")]
                if u"（" in material and u"）" in material:
                    material = material[material.find(u"（") + len(u"（"): material.find(u"）")]
                if material not in [u"其他", u"其它"]:
                    material_dict.update({material: purity})
            for k, v in material_dict.iteritems():
                material_list.append(u"{0}-{1}".format(k, v))
            self.current_attr_dict.update({key: material_list})
            return
        # 特殊的结构
        elif key in [u"颜色", u"颜色分类", u"主要颜色"]:
            color_set = set(color_cut(value))
            # row: 0:ColorGroupName, 1:ColorName, 2:SimilarColor, 3:BlurredColor
            # 先判断是否有相似颜色，有则返回颜色，没有则判断是否有模糊色，有模糊色就返回颜色，最后再判断是否有颜色
            # for row in self.color_list:
            #     if row[2]:
            #         for s in row[2].split(u","):
            #             if value.find(s) > -1:
            #                 color_set.add(row[1])
            #             else:
            #                 if row[3]:
            #                     if value.find(row[3]):
            #                         color_set.add(row[1])
            #                         self.none_attr_value.add((str(self.current_item_id), key, value))
            #                     else:
            #                         continue
            #                 else:
            #                     continue
            #     else:
            #         if value.find(row[1]):
            #             color_set.add(row[1])
            #         else:
            #             continue
            # if len(color_set) > 0:
            #     self.none_attr_value.add((str(self.current_item_id), key, value))
            color_dict = {u"颜色": list(color_set)}
            self.current_attr_dict.update(color_dict)
            return
        # 通用的处理方式
        else:
            # 用属性值在商品描述匹配
            valid_value_list = self.tag_df[self.tag_df.DisplayName == key].AttrValue.values.tolist()[0].split(u",")
            match_list = []
            for v in valid_value_list:
                # 匹配到维度值的时候，需要把所有的匹配结果纳入其中
                if value.find(v) > -1:
                    match_list.append(v)
                # 匹配不到就存放到一个列表，方便导出
                else:
                    self.none_attr_value.add((str(self.current_item_id), key, value))
            if match_list:
                ret = u",".join(match_list)
            else:
                ret = None
        return ret

    # endregion
    # endregion

    # region 标签入库
    def update_tag(self):
        """
        指定更新列，需要重载
        :return:
        """
        print u"{0} 正在将品类<{1}>的标签写入数据库".format(datetime.now(), self.category_id)
        set_tag(db=self.db, table=self.table, column_name=self.tag_column, args=zip(self.items_attr, self.items_data))
        return
    # endregion

    # region 根据品类调度的主程序
    def main(self):
        for category_id in tqdm(self.category_dict.keys()):
            self.category_id = category_id
            self.prepare_data()
            if not self.has_data:
                continue
            self.tag_attr_by_desc()
            export_excel(
                data=self.none_attr_value, category=self.category_dict[self.category_id], category_id=self.category_id
            )
            self.update_tag()
        return
    # endregion
# endregion


# region TaggedBrandName字段打标签
class BrandTagger(AttrTagger):
    def __init__(self, db, table):
        AttrTagger.__init__(self, db=db, table=table)
        self.tag_column = u"TaggedBrandName"

    def make_tag_list(self):
        return [u"品牌"]
# endregion


# region TaggedMaterial字段打标签
class MaterialTagger(AttrTagger):
    def __init__(self, db, table):
        AttrTagger.__init__(self, db=db, table=table)
        self.tag_column = u"TaggedMaterial"

    def make_tag_list(self):
        return [u"材质成分"]

    def tag_value_process(self, key, value):
        """
        给面料打标签有以下几个需求：
        1. 只针对 DisplayName 为材质成为的属性打标签
        2. 保存格式 string: ",材质成分:占比,"
            2.1 如果只有材质，没有百分比数据，且材质类型大于1种，则对所有材质类型进行均分,占比为 1/count(材质)
                2.1.1 针对皮草和皮草类商品，除了需要匹配材质成分，还要匹配毛
        3. 如果材质成分内含"()"则取括号内的文字
        4. 属性字段没有材质成分的情况
        :param key:
        :param value:
        :return: list
        """
        if len(value) > 512:
            return None
        if key == u"材质成分":
            material_dict = dict()
            material_list = []
            # valid_value_list = self.tag_df[self.tag_df.DisplayName == key].AttrValue.values.tolist()[0].split(u",")
            for material_purity in value.split(u" "):
                purity = re.findall(ur"\d+\.?\d*", material_purity)[0]
                material = material_purity.replace(purity, u"").replace(u"%", u"")
                if u"(" in material and u")" in material:
                    material = material[material.find(u"(") + len(u"("): material.find(u")")]
                if u"（" in material and u"）" in material:
                    material = material[material.find(u"（") + len(u"（"): material.find(u"）")]
                if material not in [u"其他", u"其它"]:
                    material_dict.update({material: purity})
            for k, v in material_dict.iteritems():
                material_list.append(u"{0}-{1}".format(k, v))
            self.current_attr_dict.update({key: material_list})
        else:
            return None
# endregion


# region TaggedColor字段打标签
class ColorTagger(AttrTagger):
    def __init__(self, db, table):
        AttrTagger.__init__(self, db=db, table=table)
        self.tag_column = u"TaggedColor"

    def make_tag_list(self):
        return [u"颜色", u"颜色分类", u"主要颜色"]

    def tag_value_process(self, key, value):
        if key in [u"颜色", u"颜色分类", u"主要颜色"]:
            color_group_dict = dict()
            color_set = color_cut(value)
            for color in color_set:
                for row in self.color_list:
                    for col in row:
                        if col:
                            if color in col.split(u",") and color:
                                if color in color_group_dict.keys():
                                    color_group_dict[row[0]].append(color)
                                else:
                                    color_group_dict[row[0]] = [color]
                            else:
                                pass
                        else:
                            pass
                    else:
                        pass
                else:
                    pass
            if len(color_group_dict) > 0:
                ret = {k: list(set(v)) for k, v in color_group_dict.keys()}
                self.current_attr_dict.update(ret)
            return
        # 通用的处理方式
        else:
            # 用属性值在商品描述匹配
            valid_value_list = self.tag_df[self.tag_df.DisplayName == key].AttrValue.values.tolist()[0].split(u",")
            match_list = []
            for v in valid_value_list:
                # 匹配到维度值的时候，需要把所有的匹配结果纳入其中
                if value.find(v) > -1:
                    match_list.append(v)
                # 匹配不到就存放到一个列表，方便导出
                else:
                    self.none_attr_value.add((str(self.current_item_id), key, value))
            if match_list:
                ret = u",".join(match_list)
            else:
                ret = None
        return ret
# endregion

if __name__ == u"__main__":
    _db = u"mp_women_clothing"
    _table = u"TaggedItemAttr"
    # at = AttrTagger(db=_db, table=_table)
    # at.main()
    # bt = BrandTagger(db=_db, table=_table)
    # bt.main()
    ct = ColorTagger(db=_db, table=_table)
    ct.main()
    # mt = MaterialTagger(db=_db, table=_table)
    # mt.main()

