# coding: utf-8
# __author__: u"John"
"""
打标签主程序
将数据分成有商品描述和没有商品描述的两个类
"""
from db_apis import *
from helper import *
from os import makedirs
from common.db_apis import *
from common.settings import *


# region TaggedItemAttr字段打标签
class AttrTagger(object):
    """
    打标签流程：
        0. 获取必要数据
            get_categories => self.category_dict
            get_brand => self.brand_list
            get_simplified_color => self.color_list
        1. 选择标签种类[TaggedItemAttr, TaggedBrand, TaggedMaterial, TaggedColor] => self.tag_column， 生成需要打标签的列表
        2. 获取标签类数据
            维度值列表 get_tag_attribute_meta => self.attribute_value_list
        3. 根据行业 self.db、表self.table、品类self.category_id、时间范围self.date_range
            获取数据 get_items_attr => self.items_attr_list
        4. 根据self.items_attr.HasAttribute展开分支，解析数据，生成标签字典，打标签
            4.1 self.current_item_attr有描述的直接解析成字典self.parsed_item，根据标签种类打标签
                self.attr_desc_parser(self.parsed_item) => self.tagged_items_attr_list
                通用标签 -> 需要维度值列表、品牌列表
                品牌标签 -> 需要品牌列表
                材质标签 -> 不需要任何列表
                颜色标签 -> 需要颜色列表
                依据每个列表的打标签规则，对字典self.parsed_item进行匹配，匹配结果存到self.tagged_items_attr
            4.2 self.current_item_attr没有描述的遍历如下的列表生成字典，在遍历的过程中，匹配到的字符串都将被移除
                self.current_item_attr := self.current_item_attr.replace(AttrValue)
                self.attr_parser => self.tagged_items_attr_list
                4.2.1 品牌列表
                4.2.2 常规维度列表，需要考虑维度值歧义的问题 self.ambiguous_attr_df
                4.2.3 颜色组
                在遍历过所有情况后，一个商品打标签结束
        5. 对打完标签的数据处理成字符串format_tag(self.tagged_items_attr_list) => self.items_attr_list
        6. 将新的字符串更新到数据库update_tag(self.items_attr_list)
    """
    # region 初始化
    def __init__(self, db, table, date_range=None):
        """
        0. 获取必要数据
        :param db:
        :param table:
        :param date_range:
        """
        self.db = db
        self.table = table
        self.date_range = date_range
        self.category_id = None  # 当前品类id
        self.category_dict = {int(row[0]): row[1] for row in get_categories(db=self.db, category_id_list=[])}
        self.brand_list = get_brand(db=self.db).values.tolist()  # 品牌列表
        self.color_list = get_simplified_color().values.tolist()  # 颜色组列表
        self.color_key_list = []  # 由于牛仔裤的特殊性，单独把打标签用的颜色维度进行处理
        self.attribute_value_list = None  # 当前属性维度值列表
        self.ambiguous_attr_value_df = []  # 0: CategoryID, 1: CategoryName, 2: AttrName, 3: AttrValue, 4: Flag
        self.dimension_list = []  # 有效维度的列表，指定哪些维度是属于需要的标签
        self.items_attr_list = []  # 属性列表
        self.current_item_id = None  # 当前商品ID
        self.current_item_attr = None  # 当前商品ID的原始数据
        self.parsed_item = None  # 当前正在处理的商品标签字典
        self.tagged_items_attr_list = []  # 已完成打标签工作的列表
        self.error_list = []  # 异常列表
        self.incremental = True  # 是否增量
        self.tag_column = u"TaggedItemAttr"  # 需要打标签的字段
        self.potential_new_attr_value_list = []  # 可能是新的维度值的列表，用于字典的维护
        self.processed_item_id_list = []  # 这是一个中间的变量
        self.item_id = None  # 用于单个商品打标签的子类
        return
    # endregion

    # region 打标签的对象
    def make_tag_list(self):
        """
        2. 获取标签类数据
        用于重载时候代码复用
        :return:
        """
        self.attribute_value_list = get_tag_attribute_meta(db=self.db, category_id=self.category_id)
        self.make_dimension_list()
        self.attribute_value_list = self.attribute_value_list[[u"DisplayName", u"AttrValue"]]
        self.ambiguous_attr_value_df = get_ambiguous_attr_value(category_id=self.category_id)
        return

    def make_dimension_list(self):
        self.dimension_list = list(set(self.attribute_value_list.DisplayName.values.tolist())) + [u"品牌"]
        return
    # endregion

    # region 数据准备：取数据、解析数据
    # region 数据获取
    def get_data(self):
        """
        3. 获取数据
        :return:
        """
        self.make_tag_list()
        self.processed_item_id_list = []
        self.tagged_items_attr_list = []
        print u"{0} 开始获取品类<{1}>的数据...".format(datetime.now(), self.category_id)
        self.items_attr_list = get_items_attr(
            db=self.db, table=self.table, category_id=self.category_id, date_range=self.date_range,
            incremental=self.incremental
        ).values.tolist()
        print u"{0} 品类<{1}>的数据获取完成一共<{2}>条数据...".format(
            datetime.now(), self.category_id, len(self.items_attr_list))
        return

    def get_item_data(self):
        self.make_tag_list()
        print u"{0} 开始获取品类<{1}>的数据...".format(datetime.now(), self.category_id)
        self.items_attr_list = get_item_attr(db=self.db, table=self.table, item_id=self.item_id).values.tolist()
        return
    # endregion

    # region 数据处理
    def process_data(self):
        """
        4. 打标签
        有AttrDesc和没有AttrDesc会同时执行，因为解析方式不一样，所以调用的函数不同
        :return:
        """
        # 0: ItemID, 1: CategoryID, 2: Attribute, 3: HasDescription
        print u"{0} 正在转换品类<{1}>条数据的格式...".format(datetime.now(), self.category_id)
        for line in self.items_attr_list:
            self.current_item_id = line[0]
            self.current_item_attr = line[2]
            self.parsed_item = dict()
            if line[3] == u"y":  # 有商品描述
                self.attr_desc_parser()
            else:  # 没有商品描述，标题字段
                self.name_parser()
            self.tagged_items_attr_list.append(self.parsed_item)
            self.processed_item_id_list.append(self.current_item_id)
        return
    # endregion
    # endregion

    # region 标签的生成
    # region 商品描述/商品标题解析器
    # region 商品描述的解析器
    def attr_desc_parser(self):
        """
        将爬虫获取商品描述字符串数据转换成字典的格式
        :return: items_attr: list(dict(key=维度,value=维度值)), error_info: list, error_items: list
        """
        try:
            if self.current_item_attr[-1] == u",":
                self.current_item_attr = self.current_item_attr[0:-1]
        except IndexError as e:
            self.error_handler(code=u"T-000001", message=e)
        for dimension_value in self.current_item_attr.split(u","):
            key_pair = dimension_value.split(u":")
            try:
                key = unicode_decoder(key_pair[0])
                # 匹配不到的关键词就不需要
                if not is_tag(key, self.dimension_list):
                    continue
                value = self.tag_value_process(key, strip(unicode_decoder(key_pair[1])))
                # 通用维度值处理的时候如果找不到维度值，该维度就不能算作标签
                if not value:
                    continue
            except IndexError as e:
                self.error_handler(code=u"T-000002", message=e)
                continue
            try:
                self.parsed_item_chunk(key=key, value=value)
            except KeyError as e:
                self.error_handler(code=u"T-000003", message=e)
                continue
        return
    # endregion

    # region 商品标题的解析器
    def name_parser(self):
        """
        将爬虫获取的店铺名、标题、子标题的组合字符串数据转换成字典的格式
        遍历顺序：
            1. brand_list 品牌
            2. attr_list 维度-值
            3. color_list 颜色
        :return: items_attr: list(dict(key=维度,value=维度值)), error_info: list, error_items: list
        """
        # if self.tag_column in [u"TaggedItemAttr", u"TaggedBrandName"]:
        #     self.brand_enumerate()
        if self.tag_column in [u"TaggedItemAttr"]:
            self.attribute_value_enumerate()
        if self.tag_column in [u"TaggedItemAttr", u"TaggedColor"]:
            self.color_enumerate()
        return

    def brand_enumerate(self):
        breaker = False
        for line in self.brand_list:
            if breaker:
                break
            for brand in line[1].split(u","):
                if self.include_chunk(value=brand):
                    self.parsed_item_chunk(key=u"品牌", value=line[0])
                    self.current_item_attr_chunk(value=brand)
                    breaker = True
                    break
        return

    def attribute_value_enumerate(self):
        for line in self.attribute_value_list.values.tolist():
            if line[0] in [u"材质成分"]:
                continue
            if line[0] in [u"颜色"] and self.category_id == 162205:
                continue
            for value in line[1].split(u","):
                if self.include_chunk(value):
                    self.parsed_item_chunk(key=line[0], value=value)
                    self.current_item_attr_chunk(value)
        return

    def color_enumerate(self):
        # line -- 0: ColorGroup, 1: ColorName, 1: ColorName + SimilarColor, 2: BlurredColor
        for line in self.color_list:
            hit = False
            for color in line[2].split(u","):
                if self.include_chunk(value=color):
                    self.parsed_item_chunk(key=u"颜色分类", value=line[1])
                    self.current_item_attr_chunk(value=line[1])
                    hit = True
            if not hit:
                if self.include_chunk(value=line[2]):
                    self.parsed_item_chunk(key=u"颜色分类", value=line[1])
        return
    # endregion
    # endregion

    # region 维度值处理器
    def tag_value_process(self, key, value):
        if len(value) > 512:
            self.error_handler(code=u"T-000004", message=u"tag_value_process too long value, must be ngt 512")
            return None
        if key == u"品牌":
            ret, code = brand_unify(value, self.brand_list)
            if code == 1:
                self.potential_new_attr_value_list.append((str(self.current_item_id), key, value))
        # 特殊的结构
        elif key == u"材质成分":
            ret = value
        elif key in self.color_key_list:
            color_list = list(color_cut(value))  # 颜色分词
            result = []
            for color in color_list:  # 枚举属性中的关键词的color_list
                # line - 0:ColorGroup, 1:ColorName, 2: ColorName + SimilarColor, 3:BlurredColor
                match_list = []
                for line in self.color_list:  # 枚举字典中的颜色
                    if color in line[2].split(u","):
                        match_list.append(line[1])
                        result.append(line[1])
                        continue
                    if self.include_chunk(line[3]):
                        match_list.append(line[1])
                        result.append(line[1])
                if len(match_list) == 0:
                    self.potential_new_attr_value_list.append((str(self.current_item_id), key, color))
            if len(result) > 0:
                ret = list(set(result))
            else:
                ret = None
        elif key in [u"流行元素", u"流行元素/工艺", u"图案", u"图案文化", u"中老年女装图案", u"里料图案",
                     u"工艺", u"制作工艺", u"服饰工艺", u"服装款式细节"]:
            # 模糊匹配
            valid_value_df = self.attribute_value_list[self.attribute_value_list.DisplayName == key].AttrValue
            valid_value_list = valid_value_df.values.tolist()[0].split(u",")
            match_list = []
            for v in valid_value_list:
                # 匹配到维度值的时候，需要把所有的匹配结果纳入其中
                if value.find(v) > -1 and v:
                    match_list.append(v)
            if len(match_list) > 0:
                ret = match_list
            else:
                ret = None
                self.potential_new_attr_value_list.append((str(self.current_item_id), key, value))
        else:
            # 精确匹配
            valid_value_df = self.attribute_value_list[self.attribute_value_list.DisplayName == key].AttrValue
            valid_value_list = valid_value_df.values.tolist()[0].split(u",")
            match_list = []
            for v in valid_value_list:
                # 匹配到维度值的时候，需要把所有的匹配结果纳入其中
                if value == v and v:
                    match_list.append(v)
                # 匹配不到就存放到一个列表，方便导出
            if len(match_list) > 0:
                ret = match_list
            else:
                ret = None
                self.potential_new_attr_value_list.append((str(self.current_item_id), key, value))
        return ret
    # endregion

    # region 数据入库的格式化
    def format_tagged_item(self):
        """
        5. 格式化
        :return:
        """
        print u"{0} 正在生成<{1}>条入库数据".format(datetime.now(), len(self.tagged_items_attr_list))
        pickle_dump(file_name=TAGGED_ITEMS_ATTR_LIST,
                    dump_object=zip(self.tagged_items_attr_list, self.processed_item_id_list))
        self.items_attr_list = format_tag(self.tagged_items_attr_list)
        return
    # endregion
    # endregion

    # region 标签入库
    def update_tag(self):
        """
        6. 更新入库
        指定更新列，需要重载
        :return:
        """
        print u"{0} 正在将品类<{1}>的标签写入数据库".format(datetime.now(), self.category_id)
        update_tag(db=self.db, table=self.table, column_name=self.tag_column,
                   args=zip(self.items_attr_list, self.processed_item_id_list))
        return
    # endregion

    # region Chunk
    def parsed_item_chunk(self, key, value):
        if key in self.parsed_item.keys():
            if isinstance(value, list):
                self.parsed_item[key] += value
            elif isinstance(value, unicode):
                self.parsed_item[key].append(value)
            elif isinstance(value, str):
                self.parsed_item[key].append(value)
            else:
                self.error_handler(code=u"T-000005", message=u"value type error, please check parser")
        else:
            self.parsed_item[key] = attr_value_chunk(value)
        return

    def include_chunk(self, value):
        if self.current_item_attr.find(value) > -1 and value != u"":
            return True
        else:
            return False

    def current_item_attr_chunk(self, value):
        self.current_item_attr = self.current_item_attr.replace(value, u"")
        return
    # endregion

    # region 异常处理中心
    def error_handler(self, code, message):
        self.error_list.append((str(self.current_item_id), code, unicode(message)))
        return
    # endregion

    # region 分析模块
    def analyzer(self):
        # dir_name = datetime.now().strftime(u"%Y%m%d%H%M%S")
        dir_name = self.category_dict[self.category_id].replace(u"/", u"")
        dir_path = path.join(path.join(path.dirname(path.abspath(__file__)), u"attribute_analyze"), dir_name)
        if not path.exists(dir_path):
            makedirs(dir_path)
        # 新维度值导出
        if len(self.potential_new_attr_value_list) > 0:
            export_excel(
                data=self.potential_new_attr_value_list, file_name=datetime.now().strftime(u"%Y%m%d%H%M%S"),
                sheet_name=self.category_dict[self.category_id].replace(u"/", u""), dir_path=dir_path
            )
        # 异常导出
        if len(self.error_list) > 0:
            export_excel(
                data=self.error_list, file_name=u"{0}_error_list".format(datetime.now().strftime(u"%Y%m%d%H%M%S")),
                sheet_name=self.category_dict[self.category_id].replace(u"/", u""), dir_path=dir_path
            )
        return
    # endregion

    # region main
    def main(self, category_id, date_range=None):
        """
        1. 选择标签种类
        需要重载main
        :param category_id:
        :param date_range:
        :return:
        """
        self.category_id = category_id
        self.date_range = date_range
        if self.category_id == 162205:
            self.color_key_list = [u"颜色分类", u"主要颜色"]
        else:
            self.color_key_list = [u"颜色", u"颜色分类", u"主要颜色"]
        self.get_data()
        if len(self.items_attr_list) == 0:
            print u"{0} 没有数据，跳过品类<{1}>".format(datetime.now(), self.category_id)
            return
        self.process_data()
        self.format_tagged_item()
        self.update_tag()
        self.analyzer()
        return
    # endregion
# endregion


# region TaggedBrandName字段打标签
class BrandTagger(AttrTagger):
    def __init__(self, db, table):
        AttrTagger.__init__(self, db=db, table=table)
        self.tag_column = u"TaggedBrandName"
        return

    def make_dimension_list(self):
        self.dimension_list = [u"品牌"]
        return
# endregion


# region TaggedMaterial字段打标签
class MaterialTagger(AttrTagger):
    def __init__(self, db, table):
        AttrTagger.__init__(self, db=db, table=table)
        self.tag_column = u"TaggedMaterial"
        return

    def make_dimension_list(self):
        self.dimension_list = [u"材质成分"]
        return

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
            self.parsed_item.update({key: material_list})
        else:
            return None
# endregion


# region TaggedColor字段打标签
class ColorTagger(AttrTagger):
    def __init__(self, db, table):
        AttrTagger.__init__(self, db=db, table=table)
        self.tag_column = u"TaggedColor"
        return

    def make_dimension_list(self):
        self.dimension_list = [u"颜色", u"颜色分类", u"主要颜色"]
        return

    def format_tagged_item(self):
        print u"{0} 正在生成<{1}>条入库数据".format(datetime.now(), len(self.tagged_items_attr_list))
        self.items_attr_list = format_color_group_tag(self.tagged_items_attr_list)
        return

    def tag_value_process(self, key, value):
        """
        颜色组匹配规则：
        1. value的值包含颜色组，返回颜色组
        2. value的值包含颜色，返回颜色组
        3. value的值包含任何一个相似色，返回颜色组
        4. value的值包含模糊色， 返回颜色组
        :param key:
        :param value:
        :return:
        """
        if key in [u"颜色", u"颜色分类", u"主要颜色"]:
            color_list = list(color_cut(value))
            ret = []
            for color in color_list:
                # row - 0:ColorGroup, 1:ColorName, 2:SimilarColor, 3:BlurredColor
                for row in self.color_list:
                    row_colors = []
                    for i in xrange(3):
                        if row[i]:
                            row_colors += row[i].split(u",")
                    if color in row_colors:
                        ret.append(row[0])
                    else:
                        pass
                    if row[3] and color.find(row[3]) > -1:
                        ret.append(row[0])
                    else:
                        continue
            return list(set(ret))
        else:
            return None
# endregion


# region所有字段打标签
class AllTagger(object):
    def __init__(self, db, table, category_id, date_range=None):
        self.db = db
        self.table = table
        self.category_id = category_id
        self.date_range = date_range
        return

    def main(self, **kwargs):
        if kwargs[u"attribute"]:
            print u"{0} {1} 开始执行AttrTagger {1}".format(datetime.now(), u"-" * 10)
            attr_tagger = AttrTagger(db=self.db, table=self.table)
            attr_tagger.main(category_id=self.category_id, date_range=self.date_range)
        if kwargs[u"brand"]:
            print u"{0} {1} 开始执行BrandTagger {1}".format(datetime.now(), u"-" * 10)
            brand_tagger = BrandTagger(db=self.db, table=self.table)
            brand_tagger.main(category_id=self.category_id, date_range=self.date_range)
        if kwargs[u"material"]:
            print u"{0} {1} 开始执行MaterialTagger {1}".format(datetime.now(), u"-" * 10)
            material_tagger = MaterialTagger(db=self.db, table=self.table)
            material_tagger.main(category_id=self.category_id, date_range=self.date_range)
        if kwargs[u"color"]:
            print u"{0} {1} 开始执行ColorTagger {1}".format(datetime.now(), u"-" * 10)
            color_tagger = ColorTagger(db=self.db, table=self.table)
            color_tagger.main(category_id=self.category_id, date_range=self.date_range)

# endregion


# region 针对某个ItemID打Attr标签
class OneItemAttrTagger(AttrTagger):
    def __init__(self, db, table):
        AttrTagger.__init__(self, db=db, table=table)
        return

    # region 根据商品ID调度的主程序
    def main(self, item_id):
        self.item_id = item_id
        self.category_id = get_category_by_item_id(db=self.db, table=self.table, item_id=self.item_id)
        if self.category_id == 162205:
            self.color_key_list = [u"颜色分类", u"主要颜色"]
        else:
            self.color_key_list = [u"颜色", u"颜色分类", u"主要颜色"]
        self.get_item_data()
        if len(self.items_attr_list) == 0:
            print u"{0} 没有数据，跳过品类<{1}>".format(datetime.now(), self.category_id)
        self.process_data()
        self.format_tagged_item()
        self.update_tag()
        return
    # endregion
# endregion


# region 针对某个ItemID打BrandName标签
class OneItemBrandTagger(BrandTagger):
    def __init__(self, db, table):
        BrandTagger.__init__(self, db=db, table=table)
        return

    # region 根据商品ID调度的主程序
    def main(self, item_id):
        self.item_id = item_id
        self.category_id = get_category_by_item_id(db=self.db, table=self.table, item_id=self.item_id)
        self.get_item_data()
        if len(self.items_attr_list) == 0:
            print u"{0} 没有数据，跳过品类<{1}>".format(datetime.now(), self.category_id)
        self.process_data()
        self.format_tagged_item()
        self.update_tag()
        return
    # endregion
# endregion


# region 针对某个ItemID打Material标签
class OneItemMaterialTagger(MaterialTagger):
    def __init__(self, db, table):
        MaterialTagger.__init__(self, db=db, table=table)
        return

    # region 根据商品ID调度的主程序
    def main(self, item_id):
        self.item_id = item_id
        self.category_id = get_category_by_item_id(db=self.db, table=self.table, item_id=self.item_id)
        self.get_item_data()
        if len(self.items_attr_list) == 0:
            print u"{0} 没有数据，跳过品类<{1}>".format(datetime.now(), self.category_id)
        self.process_data()
        self.format_tagged_item()
        self.update_tag()
        return
    # endregion
# endregion


# region 针对某个ItemID打Color标签
class OneItemColorTagger(ColorTagger):
    def __init__(self, db, table):
        ColorTagger.__init__(self, db=db, table=table)
        return

    # region 根据商品ID调度的主程序
    def main(self, item_id):
        self.item_id = item_id
        self.category_id = get_category_by_item_id(db=self.db, table=self.table, item_id=self.item_id)
        if self.category_id == 162205:
            self.color_key_list = [u"颜色分类", u"主要颜色"]
        else:
            self.color_key_list = [u"颜色", u"颜色分类", u"主要颜色"]
        self.get_item_data()
        if len(self.items_attr_list) == 0:
            print u"{0} 没有数据，跳过品类<{1}>".format(datetime.now(), self.category_id)
        self.process_data()
        self.format_tagged_item()
        self.update_tag()
        return
    # endregion
# endregion


# region 针对某个ItemID打所有标签
class OneItemTagger(object):
    def __init__(self, db, table):
        self.db = db
        self.table = table
        return

    def main(self, item_id):
        one_attr_tagger = OneItemAttrTagger(db=self.db, table=self.table)
        one_attr_tagger.main(item_id=item_id)
        one_brand_tagger = OneItemBrandTagger(db=self.db, table=self.table)
        one_brand_tagger.main(item_id=item_id)
        one_material_tagger = OneItemMaterialTagger(db=self.db, table=self.table)
        one_material_tagger.main(item_id=item_id)
        one_color_tagger = OneItemColorTagger(db=self.db, table=self.table)
        one_color_tagger.main(item_id=item_id)
        return
# endregion


if __name__ == u"__main__":
    from tqdm import tqdm
    _db = u"mp_women_clothing"
    _table = u"TaggedItemAttr"
    _item_id = 525316560097
    # _category_id = 1623
    for _category_id in tqdm([int(row[0]) for row in get_categories(db=_db, category_id_list=[])]):
        at = AttrTagger(db=_db, table=_table)
        at.main(category_id=_category_id)
    # at = AttrTagger(db=_db, table=_table)
    # at.main(category_id=_category_id)
    # bt = BrandTagger(db=_db, table=_table)
    # bt.main()
    # ct = ColorTagger(db=_db, table=_table)
    # ct.main()
    # mt = MaterialTagger(db=_db, table=_table)
    # mt.main()
    # brand = OneItemBrandTagger(db=_db, table=_table)
    # brand.main(item_id=_item_id)
    # oc = OneItemColorTagger(db=_db, table=_table)
    # oc.main(item_id=_item_id)
    # one = OneItemTagger(db=_db, table=_table)
    # one.main(item_id=_item_id)
    # oit = OneItemAttrTagger(db=_db, table=_table)
    # oit.main(item_id=_item_id)
    # all_tagger = AllTagger(db=_db, table=_table)
    # all_tagger.main(attribute=True, brand=False, material=False, color=False)



