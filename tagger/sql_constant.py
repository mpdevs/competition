# coding: utf-8
# __author__ = 'John'

CATEGORY_QUERY = u"""SELECT DISTINCT c.CategoryID, c.CategoryName FROM mp_portal.category c JOIN mp_portal.industry i
ON c.IndustryID = i.IndustryID WHERE i.DBName = '{0}' {1};"""

ATTR_META_QUERY = u"""SELECT a.CID, a.Attrname, a.DisplayName, a.AttrValue, a.Flag FROM mp_portal.attr_value AS a
JOIN mp_portal.industry AS i ON a.IndustryID = i.IndustryID WHERE a.IsCalc='y' AND i.DBName ='{0}'"""

SHOP_QUERY = u"SELECT ShopID FROM shop where IsClient='y';"

TRAINING_DATA_QUERY = u"""SELECT a1.TaggedItemAttr AS attr1, a2.TaggedItemAttr AS attr2, c.score
FROM tagged_competitive_items c JOIN TaggedItemAttr a1 ON a1.ItemID = c.SourceItemID
JOIN TaggedItemAttr a2 ON a2.ItemID = c.TargetItemID WHERE c.CategoryID = {0}
AND a1.TaggedItemAttr LIKE ',%' AND a2.TaggedItemAttr LIKE ',%';"""

SET_SCORES_QUERY = u"""INSERT INTO {0}.{1}
(ShopID, SourceItemID, TargetItemID, Score ,DateRange, CategoryID, RelationType, Status)
VALUES (%s, %s, %s, %s, %s, %s, 1, 1);"""

DELETE_SCORES_QUERY = u"DELETE FROM {0}.{1} WHERE ShopID = {2} AND CategoryID = {3} AND DateRange = '{4}';"

MAX_DATE_RANGE_QUERY = u"SELECT MAX(DateRange) AS DateRange FROM {0}.{1};"

ESSENTIAL_DIMENSIONS_QUERY = u"""SELECT CategoryID, AttrName, AttrValue FROM essential_dimensions
WHERE MoreThanTwoNegPercent >= {0} AND ConfidenceLevel >= {1};"""

ITEMS_DATA_QUERY = u"""SELECT ItemID, CategoryID, concat_ws(' ',ItemSubTitle,ItemName) AS Title,
ItemAttrDesc AS Attribute, concat_ws(' ',ShopName,ItemSubTitle,ItemName) AS ShopNameTitle
FROM {0} WHERE TaggedItemAttr NOT LIKE ',%' {1};"""

CUSTOMER_ITEM_QUERY = u"""SELECT ItemID, TaggedItemAttr, DiscountPrice, CategoryID, DateRange
FROM {0} WHERE ShopID = {1} AND CategoryID = {2} AND DateRange = '{3}' AND TaggedItemAttr LIKE ',%';"""

COMPETITIVE_ITEM_QUERY = u"""SELECT ItemID, TaggedItemAttr, DiscountPrice, CategoryID, DateRange
FROM {0} WHERE (ShopID != {1} OR ShopID IS NULL) AND CategoryID = {2}
AND DateRange = '{3}' AND TaggedItemAttr LIKE ',%';"""

PREDICT_PAIR_INFO_QUERY = u"""SELECT ItemID, TaggedItemAttr, DiscountPrice, CategoryID, DateRange FROM {0}
WHERE ItemID IN ({1}, {2}) AND DateRange = '{3}' AND TaggedItemAttr LIKE ',%';"""

TRAIN_PAIR_INFO_QUERY = u"SELECT * FROM TaggedItemAttr WHERE ItemID IN ({0}, {1});"

GET_CATEGORY_ID_QUERY = u"SELECT CategoryID FROM {0} WHERE ItemID = {1} ORDER BY DateRange"

# region 废弃
Insert_sql = {
            'Normal': """(SourceItemID,TargetItemID,RelationType,Status,ShopId,Score)
            VALUES('%s',%s,'%s','%s', %s, %s)""",
            'Monthly': """(SourceItemID,TargetItemID,RelationType,Status,ShopId,DateRange,Score)
            VALUES(%s, %s ,%s, %s , %s, %s, %s)""",
            'History': """(SourceItemID,TargetItemID,RelationType,Status,ShopId,DateRange,Score)
            VALUES(%s, %s ,%s, %s , %s, %s, %s)"""
            }

Select_sql = {
            'Normal': """SELECT TaggedItemAttr as label, ItemID as itemid, ShopId as shopid, DiscountPrice, CategoryID
            FROM """,
            'Monthly': """SELECT TaggedItemAttr as label, ItemID as itemid, ShopId as shopid, DiscountPrice, CategoryID, DateRange
            FROM """,
            'History': """SELECT TaggedItemAttr as label, ItemID as itemid, ShopId as shopid, DiscountPrice, CategoryID, DateRange
            FROM """
            }

ATTRIBUTES_QUERY = """SELECT ItemID, TaggedItemAttr FROM {0} WHERE TaggedItemAttr IS NOT NULL AND TaggedItemAttr != ''
AND TaggedItemAttr LIKE ',%' {1};"""

DICT_FL = {
    'mp_women_clothing': [u"感官", u"风格", u"做工工艺", u"厚薄", u"图案", u"扣型", u"版型", u"廓型", u"领型", u"袖型",
                          u"腰型", u"衣长", u"袖长", u"衣门襟", u"穿着方式", u"组合形式", u"面料", u"色系", u"毛线粗细",
                          u"适用体型", u"裤型", u"裤长", u"裙型", u"裙长", u"fea", u"fun", u"适用年龄", u"适用人群"],
    'mp_men_clothing': [u"感官", u"风格", u"做工工艺", u"厚薄", u"图案", u"扣型", u"版型", u"廓型", u"领型", u"袖型",
                        u"腰型", u"衣长", u"袖长", u"衣门襟", u"穿着方式", u"组合形式", u"面料", u"颜色", u"毛线粗细",
                        u"适用体型", u"裤型", u"裤长", u"fea", u"fun", u"适用人群"],
    'mp_sports': [u"材质", u"儿童泳衣类型", u"厚薄", u"版型", u"闭合方式", u"干湿", u"肩带", u"扣型", u"裤门襟",u"裤型",
                  u"领型", u"露体", u"配件", u"裙型", u"调节性", u"形状", u"袖型", u"泳包适用对象", u"遮体", u"面料",
                  u"男士泳衣类型", u"女士泳衣类型", u"品质", u"轻重", u"容量", u"适用季节", u"适用年龄", u"适用身份",
                  u"适用性别", u"图案", u"颜色", u"泳镜类型", u"泳帽类型", u"fun", u"套餐", u"专业运动"]
}


DICT_MUST = {
    'mp_women_clothing': {
        'name': [
                []
        ],
        'value': [
                []
        ]
    },
    'mp_men_clothing': {
        'name': [
                [],
                [],
         ],
        'value': [
                [],
                [],
        ]
    },
    'mp_children_clothing': {
        'name': [
                [],
                [],
        ],
        'value': [
                [],
                [],
        ]
    },
    'mp_sports': {
        'name': [
            [u"连体泳衣", u"分体泳衣"],
            [u"男士泳衣"],
            [u"儿童泳衣/裤"],
            [u"泳镜"],
            [u"泳帽"]
        ],
        'value': [
            [u"女士泳衣类型"],  # [u"裤型",u"专业运动",u"女士泳衣类型"],
            [u"男士泳衣类型"],  # [u"男士泳衣类型",u"套餐"],
            [u"儿童泳衣类型", u"适用年龄", u"套餐", u"专业运动"],
            [u"泳镜类型", u"颜色"],  # [u"泳镜类型",u"适用年龄",u"套餐"],
            [u"泳帽类型"]  # [u"泳帽类型",u"面料",u"适用年龄"]
        ]
    }
}

DICT_EXCLUSIVES = {
    'mp_women_clothing': [u'领型', u'面料', u'袖款', u'袖长', u'腰型', u'厚薄', u"扣型"],
    'mp_men_clothing': [u'领型', u'面料', u'袖款', u'袖长', u'腰型', u'厚薄', u"扣型"],
    'mp_home_textile': [u'风格', u'安装方式', u'被子款式', u'闭合方式', u'边数', u'床单类型', u'床垫', u'宽度',
                        u'门数量', u'面数', u'毯类型', u'蚊帐', u'席子款式', u'枕头形态', u'边工艺', u'喷涂', u'边印染',
                        u'做工品质', u'尺寸', u'粗细', u'大小', u'弹性', u'档次', u'厚度', u'能效等级', u'软硬度',
                        u'性价比', u'长短'],
    'mp_children_clothing': [u"风格", u"穿着方式", u"扣型", u"裤门襟", u"裤款", u"裤长", u"领型", u"帽型", u"领型",
                             u"裙款", u"裙长", u"袖款", u"袖长", u"腰型", u"适用年龄", u"组合形式", u"厚薄",
                             u"洗涤方式"],
    'mp_sports': [u"泳镜类型", u"泳帽类型", u"裤长", u"领型", u"袖长", u"袖款", u"儿童泳衣类型", u"适用年龄",
                  u"专业运动", u"套餐", u"男士泳衣类型", u"女士泳衣类型"]
}

IMPORTANT_ATTR_ENUM = {
    'mp_women_clothing': {
        'name': [
            [u"羽绒服", u"棉衣/棉服"],
            [u"休闲裤", u"西装裤/正装裤", u"打底裤"],
            [u"牛仔裤"],
            [u"棉裤/羽绒裤"],
            [u"风衣", u"毛呢外套", u"短外套", u"皮草", u"西装", u"皮衣", u"西装套装", u"时尚套装"],
            [u"T恤", u"长袖", u"衬衫", u"背心吊带", u"蕾丝衫/雪纺衫", u"马夹"],
            [u"卫衣/绒衫", u"毛衣", u"毛针织衫"],
            [u"短裙", u"连衣裙", u"长裙", u"半身裙"],
            [u"大码女装", u"中老年女装"]
        ],
        'value': [
            [u"版型", u"廓型", u"衣门襟", u"图案", u"衣长", u"袖型", u"腰型",u"fun"],
            [u"版型", u"裤型", u"面料", u"裤长", u"腰型"],
            [u"版型", u"裤型", u"面料", u"裤长", u"腰型", u"做工工艺"],
            [u"版型", u"裤型", u"面料", u"裤长", u"腰型", u"厚薄"],
            [u"版型", u"廓型", u"面料", u"图案", u"衣长", u"袖型", u"腰型"],
            [u"版型", u"廓型", u"面料", u"图案", u"风格", u"颜色"],
            [u"版型", u"廓型", u"面料", u"图案", u"袖型", u"风格", u"颜色"],
            [u"版型", u"廓型", u"面料", u"图案", u"裙型", u"袖型"],
            [u"版型", u"廓型", u"面料", u"图案", u"裙型", u"衣长", u"袖型", u"裤型", u"裤长", u"腰型"]
        ]
    },
    'mp_men_clothing': {
        'name': [
            [u"羽绒服", u"棉衣"],
            [u"休闲裤", u"西裤", u"皮裤"],
            [u"牛仔裤"],
            [u"棉裤", u"羽绒裤"],
            [u"风衣", u"毛呢大衣", u"短外套", u"皮草", u"西服", u"皮衣", u"西服套装", u"夹克"],
            [u"T恤", u"衬衫", u"Polo衫", u"背心/马甲", u"民族服装"],
            [u"卫衣", u"针织衫/毛衣"]
        ],
        'value': [
            [u"版型", u"廓型", u"衣门襟", u"图案", u"衣长", u"袖型", u"腰型", u"fun"],
            [u"版型", u"裤型", u"面料", u"裤长", u"腰型"],
            [u"版型", u"裤型", u"面料", u"裤长", u"腰型", u"做工工艺"],
            [u"版型", u"裤型", u"面料", u"裤长", u"腰型", u"厚薄"],
            [u"版型", u"廓型", u"面料", u"图案", u"衣长", u"袖型", u"腰型"],
            [u"版型", u"廓型", u"面料", u"图案", u"风格", u"颜色"],
            [u"版型", u"廓型", u"面料", u"图案", u"袖型", u"风格", u"颜色"]
        ]
    },
    'mp_home_textile': {
        'name': [
            [u"休闲毯/毛毯/绒毯", u"毯子"],
            [u"婴童多件套", u"婴童/儿童三件套", u"婴童/儿童四件套", u"套件定制", u"床品套件/四件套/多件套"],
            [u"抱被", u"被子", u"被子/被芯定制", u"被子/蚕丝被/羽绒被/棉被"],
            [u"婴童蚊帐", u"蚊帐", u"床幔"],
            [u"床垫定制定做", u"床笠", u"床垫/床褥/床护垫/榻榻米床垫"],
            [u"靠垫定制", u"沙发垫/沙发套定制定做"],
            [u"婴童睡袋/防踢被", u"睡袋"],
            [u"电热毯"],
            [u"婴童凉席", u"凉席定制定做", u"凉席/竹席/藤席/草席/牛皮席"],
            [u"婴童枕头/枕芯", u"枕头/枕芯/保健枕/颈椎枕"],
            [u"床单定制定做", u"床单", u"床罩", u"婴童床单", u"婴童被套", u"床罩定制", u"被套定制", u"枕套定制",
             u"枕套", u"床裙", u"被套", u"枕巾", u"床盖", u"桌布/桌旗定制定做"],

        ],
        'value': [
            [u"图案", u"风格", u"材质", u"款式"],
            [u"fea", u"图案", u"风格"],
            [u"fea", u"面料", u"填充物", u"适用季节"],
            [u"fea", u"款式"],
            [u"fea"],
            [u"填充物", u"材质", u"fea"],
            [u"适用人群", u"款式", u"适用季节"],
            [u"fea", u"适用人数"],
            [u"材质", u"款式", u"fea"],
            [u"填充物", u"适用人群", u"面料", u"款式"],
            [u"材质", u"fea", u"图案", u"风格"],
        ]
    },   
    'mp_children_clothing': {
        'name': [
            [u"棉袄/棉服", u"羽绒服/羽绒内胆", u"羽绒服", u"羽绒连体衣", u"羽绒内胆", u"羽绒马甲"],
            [u"家居裤/睡裤", u"内裤", u"裤子"],
            [u"儿童冲锋裤", u"儿童速干裤", u"儿童滑雪裤", u"儿童软壳裤", u"儿童运动裤"],
            [u"羽绒裤", u"保暖裤"],
            [u"普通外套", u"西服/小西装", u"夹克/皮衣", u"呢大衣", u"风衣", u"皮草/仿皮草", u"套装", u"披风/斗篷",
             u"反穿衣/罩衣", u"家居袍/睡袍", u"浴袍"],
            [u"T恤", u"肚围/护脐带", u"衬衫", u"背心吊带", u"保暖上装", u"抹胸", u"连身衣/爬服/哈衣", u"马甲", u"旗袍",
             u"唐装", u"肚兜", u"家居服上装", u"肚兜", u"家居服连体衣", u"婴儿礼盒", u"亲子装/亲子时装",
             u"校服/校服定制", u"内衣套装", u"校服/校服定制", u"家居服套装", u"儿童演出服", u"儿童礼服",
             u"儿童运动套装"],
            [u"儿童抓绒衣", u"儿童软壳衣", u"儿童皮肤衣/防晒衣", u"儿童速干T恤", u"儿童速干衬衫", u"儿童滑雪服",
             u"儿童运动衣", u"儿童运动套装", u"儿童冲锋衣"],
            [u"卫衣/绒衫", u"毛衣/针织衫"],
            [u"家居裙/睡裙", u"连衣裙", u"半身裙"]
        ],
        'value': [
            [u"版型", u"廓型", u"衣门襟", u"图案", u"衣长", u"袖型", u"腰型", u"fun"],
            [u"版型", u"裤型", u"面料", u"裤长", u"腰型"],
            [u"版型", u"裤型", u"面料", u"裤长", u"腰型", u"fun"],
            [u"版型", u"裤型", u"面料", u"裤长", u"腰型", u"厚薄"],
            [u"版型", u"廓型", u"面料", u"图案", u"衣长", u"袖型", u"腰型"],
            [u"版型", u"廓型", u"面料", u"图案", u"风格", u"颜色"],
            [u"版型", u"廓型", u"面料", u"图案", u"风格", u"颜色", u"fun"],
            [u"版型", u"廓型", u"面料", u"图案", u"袖型", u"风格", u"颜色"],
            [u"版型", u"廓型", u"面料", u"图案", u"裙型", u"袖型"]
        ]
    },
    'mp_sports': {
        'name': [
            [u"连体泳衣", u"分体泳衣"],
            [u"男士泳衣"],
            [u"儿童泳衣/裤"],
            [u"泳镜"],
            [u"泳帽"]
        ],
        'value': [
            [u"女士泳衣类型", u"颜色", u"专业运动", u"适用年龄", u"fun"],  # [u"裤型",u"专业运动",u"女士泳衣类型",u"图案",u"材质",u"面料",u"颜色",u"fun"],
            [u"男士泳衣类型", u"颜色", u"专业运动", u"适用年龄", u"材质"],  # [u"男士泳衣类型",u"套餐",u"面料",u"材质",u"颜色",u"专业运动",u"fun"],
            [u"儿童泳衣类型", u"套餐", u"专业运动", u"图案", u"材质", u"面料", u"颜色", u"fun", u"适用年龄"],
            [u"泳镜类型", u"颜色", u"fun", u"适用性别", u"适用年龄", u"材质"],  # [u"泳镜类型",u"适用年龄",u"颜色",u"fun",u"专业运动",u"套餐"],
            [u"泳帽类型", u"颜色", u"适用性别", u"适用年龄", u"材质"],  # [u"泳帽类型",u"面料",u"适用年龄",u"专业运动",u"fun"]
        ]
    }
}
# endregion
