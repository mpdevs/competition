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

ITEMS_ATTR_DESC_QUERY = u"""SELECT ItemID, CategoryID, concat_ws(' ',ItemSubTitle,ItemName) AS Title,
ItemAttrDesc AS Attribute, concat_ws(' ',ShopName,ItemSubTitle,ItemName) AS ShopNameTitle
FROM {0} WHERE CategoryID = {1};"""

CUSTOMER_ITEM_QUERY = u"""SELECT ItemID, TaggedItemAttr, DiscountPrice, CategoryID, DateRange
FROM {0} WHERE ShopID = {1} AND CategoryID = {2} AND DateRange = '{3}' AND TaggedItemAttr LIKE ',%';"""

COMPETITIVE_ITEM_QUERY = u"""SELECT ItemID, TaggedItemAttr, DiscountPrice, CategoryID, DateRange
FROM {0} WHERE (ShopID != {1} OR ShopID IS NULL) AND CategoryID = {2}
AND DateRange = '{3}' AND TaggedItemAttr LIKE ',%';"""

PREDICT_PAIR_INFO_QUERY = u"""SELECT ItemID, TaggedItemAttr, DiscountPrice, CategoryID, DateRange FROM {0}
WHERE ItemID IN ({1}, {2}) AND DateRange = '{3}' AND TaggedItemAttr LIKE ',%';"""

TRAIN_PAIR_INFO_QUERY = u"SELECT * FROM TaggedItemAttr WHERE ItemID IN ({0}, {1});"

GET_CATEGORY_ID_QUERY = u"SELECT CategoryID FROM {0} WHERE ItemID = {1} ORDER BY DateRange"

ATTRIBUTES_QUERY = u"""SELECT ItemID, TaggedItemAttr FROM {0} WHERE TaggedItemAttr IS NOT NULL AND TaggedItemAttr != ''
AND TaggedItemAttr LIKE ',%' {1};"""
