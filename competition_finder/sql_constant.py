# coding: utf-8
# __author__ = "John"


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

CUSTOMER_ITEM_QUERY = u"""SELECT ItemID, TaggedItemAttr, DiscountPrice, CategoryID, DateRange
FROM {0} WHERE ShopID = {1} AND CategoryID = {2} AND DateRange = '{3}' AND TaggedItemAttr LIKE ',%';"""

COMPETITIVE_ITEM_QUERY = u"""SELECT ItemID, TaggedItemAttr, DiscountPrice, CategoryID, DateRange
FROM {0} WHERE (ShopID != {1} OR ShopID IS NULL) AND CategoryID = {2}
AND DateRange = '{3}' AND TaggedItemAttr LIKE ',%';"""

ATTRIBUTES_QUERY = u"""SELECT ItemID, TaggedItemAttr FROM {0} WHERE TaggedItemAttr IS NOT NULL AND TaggedItemAttr != ''
AND TaggedItemAttr LIKE ',%' {1};"""


