# coding: utf-8
# __author__ = u"John"
from os import path, sys
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))


TAG_ATTR_META_QUERY = u"""SELECT a.CID, a.Attrname, a.DisplayName, a.AttrValue, a.Flag FROM mp_portal.attr_value AS a
JOIN mp_portal.industry AS i ON a.IndustryID = i.IndustryID WHERE a.IsTag='y' AND a.Flag='A' AND i.DBName ='{0}';"""


ITEMS_ATTR_DESC_QUERY = u"""SELECT ItemID, CategoryID ,REPLACE(ItemAttrDesc, '，', ',') AS Attribute
FROM {0}
WHERE CategoryID = {1} AND ItemAttrDesc IS NOT NULL AND ItemAttrDesc != '' AND ItemAttrDesc NOT LIKE '%???%';"""

ITEMS_ATTR_OTHER_QUERY = u"""SELECT ItemID, CategoryID ,
CONCAT_WS('', REPLACE(ShopName, '，', ','), REPLACE(ItemSubTitle, '，', ','), REPLACE(ItemName, '，', ',')) AS Attribute
FROM {0} WHERE CategoryID = {1} AND (ItemAttrDesc IS NULL OR ItemAttrDesc = '' OR ItemAttrDesc LIKE '%???%');"""

TAG_DICT_QUERY = u"""SELECT AttrName FROM mp_portal.attr_value WHERE CID = {0} AND IsTag = 'y'
UNION SELECT DisPlayName FROM mp_portal.attr_value WHERE CID = {0}  AND IsTag = 'y';"""

SET_ATTR_QUERY = u"UPDATE {0}.{1} SET {2} = %s WHERE ItemID = %s"

BRAND_QUERY = u"""SELECT b.AttrBrandName, CONCAT(b.AttrBrandName, ',', b.TitleBrandName) AS TitleBrandName
FROM mp_portal.brand_dict b JOIN mp_portal.industry i ON b.IndustryID = i.IndustryID WHERE i.DBName = '{0}';"""

COLOR_QUERY = u"SELECT ColorGroupName, ColorName, SimilarColor, BlurredColor FROM color_group_value;"

ITEM_ATTR_DESC_QUERY = u"""SELECT ItemID, CategoryID ,REPLACE(ItemAttrDesc, '，', ',') AS Attribute
FROM {0}
WHERE ItemID = {1} AND ItemAttrDesc IS NOT NULL AND ItemAttrDesc != '' AND ItemAttrDesc NOT LIKE '%???%';"""

ITEM_ATTR_OTHER_QUERY = u"""SELECT ItemID, CategoryID ,
CONCAT_WS('', REPLACE(ShopName, '，', ','), REPLACE(ItemSubTitle, '，', ','), REPLACE(ItemName, '，', ',')) AS Attribute
FROM {0} WHERE ItemID = {1} AND (ItemAttrDesc IS NULL OR ItemAttrDesc = '' OR ItemAttrDesc LIKE '%???%');"""

CATEGORY_BY_ITEM_QUERY = u"SELECT DISTINCT CategoryID FROM {0} WHERE ItemID = {1};"

