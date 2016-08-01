# coding: utf-8
# __author__ = u"John"


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

