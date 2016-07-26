# coding: utf-8
# __author__ = u"John"


ITEMS_ATTR_DESC_QUERY = u"""SELECT ItemID, CategoryID ,REPLACE(ItemAttrDesc, '，', ',') AS Attribute
FROM {0} WHERE CategoryID = {1} AND ItemAttrDesc IS NOT NULL AND ItemAttrDesc != '';"""

ITEMS_ATTR_OTHER_QUERY = u"""SELECT ItemID, CategoryID ,
CONCAT_WS('', REPLACE(ShopName, '，', ','), REPLACE(ItemSubTitle, '，', ','), REPLACE(ItemName, '，', ',')) AS Attribute
FROM {0} WHERE CategoryID = {1} AND (ItemAttrDesc IS NULL OR ItemAttrDesc = '');"""

BRAND_QUERY = u"""SELECT b.AttrBrandName, b.TitleBrandName FROM mp_portal.brand_dict b
JOIN mp_portal.industry i ON b.IndustryID = i.IndustryID WHERE i.DBName = '{0}';"""

