# coding: utf-8
# __author__ = 'John'
PREDICT_PAIR_INFO_QUERY = u"""SELECT ItemID, TaggedItemAttr, ItemMainPicUrl, DiscountPrice, CategoryID, DateRange
 FROM {0} WHERE ItemID IN ({1}, {2}) AND DateRange = '{3}' AND TaggedItemAttr LIKE ',%';"""

# TRAIN_PAIR_INFO_QUERY = u"SELECT * FROM TaggedItemAttr WHERE ItemID IN ({0}, {1});"
TRAIN_PAIR_INFO_QUERY = u"""SELECT a.ItemID, a.TaggedItemAttr, CASE WHEN s.ItemMainPicUrl IS NULL THEN '' ELSE
s.ItemMainPicUrl END AS  ItemMainPicUrl FROM (SELECT * FROM TaggedItemAttr WHERE ItemID IN ({0}, {1})
) a LEFT JOIN (SELECT ItemID, ItemMainPicUrl FROM itemmonthlysales2015 WHERE DateRange = '2015-12-01' AND ItemID IN
({0}, {1})) s ON a.ItemID = s.ItemID;"""


GET_CATEGORY_ID_QUERY = u"SELECT CategoryID FROM {0} WHERE ItemID = {1} ORDER BY DateRange"

