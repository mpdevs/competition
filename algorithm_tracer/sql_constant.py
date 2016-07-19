# coding: utf-8
# __author__ = 'John'
PREDICT_PAIR_INFO_QUERY = u"""SELECT ItemID, TaggedItemAttr, DiscountPrice, CategoryID, DateRange FROM {0}
WHERE ItemID IN ({1}, {2}) AND DateRange = '{3}' AND TaggedItemAttr LIKE ',%';"""

TRAIN_PAIR_INFO_QUERY = u"SELECT * FROM TaggedItemAttr WHERE ItemID IN ({0}, {1});"

GET_CATEGORY_ID_QUERY = u"SELECT CategoryID FROM {0} WHERE ItemID = {1} ORDER BY DateRange"

