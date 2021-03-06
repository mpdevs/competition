# coding: utf-8
# __author__: "John"

"""
目前，标签元信息不能复用，打标签程序根据IsTag来筛选，竞品根据IsCalc来筛选
"""
ATTR_META_QUERY = u"""SELECT a.CID, a.Attrname, a.DisplayName, a.AttrValue, a.Flag FROM mp_portal.attr_value AS a
JOIN mp_portal.industry AS i ON a.IndustryID = i.IndustryID WHERE a.IsCalc='y' AND i.DBName ='{0}'"""


CATEGORY_QUERY = u"""SELECT DISTINCT c.CategoryID, c.CategoryName, c.CategoryDesc
FROM mp_portal.category c JOIN mp_portal.industry i ON c.IndustryID = i.IndustryID WHERE i.DBName = '{0}' {1}
AND c.IsBottom = 'y';"""


GET_DATE_RANGES_QUERY = u"SELECT CAST(DateRange AS CHAR) AS DateRange FROM (SELECT DISTINCT DateRange FROM {0})t;"

