SET_EVALUATIONS_QUERY = u"""INSERT INTO {0}.tagged_competitive_items (SourceItemID, TargetItemID, Score, Consistency, CategoryID)
VALUES (%s, %s, %s, %s, %s);"""

DELETE_EVALUATIONS_QUERY = u"DELETE FROM {0}.tagged_competitive_items WHERE CategoryID = {1}"


