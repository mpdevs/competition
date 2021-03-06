# coding: utf-8
# __author__: "John"
import numpy as np


def score_mean(df, subject_id):
    """
    先根据source_item和target_item进行聚合，取平均值
    :param df:
    :param subject_id:
    :return:
    """
    # print type(subject_id), subject_id
    todo = df.groupby([u"source_item", u"target_item"]).mean().reset_index()

    todo['Consistency'] = df.groupby([u"source_item", u"target_item"]).score.nunique().reset_index().iloc[:, -1]
    todo['Consistency'] = [1.0/3.0 if x == 2 else x for x in todo['Consistency']]
    todo['Consistency'] = [0 if x == 1 else x for x in todo['Consistency']]
    todo['Consistency'] = [1 if x == 3 else x for x in todo['Consistency']]

    # exclude item pairs without pictures
    print 'Number of instances without pics:', len(todo) - sum(todo['score'] >= 0)
    todo = todo[todo['score'] >= 0]
    # print todo['Consistency'].sum()
    ret = todo.values

    ret[:, 2:] = np.around(ret[:, 2:].astype(np.double), decimals=4)
    ret = np.concatenate((ret, np.asarray([[subject_id] * len(ret)]).T), axis=1).tolist()
    return [tuple(i) for i in ret]


if __name__ == u"__main__":
    from teacher_john import TeachTaggingClass as Teach
    t = Teach(u"mp_women_clothing")
    _subject = u"半身裙"
    _lesson = u"2label"
    _df = t.data[(t.data.subject == _subject) & (t.data.lesson == _lesson)]
    _students_count = len(_df.student.unique().tolist())
    score_mean(_df, _students_count)

