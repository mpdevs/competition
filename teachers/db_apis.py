# coding: utf-8
# __author__: "John"
from pandas import DataFrame
from os import path, walk, sys
from math import ceil
from datetime import datetime
from sql_constant import *
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from common.mysql_helper import connect_db, MySQLDBPackage as MySQL


def get_assignment(subject):
    """
    将行业作为学生的科目看待 ，品类当作章节看待
    :param subject: unicode
    :return:
    """
    assignments_place = path.join(path.join(path.dirname(__file__), u"assignments"), subject)

    answers = []
    unfinished_assignment = set()

    for place, chapters, assignments in walk(assignments_place):
        # print u"root={0},dirs={1},files={2}".format(place, chapter, assignments)
        for assignment in assignments:
            assignment = path.join(place, assignment)
            subject = path.basename(path.dirname(assignment))
            lesson, student = path.splitext(path.basename(assignment))[0].split(u"_")[:-1]
            with open(assignment) as content:
                for line in content:
                    try:
                        source_item, target_item, score = line.replace(u"\r", u"").replace(u"\n", u"").split(u"\t")
                        # 1，2算同位，3算异位
                        score = 1 if int(score) <= 2 else 0
                        answers.append((subject, lesson, student, source_item, target_item, score))
                    except ValueError:
                        unfinished_assignment.add(assignment)
    answers = DataFrame(data=answers,
                        columns=[u"subject", u"lesson", u"student", u"source_item", u"target_item", u"score"])
    print u"{0} 以下文件没有完成打标签的工作".format(datetime.now())
    for i in unfinished_assignment:
        print i
    return answers


def set_evaluations(db, args):
    """
    :param db:
    :param args:
    :return:
    """
    db_connection = MySQL()
    row_count = len(args)
    batch = int(ceil(float(row_count) / 100))
    for size in range(batch):
        start_index = size * 100
        end_index = min((size + 1) * 100, row_count)
        data = args[start_index: end_index]
        db_connection.execute_many(sql=SET_EVALUATIONS_QUERY.format(db), args=data)
    return


def delete_evaluations(db, category_id):
    db_connection = MySQL()
    db_connection.execute(DELETE_EVALUATIONS_QUERY.format(db, category_id))
    return

if __name__ == u"__main__":
    df = get_assignment(u"mp_women_clothing")
    # print df.student.unique().tolist()
    print df.score.unique().tolist()
    # a = [u"{0}:{1}".format(s, l) for s in df[u"subject"].unique().tolist() for l in df[u"lesson"].unique().tolist()]
    # for k in a:
    #     print k
    subject = u"休闲裤女"
    lesson = u"10label"
    for item in df[(df.lesson == lesson) & (df.subject == subject) ].student.unique().tolist():
        print item
    print u"_" * 100
    for item in df[(df.subject == subject)].student.unique().tolist():
        print item
    print u"_" * 100
    for item in df[(df.lesson == lesson)].student.unique().tolist():
        print item

