# coding: utf-8
# __author__: "John"
from db_apis import *
from helper import *
from datetime import datetime
import os
os.sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.db_apis import *


class TeachTaggingClass(object):
    """
    将人工标签转换成训练数据
    1. 读取人工标签数据
    2. 对人工标签
    """
    def __init__(self, industry):
        # columns=[u"subject", u"lesson", u"student", u"source_item", u"target_item", u"score"]
        self.data = get_assignment(industry)
        self.db = industry
        self.subject = self.data.subject.unique().tolist()
        self.subject_id = None
        self.category_dict = {row[2]: row[0] for row in get_categories(db=self.db, category_id_list=[])}
        self.todo_dict = dict()
        self.todo_df = None
        self.students = None
        for s in self.subject:
            temp_df = self.data[self.data.subject == s]
            for l in temp_df.lesson.unique().tolist():
                if s in self.todo_dict.keys():
                    self.todo_dict[s].append(l)
                else:
                    self.todo_dict[s] = [l]
        self.evaluations = None
    print (u"{0} John老师的课程初始化完毕".format(datetime.now()))

    def evaluate_assignment(self, df):
        """
        将同位标注成一致同位率，记分规则：
        :param df:
        :return:
        """
        self.students = len(df.student.unique().tolist())
        self.evaluations = score_mean(df=df[[u"source_item", u"target_item", u"score"]], subject_id=self.subject_id)
        return

    def delete_evaluations(self):
        delete_evaluations(db=self.db, category_id=self.subject_id)
        return

    def set_evaluations(self):
        set_evaluations(db=self.db, args=self.evaluations)
        return

    def main(self):
        print (u"{0} 开始评估所有的科目的作业".format(datetime.now()))
        for subject, lessons in self.todo_dict.iteritems():
            try:
                subject_id = self.category_dict[subject]
                self.subject_id = subject_id
                print u"{0} 正在评估科目<{1}>".format(datetime.now(), subject)
                self.delete_evaluations()
                for lesson in lessons:
                    self.todo_df = self.data[(self.data.subject == subject) & (self.data.lesson == lesson)]
                    self.evaluate_assignment(df=self.todo_df)
                    self.set_evaluations()
            except KeyError:
                print u"{0} 科目<{1}>没有找到category_id号，跳过".format(datetime.now(), subject)
                continue
        print (u"{0} 所有的科目的作业评估完毕".format(datetime.now()))
        return

if __name__ == u"__main__":
    tj = TeachTaggingClass(industry=u"mp_women_clothing")
    tj.main()




