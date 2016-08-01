# coding: utf-8
# __author__: "John"
import MySQLdb
from datetime import datetime
from settings import HOST, USER, PASSWD, DB
from os import path, sys
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))


def connect_db(db=u"mp_portal"):
    connection = MySQLdb.Connect(host=HOST, user=USER, passwd=PASSWD, db=db, charset=u"utf8")
    return connection


def db_cursor(db=u"mp_portal"):
    cursor = MySQLdb.Connect(host=HOST, user=USER, passwd=PASSWD, db=db, charset=u"utf8").cursor()
    return cursor


class MySQLDBPackage(object):

    def __init__(self):
        self.HOST = HOST
        self.USER = USER
        self.PASSWD = PASSWD
        self.DB = DB
        self.PORT = 3306
        self.CHARSET = u"utf8"

    def query(self, sql, dict_cursor=False, fetchone=False):
        conn = MySQLdb.connect(host=self.HOST, user=self.USER, passwd=self.PASSWD, db=self.DB, port=self.PORT,
                               charset=self.CHARSET)
        if dict_cursor:
            cursor = conn.cursor(MySQLdb.cursors.DictCursor)
        else:
            cursor = conn.cursor()
        cursor.execute(sql)
        try:
            if fetchone:
                ret = cursor.fetchone()
            else:
                ret = cursor.fetchall()
        except Exception as e:
            print u"error message:{0}".format(e)
            return False
        else:
            return ret
        finally:
            cursor.close()
            conn.close()

    def execute(self, sql):
        conn = MySQLdb.connect(host=self.HOST, user=self.USER, passwd=self.PASSWD, db=self.DB, port=self.PORT,
                               charset=self.CHARSET)
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            conn.commit()
        except Exception as e:
            print u"error message:{0}".format(e)
            return False
        else:
            return True
        finally:
            cursor.close()
            conn.close()

    def execute_many(self, sql, args):
        conn = MySQLdb.connect(host=self.HOST, user=self.USER, passwd=self.PASSWD, db=self.DB, port=self.PORT,
                               charset=self.CHARSET)
        cursor = conn.cursor()
        try:
            cursor.executemany(sql, args)
            conn.commit()
        except Exception as e:
            print u"error message:{0}".format(e)
            return False
        else:
            return True
        finally:
            cursor.close()
            conn.close()


if __name__ == u"__main__":
    print u"{0} start testing MySQLDBPackage.query".format(datetime.now())
    dbi = MySQLDBPackage()
    result = dbi.query(sql=u"SELECT now();", fetchone=True)
    print u"{0} result={1}, type is {2}".format(datetime.now(), result, type(result))

    print u"{0} start testing MySQLDBPackage.execute".format(datetime.now())
    result = dbi.execute(sql=u"SELECT now();")
    print u"{0} result={1}".format(datetime.now(), result)

    print u"{0} start testing MySQLDBPackage.execute_many".format(datetime.now())
    arg_list = [(1, u""), (2, u""), (1, u""), (2, u"")]
    result = dbi.execute_many(sql=u"INSERT INTO mp_women_clothing.TaggedItemAttr VALUES(%s, %s);", args=arg_list)
    print u"{0} result={1}".format(datetime.now(), result)

    print u"{0} start testing db_cursor".format(datetime.now())
    db_cursor()

    print u"{0} start testing connect_db".format(datetime.now())
    from competition_finder.sql_constant import CATEGORY_QUERY
    import pandas as pd
    print CATEGORY_QUERY.format(u"mp_women_clothing", u"")
    connect_cursor = connect_db().cursor()
    categories = pd.read_sql_query(
        CATEGORY_QUERY.format(u"mp_women_clothing", u""), connect_db(u"mp_women_clothing"))
    print u"{0} len(items_attributes)={1}".format(datetime.now(), len(categories))



