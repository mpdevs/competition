# coding: utf-8
import MySQLdb
from datetime import datetime
host = u"192.168.1.120"
user = u"dev"
pwd = u"Dev_123123"
db = u"mp_portal"


def connect_db(industry=u"mp_portal"):
    connection = MySQLdb.Connect(host=host, user=user, passwd=pwd, db=industry, charset=u"utf8")
    return connection


def db_cursor(industry=u"mp_portal"):
    cursor = MySQLdb.Connect(host=host, user=user, passwd=pwd, db=industry, charset=u"utf8").cursor()
    return cursor


class MySQLDBPackage(object):

    def __init__(self):
        global host
        global user
        global pwd
        global db
        self.HOST = host
        self.USER = user
        self.PASSWD = pwd
        self.DB = db
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
        conn = MySQLdb.connect(host=self.HOST, user=self.USER, passwd=self.PASSWD, db=self.DB, port=self.PORT)
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
        conn = MySQLdb.connect(host=self.HOST, user=self.USER, passwd=self.PASSWD, db=self.DB, port=self.PORT)
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
    db = MySQLDBPackage()
    result = db.query(sql=u"SELECT now();", fetchone=True)
    print u"{0} result={1}, type is {2}".format(datetime.now(), result, type(result))

    print u"{0} start testing MySQLDBPackage.execute".format(datetime.now())
    result = db.execute(sql=u"SELECT now();")
    print u"{0} result={1}".format(datetime.now(), result)

    print u"{0} start testing MySQLDBPackage.execute_many".format(datetime.now())
    arg_list = [(1, u""), (2, u""), (1, u""), (2, u"")]
    result = db.execute_many(sql=u"INSERT INTO mp_women_clothing.TaggedItemAttr VALUES(%s, %s);", args=arg_list)
    print u"{0} result={1}".format(datetime.now(), result)

    print u"{0} start testing db_cursor".format(datetime.now())
    db_cursor()

    print u"{0} start testing connect_db".format(datetime.now())
    from enums import ATTRIBUTES_QUERY
    import pandas as pd
    print (ATTRIBUTES_QUERY.format(u"TaggedItemAttr", u""))
    connect_cursor = connect_db().cursor()
    items_attributes = pd.read_sql_query(
        ATTRIBUTES_QUERY.format(u"TaggedItemAttr", u""), connect_db(u"mp_women_clothing"))
    print u"{0} len(items_attributes)={1}".format(datetime.now(), len(items_attributes))



