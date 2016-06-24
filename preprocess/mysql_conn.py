# coding=utf-8
'''
Created on Mar 23, 2016

@author: Jason Chu
'''
import MySQLdb
import traceback


class MySQLDB():
    def __init__(self, host='192.168.1.120', user='dev', passwd='Dev_123123', db='mp_portal', port=3306):
        self.HOST = host
        self.USER = user
        self.PASSWD = passwd
        self.DB = db
        self.PORT = port
        self.conn = None

    def connection(self):
        return MySQLdb.connect(host=self.HOST,
                               user=self.USER,
                               passwd=self.PASSWD,
                               db=self.DB,
                               port=self.PORT,
                               charset='utf8')

    def query(self, sql, dict_cursor=True, fetchone=False):
        conn = self.connection()
        if dict_cursor:
            cursor = conn.cursor(MySQLdb.cursors.DictCursor)
        else:
            cursor = conn.cursor()
        cursor.execute(sql)
        rows = None
        try:
            if fetchone:
                rows = cursor.fetchone()
            else:
                rows = cursor.fetchall()
        except:
            traceback.print_exc()
            return False
        else:
            return rows
        finally:
            cursor.close()
            conn.close()
        
    def execute(self, sql):
        conn = self.connection()
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            conn.commit()
        except:
            traceback.print_exc()
            return False
        else:
            return True
        finally:
            cursor.close()
            conn.close()
    
    def execute_many(self, query, args):
        conn = self.connection()
        cursor = conn.cursor()
        try:
            cursor.executemany(query, args)
            conn.commit()
        except:
            traceback.print_exc()
            return False
        else:
            return True
        finally:
            cursor.close()
            conn.close()

    def cursor(self):
        self.conn = self.connection()
        return self.conn

    def close(self):
        conn = self.conn
        conn.close()
        return


if __name__ == '__main__':
    db = MySQLDB(db='mp_women_clothing')
    c = db.query('SELECT now()')
    print c
    import pandas as pd
    training_data = pd.read_sql_query("Select ItemID, TaggedItemAttr "
                                      "FROM TaggedItemAttr "
                                      "WHERE TaggedItemAttr IS NOT NULL AND TaggedItemAttr != ''", db)
