#! -*- coding:utf-8 -*-

import cx_Oracle as co
import os

# 设置编码，以防读出的中文乱码
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

class Dba(object):

    def __init__(self):
        pass

    def connect(self):
        # db = co.connect('system/oracle@192.168.20.216:1521/bigdb')
        # db = co.connect('system', 'oracle', '192.168.20.216:1521/bigdb')
        tns = co.makedsn('192.168.20.216', 1521, 'bigdb')
        db = co.connect('system', 'oracle', tns)
        return db

    def cursor(self):
        consor = self.connect().cursor()
        return consor

    def query(self, date):
        sql = """select DISTINCT type01,type03,dta_date from 
              qxj.qxj_info_news_list where flag=0 and dta_date > date'%s'""" % date
        cursor = self.cursor()
        cursor.execute(sql)
        data = cursor.fetchall()
        self.close()
        return data

    def query_data(self, sql):
        cursor = self.cursor()
        cursor.execute(sql)
        data = cursor.fetchall()
        self.close()
        return data
		
    def update(self, flag, type01, type03):
        sql = "update qxj.qxj_info_news_list set flag=%d WHERE type01 = '%s' AND type03 = '%s'" \
              % (flag, type01, type03)
        cursor = self.cursor()
        cursor.execute(sql)
        cursor.connection.commit()

    def cux_sql(self, db, sql):
        ##插入，更新，删除
        cursor = db.cursor()
        cursor.execute(sql)
        cursor.close()
        db.commit()

    def close(self):
        self.cursor().connection.commit()
        self.cursor().close()
        self.connect().close()

if __name__ == "__main__":
    # print len(Dba().query("2017-10-01"))
    # for i in Dba().query("2017-10-01"):
    #     print i[0], i[1]
    db = Dba()
    # print len(db.query())
    # sql = "select dta_date,COUNT (type03) from qxj.qxj_info_news_list where flag = 1 AND dta_date >= date'2017-12-01' GROUP BY dta_date"
    # sql = "select dta_date,COUNT (contentid) from QXJ.QXJ_YQ_WEIBO_DAY where flag = 1 AND dta_date >= date'2017-12-01' GROUP BY dta_date"
    sql = "select dta_date,count(DISTINCT type03)  from qxj.qxj_info_news_list where flag = 1 GROUP BY dta_date"
    cursor = db.cursor()
    cursor.execute(sql)
    for i in cursor.fetchall():
        print(i[0], i[1])


    # sql = "select * from  qxj.qxj_keyword_all_day"
    # cursor = db.cursor()
    # cursor.execute(sql)
    # for i in cursor.fetchall():
    #     print i

