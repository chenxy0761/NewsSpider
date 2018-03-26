#! -*- coding:utf-8 -*-
import re
import jieba.analyse
from util.con_oracle import Dba


class SpiltKeyWord(object):

    def __init__(self):
        self.dba = Dba()
        self.keys = self.load_key()
        self.tags = jieba.analyse.extract_tags

    def load_key(self):
        fd = open("../weatheroutput/weatheroutput/weatherwords.txt")
        keyword = fd.readlines()
        keys = []
        for key in keyword:
            key = key.replace("\n", "").replace("\r", "")
            if key != "":
                keys.append(key)
        return keys



    def query_data(self):

        sql = """select dta_date, contentid, sinaid, content from  QXJ.QXJ_YQ_WEIBO_DAY where flag= 1"""
        ins_sql = """insert into qxj.qxj_yq_weibo_keyword_day (weibo_id, contentid, keyword, num, ts, dta_date)
                VALUES('%s','%s','%s', '%d',to_date('%s','yyyy-mm-dd hh24:mi:ss'), date'%s')"""
        ins_all = """
            insert into qxj.qxj_keyword_all_day (keyword, type, dta_date) VALUES ('%s','%s',date'%s')        
        """
        data = self.dba.query_data(sql)
        for i in data:
            # i[3] = ":双休日本市天空云系较多，并伴有短时阵雨，最高气温在29-30℃。其中，明天（周六）上海以多云到阴为主，有分散性短时阵雨。偏东风3-4级。气温在24-29℃左右。下周本市处在副热带高压边缘，多阵雨或雷雨天气。下周初气温开始上升，最高气温将重回30℃以上，其中，周二的最高气温将上升至34℃上下。​​​嗯。。。美的悄无声息[呵呵]，不露痕迹!http://t.cn/RNX7sak"
            p = re.compile(r"\w*", re.L)
            sent = p.sub("", i[3])
            dta_date = str(i[0]).split(" ")[0]
            try:
                item = {}
                for key in self.keys:
                    num = sent.count(key)
                    if num != 0:
                        item[key] = num
                key_tup = zip(item.values(), item.keys())
                key_sor = sorted(key_tup, reverse=True)
                for sor in key_sor[:20]:
                    ins_sqlq = ins_sql % (i[2], i[1], sor[1], sor[0], i[0], dta_date)
                    self.dba.cux_sql(self.dba.connect(), ins_sqlq)
            except Exception as e:
                print("keywords:::", e)
            tags = self.tags(sentence=sent, topK=None, withWeight=True)
            for k, v in tags:
                ins_allq = ins_all % (k, "sina", dta_date)
                try:
                    self.dba.cux_sql(self.dba.connect(), ins_allq)
                except Exception as e:
                    print("Newword:::", e)
            break

if __name__ == "__main__":
    SpiltKeyWord().query_data()
    # SpiltKeyWord().load_key()