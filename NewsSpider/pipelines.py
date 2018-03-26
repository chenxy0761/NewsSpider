# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import re
import logging
import pymongo
import jieba.analyse
from util.con_oracle import Dba
from NewsSpider.WeatherModel import WeatherModel
from NewsSpider.items import SinaCommentItem, SinaContentItem, SoGouItem, \
    SinaInformationItem, SinaTweetsItem, EastItem

import sys
reload(sys)
sys.setdefaultencoding('utf8')

logger = logging.getLogger(__name__)

class NewsspiderPipeline(object):


    def __init__(self):
        # client = pymongo.MongoClient("192.168.20.216", 27017)
        # db = client["SinaWebchatWeather"]
        # self.sinaComment = db["SinaComment"]
        # self.sinaContent = db["SinaContent"]
        # self.sogou = db["SoGouContent"]
        # self.tweets = db["Tweets"]
        # self.Info = db["Information"]
        self.ora = Dba()
        self.path = "/data1/crawler/andycrawler/NewsSpider/weatheroutput/weatheroutput/"
        # self.path = "weatheroutput/weatheroutput/"
        self.keys = self.load_key()
        self.wm = WeatherModel(
            self.path + "LinearSVCl2.model",  self.path + "vectorizer.data",
            self.path + "ch2.data", self.path + "weatherwords.txt")
        self.tags = jieba.analyse.extract_tags
        self.ConSql = """
            INSERT INTO  QXJ.QXJ_YQ_WEIBO_DAY (sinaothid, sinaname, contentid,sinaid,vermicelli,content,flag,dta_date)
            VALUES('%s','%s','%s','%s','%s','%s','%s', to_date('%s','yyyy-mm-dd hh24:mi:ss')) """
        self.old_key_sql = """
            insert into qxj.qxj_yq_weibo_keyword_day (weibo_id, contentid, keyword, num, ts, dta_date)
            VALUES('%s','%s','%s','%d', to_date('%s','yyyy-mm-dd hh24:mi:ss'), date'%s')"""
        self.all_key_sql = """
            insert into qxj.qxj_keyword_all_day (keyword, type, dta_date) VALUES ('%s','%s',date'%s')"""
        self.CommSql = """
            INSERT INTO  QXJ.QXJ_YQ_PINGLUN_DAY (username, contentid, userid,comments,commentid,dta_date)
            VALUES('%s','%s','%s','%s','%s', to_date('%s','yyyy-mm-dd hh24:mi:ss'))"""
        self.SinComSql = """
            INSERT INTO  QXJ.QXJ_YQ_PINGLUN_DAY (username, contentid, userid,comments,commentid,dta_date)
                      VALUES('%s','%s','%s','%s','%s',DATE '%s')"""

    def process_item(self, item, spider):
        if isinstance(item, SinaContentItem):
            try:
                ConSql = self.ConSql % (
                    item["SinaOthID"],
                    item["SinaName"],
                    item["contentId"],
                    item["SinaID"],
                    item["Vermicelli"],
                    item["content"], str(item['flag']),
                    item["pushTime"].split(" ")[0]
                )
                self.ora.cux_sql(self.ora.connect(), ConSql)
            except Exception, e:
                logger.error("ConSql: <<%s>>" % e)


            sent = self.format_string(item["content"])
            dta_date = item["pushTime"].split(" ")[0]
            try:
                item_key = {}
                for key in self.keys:
                    num = sent.count(key)
                    if num != 0:
                        item_key[key] = num
                key_tup = zip(item_key.values(), item_key.keys())
                key_sor = sorted(key_tup, reverse=True)
                for sor in key_sor[:20]:
                    old_sql = self.old_key_sql % (
                        item["SinaID"],
                        item["contentId"],
                        sor[1], sor[0],
                        item["pushTime"], dta_date
                    )
                    self.ora.cux_sql(self.ora.connect(), old_sql)
            except Exception, e:
                logger.error("old_sql: <<%s>>" % e)


            """提取关键字并存入oracle"""
            self.find_new_keyword(sent, dta_date)

        if isinstance(item, EastItem):
            try:
                flag = int(self.wm.predict(item["content"])[0])
                # if flag != 1:
                #     num = 0
                #     for key in self.keys:
                #         if key in item["content"]:
                #             num += 1
                #         if num > 2:
                #             flag = 1
                #             break
                if flag == 1:
                    url = item["url"]
                    web = item["web"]
                    self.ora.update(flag, web, url)
                    """ 提取关键字并存入oracle """
                    sent = self.format_string(item["content"])
                    self.find_new_keyword(sent, item["datetime"])
            except Exception, e:
                print e

        if isinstance(item, SinaCommentItem):
            try:
                sql = self.SinComSql % (
                          item["userName"],
                          item["contentId"],
                          item["userId"],
                          item["comment"],
                          item["commentId"],
                          item["pushTime"].split(" ")[0])
                self.ora.cux_sql(self.ora.connect(), sql)
            except Exception, e:
                print e

        if isinstance(item, SoGouItem):
            try:
                sql = """ insert into QXJ.QXJ_YQ_WEIXIN_DAY (title,url,id,fileid,text,keyword,dta_date) \
                   VALUES ('%s','%s','%s','%s','%s','%s',DATE '%s' )""" % (
                    item["title"],
                    item["url"],
                    item['id'],
                    item['fileid'],
                    item['text'],
                    item['keyword'],
                    item["pushtime"].split(" ")[0]
                )
                self.ora.cux_sql(self.ora.connect(), sql)
            except Exception, e:
                logger.error("SoGouItem <<%s>>" % e)

        if isinstance(item, SinaInformationItem):
            try:
                self.Info.insert(dict(item))
            except Exception, e:
                logger.error("SinaInformationItem <<%s>>" % e)

        if isinstance(item, SinaTweetsItem):
			try:
				self.tweets.insert(dict(item))
			except Exception, e:
				logger.error("SinaTweetsItem <<%s>>" % e)


    def format_string(self, string):
        """ 去掉字母和数字 """
        p = re.compile(r"\w*", re.L)
        sent = p.sub("", string)
        return sent


    def find_new_keyword(self, sent, dta_date):
        """ 提取关键字并存入oracle """
        tags = self.tags(sentence=sent, topK=None, withWeight=True)
        for k, v in tags:
            all_sql = self.all_key_sql % (k, "sina", dta_date)
            try:
                self.ora.cux_sql(self.ora.connect(), all_sql)
            except Exception, e:
                pass

    def load_key(self):
        fd = open(self.path + "weatherwords.txt")
        keyword = fd.readlines()
        keys = []
        for key in keyword:
            key = key.replace("\n", "").replace("\r", "")
            if key != "":
                keys.append(key)
        return keys


