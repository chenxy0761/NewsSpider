#! -*- coding:utf-8 -*-
import re
import time
from bs4 import BeautifulSoup
from scrapy import Request
from scrapy.spiders import CrawlSpider
from util.con_oracle import Dba
from NewsSpider.items import EastItem
import logging
from util.redisfile import RedisSet

logger = logging.getLogger(__name__)

class QxjSpider (CrawlSpider):

    name = "QxjSpider"

    def __init__(self, *a, **kw):

        self.rconn = RedisSet().redisSet()
        self.dba = Dba()
        self.keyword = {"新浪网": "Sina", "环球网": "Huanqiu", "搜狐网": "Sohu", "网易": "WangYi",
                        "凤凰网": "Ifeng", "新华网": "Xinhua",  "篱笆网": "Liba", "新民网": "Xinmin",
                        "看看新闻网": "KanKan", "中国天气网": "Weather", "东方网": "Eastday",
                        "人民网-上海": "People", "上海热线": "Online", "上观": "ShangGuan",
                        "上海新闻网": "ShangHaiNews",  "腾讯大申网": "Tencent", "宽带山": "KuanDai",
                        "中国广播网": "Radio"}
        self.current_date = time.strftime('%Y-%m-%d', time.localtime(time.time()))
        super(QxjSpider, self).__init__(*a, **kw)


    def start_requests(self):
        lists = self.dba.query("2018-02-01")
        for i in lists:
            try:
                htmlParse = self.parse_list()[self.keyword[i[0]]]
                data = {"msg": i, "htmlParse": htmlParse}
                yield Request(url=i[1], callback=self.parse, dont_filter=True, meta={"data": data})
            except Exception as e:
                logger.error("No definition of parsing rules for <<%s>> web" % e)


    def parse(self, response):
        soup = BeautifulSoup(response.body, "lxml")
        data = response.meta["data"]
        try:
            title = soup.find("title").get_text(strip=True)
        except:
            title = "Null"
        if title != "Null" or data["msg"][0] == "腾讯大申网":
            htmlParse = data["htmlParse"]
            try:
                try:
                    keywords = soup.find('meta', {"name": "keywords"})['content']
                except TypeError:
                    keywords = soup.find('meta', {"name": "Keywords"})['content']
            except:
                keywords = "Null"
            try:
                try:
                    description = soup.find('meta', {"name": "description"})['content']
                except TypeError:
                    description = soup.find('meta', {"name": "Description"})['content']
            except :
                description = "Null"
            lines = ""
            for parse in htmlParse:
                try:
                    zw = soup.find(parse[0], {parse[1]: parse[2]}).get_text(strip=True)
                    xx = u"([\u4e00-\u9fff]+)"
                    zws = re.findall(xx, zw)
                    for line in zws:
                        lines += line
                    break
                except Exception as e:
                    pass

            if len(lines) > 5:
                item = EastItem()
                msg = data["msg"]
                item["web"] = msg[0]
                item["url"] = msg[1]
                item["datetime"] = msg[1]
                item['title'] = title
                item['keywords'] = keywords
                item['description'] = description
                item['content'] = lines
                yield item

    def parse_list(self):
        # 定义解析规则
        htmlParse = {"Sina": [["div", "id", "artibody"], ["div", "id", "article"]],
                     "Huanqiu": [["div", "class", "text"], ["article", "class", "text"]],
                     "Liba": [["div", "class", "ui-topic-content fn-break", ], ["div", "class", "clearfix"]],
                     "Sohu": [["article", "class", "article"], ["div", "id", "main_content"]],
                     "Ifeng": [["div", "id", "artical_real"], ["div", "id", "picTxt"], ["div", "id", "yc_con_txt"]],
                     "Online": [["div", "class", "newsCon"], ["div", "id", "zoom"]],
                     "Tencent": [["div", "id", "Cnt-Main-Article-QQ"], ["div", "id", "contTxt"], ["div", "class", "article"], ],
                     "KanKan": [["div", "class", "textBody"]],
                     "WangYi": [["div", "class", "post_text"], ["div", "class", "viewport"], ["div", "id", "endText"]],
                     "Eastday": [["div", "id", "zw"], ["div", "class", "main"], ["div", "class", "zw"],
                                 ["div", "class", "article-content"], ["div", "class", "newsContent"]],
                     "Xinhua": [["div", "id", "p-detail"], ["div", "id", "article"], ["div", "id", "content"]],
                     "People": [["div", "class", "box_con"], ["div", "class", "clearfix"]],
                     "Xinmin": [["div", "class", "a_p"], ["article", "class", "padding15 content"],["div", "id", "MP_article"]],
                     "Weather": [["div", "class", "xyn-text"]],
                     "ShangGuan": [["div", "id", "newscontents"]],
                     "ShangHaiNews": [["div", "class", "cms-news-article-content-block"]],
                     "KuanDai": [["div", "class", "reply_message"]],
                     "Radio": [["div", "class", "TRS_Editor"]]
                     }
        return htmlParse

