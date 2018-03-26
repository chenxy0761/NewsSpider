#! -*- coding: utf-8 -*-
import json
import re
import logging
from bs4 import BeautifulSoup
from scrapy import Request
from scrapy.spiders import CrawlSpider
from NewsSpider.items import SoGouItem
from util.redisfile import RedisSet
from util.time_stramp import Time_stamp

logger = logging.getLogger(__name__)

class SoGouSpider(CrawlSpider):
    name = "SoGouSpiderS"
    host = "http://mp.weixin.qq.com"

    def __init__(self, *a, **kw):
        super(SoGouSpider, self).__init__(*a, **kw)
        self.rconn = RedisSet().redisSet()

    def start_requests(self):
        url = "http://weixin.sogou.com/weixin?type=1&s_from=input&query=%s&ie=utf8&_sug_=n&_sug_type_="
        for key in self.rconn.smembers("News:sogou"):
            req_url = url % key.split("--")[-1]
            yield Request(url=req_url, callback=self.parse, meta={"key": key})

    def parse(self, response):
        soup = BeautifulSoup(response.body, "lxml")
        next_url = soup.find("p", class_="tit").find("a")["href"]
        yield Request(url=next_url, callback=self.parse_result, meta=response.meta)

    def parse_result(self, response):
        soup = BeautifulSoup(response.body, "lxml")
        data = response.meta
        try:
            scr = soup.find_all("script")[-2:-1][0].get_text(strip=True)
            data_list = scr.split("var msgList = ")[1].split('seajs.use("sougou/p')[0].strip().strip(";")
            j_data = json.loads(data_list)
            art_list = []
            for li in j_data["list"]:
                dic = {}
                dic["id"] = li["comm_msg_info"]["id"]
                dic["pushtime"] = li["comm_msg_info"]["datetime"]
                dic["title"] = li["app_msg_ext_info"]["title"]
                dic["fileid"] = li["app_msg_ext_info"]["fileid"]
                dic["url"] = self.host + li["app_msg_ext_info"]["content_url"].replace("amp;", "")
                art_list.append(dic)
                for ls in li["app_msg_ext_info"]["multi_app_msg_item_list"]:
                    dict_ls = {}
                    dict_ls["id"] = dic["id"]
                    dict_ls["pushtime"] = dic["pushtime"]
                    dict_ls["url"] = self.host + ls["content_url"].replace("amp;", "")
                    dict_ls["title"] = ls["title"]
                    dict_ls["fileid"] = ls["fileid"]
                    art_list.append(dict_ls)
            for ks in art_list:
                timeFormat, flag = Time_stamp().time_handle(ks["pushtime"])
                if flag == 1:
                    line = data["key"].split("--")[-1] + "--" + str(ks["pushtime"])
                    self.rconn.delete("SoGou:Account")
                    flag = self.rconn.sadd("SoGou:Account", line)
                    if flag == 1:
                        ks["pushtime"] = timeFormat
                        ks["keyword"] = data["key"]
                        data["ks"] = ks
                        yield Request(url=ks["url"], callback=self.parse_article, meta=data)
        except Exception, e:
            logger.error("parse_result error <<%s>>" % e)


    def parse_article(self, response):
        soup = BeautifulSoup(response.body, "lxml")
        text = soup.find("div", {"id": "js_content"}).get_text(strip=True)
        data = response.meta["ks"]
        item = SoGouItem()
        for key, value in data.items():
            item[key] = value
        xx = u"([\u4e00-\u9fff]+)"
        zws = re.findall(xx, text)
        texts = ""
        for line in zws:
            texts += line
        item["text"] = texts
        yield item



