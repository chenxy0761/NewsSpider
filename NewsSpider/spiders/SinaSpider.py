#! -*- coding:utf-8 -*-
import re
from bs4 import BeautifulSoup
from scrapy import Request, Selector
from scrapy.spiders import CrawlSpider
from NewsSpider.WeatherModel import WeatherModel
from NewsSpider.items import SinaContentItem, SinaCommentItem
from util.redisfile import RedisSet
from util.time_stramp import Time_stamp


class SinaSpider(CrawlSpider):
    name = "SinaSpider"
    rootUrl = "https://weibo.cn"

    def __init__(self, limitTime):
        self.limitTime = limitTime
        self.rconn = RedisSet().redisSet()
        self.path = "/data1/crawler/andycrawler/NewsSpider/weatheroutput/weatheroutput/"
        # self.path = "weatheroutput/weatheroutput/"
        self.words = self.load_keyword()
        self.wm = WeatherModel(
            self.path + "LinearSVCl2.model",  self.path + "vectorizer.data",
            self.path + "ch2.data", self.path + "keywords.txt"
        )
        super(SinaSpider, self).__init__(self.name)

    @classmethod
    def from_settings(cls, settings):
        return cls(settings.get('LIMIT_TIME'))

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = SinaSpider.from_settings(crawler.settings)
        spider._set_crawler(crawler)
        return spider

    def start_requests(self):
        url = "https://weibo.cn/%s"
        le = 1
        for key in self.rconn.smembers("News:sina"):
            req_url = url % key.split("--")[-2]
            data = {"key": key, "page": 1}
            yield Request(url=req_url, callback=self.parse, meta={"data": data}, dont_filter=True)
            if le < 1:
                break
            le += 1


    def parse(self, response):
        soup = BeautifulSoup(response.body, "lxml")
        data = response.meta["data"]
        flag_list = []
        for i in soup.find_all("div", class_="c")[1:-2]:
            strTime = i.find("span", class_="ct").get_text(strip=True).split(u" 来自")[0]
            pushTime, flag = Time_stamp().time_handle(strTime, self.limitTime)
            flag_list.append(flag)
            if flag == 1:
                content_id = i["id"].strip("M_")
                # self.rconn.delete("Sina:content_id")
                redis_flag = self.rconn.sadd("Sina:content_id", content_id)
                # redis_flag = 1
                if redis_flag == 1:
                    detail = {}
                    detail["key"] = data["key"]
                    comment_url = "https://weibo.cn/comment/%s" % content_id
                    detail["contentId"] = content_id
                    detail["pushTime"] = pushTime
                    yield Request(url=comment_url, callback=self.parse_comment, meta={"data": detail})
                    # break
        if 2 not in flag_list:
            hxs = Selector(response)
            url_next = hxs.xpath('body/div[@class="pa" and @id="pagelist"]/form/div/a[text()="下页"]/@href'.decode('utf8')).extract()[0]
            if url_next:
                req_url = "https://weibo.cn%s" % url_next
                yield Request(url=req_url, callback=self.parse, meta={"data": data})

    def parse_comment(self, response):
        data = response.meta["data"]
        hxs = Selector(response)
        if not data.has_key("page"):
            detail = {}
            detail["contentId"] = data["contentId"]
            detail["pushTime"] = data["pushTime"]
            keys = data["key"].split("--")
            detail["SinaName"] = keys[0]
            detail["Vermicelli"] = keys[1]
            detail["SinaID"] = keys[2]
            detail["SinaOthID"] = keys[2]
            contentStr = hxs.xpath('//div/span[@class="ctt"]//text()').extract()  # 微博内容
            content = ""
            for cd in contentStr:
                if len(cd) >= 3:
                    content += cd.replace(" ", "")
            detail["content"] = content
            flag = int(self.wm.predict(detail["content"])[0])
            if flag != 1:
                total = 0
                for word in self.words:
                    if word.strip() in detail["content"]:
                        total += 1
                        if total >= 2:
                            flag = 1
                            break
            if flag == 1:
                detail["flag"] = 1
                contentItem = SinaContentItem()
                for key, val in detail.items():
                    contentItem[key] = val
                yield contentItem
                c = hxs.xpath('body/div[@class="c" and @id]')[1:]
            else:
                c = []
        else:
            c = hxs.xpath('body/div[@class="c" and @id]')
        for div in c:
            comme = {}
            comme["contentId"] = data["contentId"]
            ID = div.xpath("a/@href").extract_first()
            userName = div.xpath("a//text()").extract_first()
            commentId = div.xpath("@id").extract()[0].split('C_')[1]
            try:
                userId = ID.split("u/")[1]
            except:
                userId = ID.split('/')[1]
            commentStr = div.xpath('span[@class="ctt"]//text()').extract()  # 微博内容
            comment = ""
            for co in commentStr:
                if len(co) >= 3:
                    comment += co.replace(" ", "")
            strTime = div.xpath('span[@class="ct"]//text()').extract()[0].split(u" 来自")[0]
            pushTime, flag = Time_stamp().time_handle(strTime,  self.limitTime)
            comme['pushTime'] = pushTime
            comme["userName"] = userName
            comme["commentId"] = commentId
            comme["userId"] = userId
            comme["comment"] = comment
            commentItem = SinaCommentItem()
            for key, val in comme.items():
                commentItem[key] = val
            yield commentItem
        url_next = hxs.xpath('body/div[@class="pa" and @id="pagelist"]/form/div/a[text()="下页"]/@href'.decode('utf8')).extract()
        if c != [] and url_next:
            data["page"] = True
            next_url = self.rootUrl + url_next[0]
            yield Request(url=next_url, callback=self.parse_comment, meta={"data": data}, dont_filter=True)

    def load_keyword(self):
        fs = open(self.path + "weatherwords.txt", "r")
        words = fs.readlines()
        return words

