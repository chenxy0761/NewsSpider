#! -*- coding:utf-8 -*-
import re
from bs4 import BeautifulSoup
from scrapy import Request, Selector
from scrapy.spiders import CrawlSpider

from NewsSpider.items import SinaInformationItem, SinaTweetsItem
from util.redisfile import RedisSet
from util.time_stramp import Time_stamp


class SinaMsgSpider(CrawlSpider):
    name = "SinaMsgSpider"
    host = "https://weibo.cn"

    def __init__(self, *a, **kw):
        super(SinaMsgSpider, self).__init__(*a, **kw)
        self.rconn = RedisSet().redisSet()



    def start_requests(self):
        for lines in self.rconn.smembers("Tweets:sina"):
            sinaId = lines.split("--")[2]
            req_url = self.host + "/" + sinaId
            data = {"key": lines, "page": 1}
            yield Request(url=req_url, callback=self.parse, meta={"data": data})
            fans_url = self.host + "/" + sinaId + "/fans"
            yield Request(url=fans_url, callback=self.parse_fans, meta={"data": data})

    def parse_fans(self, response):
        data = response.meta["data"]
        hxs = Selector(response)
        div = hxs.xpath('body/div[@class="c"]')
        trs = div.xpath("//tr/td[2]/a[2]/@href").extract()
        keys = data["key"].split("--")
        for d in trs:
            try:
                userId = d.split("uid=")[1].split("&rl=")[0]
                info_url = self.host + "/" + userId + "/info"
                detail = {}
                detail["userId"] = userId
                detail["SinaName"] = keys[0]
                detail["SinaId"] = keys[2]
                yield Request(url=info_url, callback=self.parse_info, meta={"data": detail}, dont_filter=True)
            except:
                pass
        next_url = hxs.xpath('//a[text()="下页"]/@href'.decode('utf8')).extract()
        if next_url:
            yield Request(url=self.host + next_url[0], callback=self.parse_fans, meta={"data": data}, dont_filter=True)

    def parse_info(self, response):
        hxs = Selector(response)
        data = response.meta["data"]
        div = ";".join(hxs.xpath('//div[@class="c"][3]/text()').extract()) + ";"
        NickName = re.findall('昵称[：:]?(.*?);'.decode('utf8'), div)
        Birthday = re.findall('生日[：:]?(.*?);'.decode('utf8'), div)
        Gender = re.findall('性别[：:]?(.*?);'.decode('utf8'), div)
        Marriage = re.findall('感情状况[：:]?(.*?);'.decode('utf8'), div)
        Province = re.findall('地区[：:]?(.*?);'.decode('utf8'), div)
        Signature = re.findall('简介[：:]?(.*?);'.decode('utf8'), div)
        if NickName and NickName[0]:
            data['NickName'] = NickName[0]
        else:
            data['NickName'] = "Null"
        if Marriage and Marriage[0]:
            data['Marriage'] = Marriage[0]
        else:
            data['Marriage'] = "Null"
        if Birthday and Birthday[0]:
            data['Birthday'] = Birthday[0]
        else:
            data['Birthday'] = "Null"
        if Gender and Gender[0]:
            data['Gender'] = Gender[0]
        else:
            data['Gender'] = "Null"
        if Province and Province[0]:
            dou = Province[0].split(" ")
            if len(dou) == 2:
                data['Province'] = dou[0]
                data['City'] = dou[1]
            else:
                data['Province'] = dou[0]
                data['City'] = "Null"
        else:
            data['Province'] = "Null"
            data['City'] = "Null"
        if Signature and Signature[0]:
            data['Signature'] = Signature[0]
        else:
            data['Signature'] = "Null"
        req_url = "https://weibo.cn/attgroup/opening?uid=" + data["userId"]
        yield Request(url=req_url, callback=self.parse_page, meta={"data": data})

    def parse_page(self, response):
        hxs = Selector(response)
        data = response.meta["data"]
        msgs = ";".join(hxs.xpath('//div[@class="tip2"]/a/text()').extract()) + ";"
        Num_Fans = re.findall('微博\[(\d+)\]'.decode('utf8'), msgs)
        Num_Follows = re.findall('关注\[(\d+)\]'.decode('utf8'), msgs)
        Num_Tweets = re.findall('粉丝\[(\d+)\]'.decode('utf8'), msgs)
        if Num_Fans and Num_Fans[0]:
            data["Num_Fans"] = Num_Fans[0]
        else:
            data["Num_Fans"] = "Null"
        if Num_Follows and Num_Follows[0]:
            data["Num_Follows"] = Num_Follows[0]
        else:
            data["Num_Follows"] = "Null"
        if Num_Tweets and Num_Tweets[0]:
            data["Num_Tweets"] = Num_Tweets[0]
        else:
            data["Num_Tweets"] = "Null"
        item = SinaInformationItem()
        for key, val in data.items():
            item[key] = val
        yield item


    def parse(self, response):
        hxs = Selector(response)
        data = response.meta["data"]
        c = hxs.xpath('body/div[@class="c" and @id]')
        for div in c:
            try:
                like = re.findall('赞\[(\d+)\]'.decode('utf8'), div.extract())[0]  # 点赞数
                transfer = re.findall('转发\[(\d+)\]'.decode('utf8'), div.extract())[0]  # 转载数
                commentNum = re.findall('评论\[(\d+)\]'.decode('utf8'), div.extract())[0]  # 评论数
                contentId = div.xpath("@id").extract()[0].split('M_')[1]
                others = div.xpath('div/span[@class="ct"]/text()').extract()  # 求时间和使用工具（手机或平台）
                strs = others[0].split(u"来自")
                pushTime, flag = Time_stamp().time_handle(strs[0].strip())
                tool = strs[1]
                detail = {}
                detail["key"] = data["key"]
                comment_url = "https://weibo.cn/comment/%s" % contentId
                detail["contentId"] = contentId
                detail["pushTime"] = pushTime
                detail["commentNum"] = commentNum
                detail["transfer"] = transfer
                detail["like"] = like
                detail["tool"] = tool
                yield Request(url=comment_url, callback=self.parse_comment, meta={"data": detail}, dont_filter=True)
                # break
            except:
                pass
        url_next = hxs.xpath(
                'body/div[@class="pa" and @id="pagelist"]/form/div/a[text()="下页"]/@href'.decode('utf8')).extract()
        if url_next and data['page'] < 5:
            req_url = "https://weibo.cn%s" % url_next[0]
            data["page"] += 1
            yield Request(url=req_url, callback=self.parse, meta={"data": data}, dont_filter=True)


    def parse_comment(self, response):
        hxs = Selector(response)
        data = response.meta["data"]
        c = hxs.xpath('body/div[@class="c" and @id]')
        contentStr = c[0].xpath('div/span[@class="ctt"]//text()').extract()  # 微博内容
        content = ""
        for con in contentStr:
            content += con
        content.strip(":")
        data["content"] = content
        keys = data["key"].split("--")
        data["SinaName"] = keys[0]
        data["SinaId"] = keys[2]
        del data["key"]
        item = SinaTweetsItem()
        for key, val in data.items():
            item[key] = val
        yield item

