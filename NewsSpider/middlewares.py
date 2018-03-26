# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/spider-middleware.html
import random
import redis
from scrapy.downloadermiddlewares.retry import RetryMiddleware
from scrapy.exceptions import IgnoreRequest
from scrapy.utils.response import response_status_message
from NewsSpider.cookies import *
from user_agents import agents, newagents


class CookiesMiddleware(RetryMiddleware):
    """ 维护Cookie """

    def __init__(self, settings, crawler):
        RetryMiddleware.__init__(self, settings)
        self.rconn = settings.get("RCONN", redis.Redis(crawler.settings.get('REDIS_HOST', 'localhost'), crawler.settings.get('REDIS_PORT', 6379), 2))
        if crawler.spider.name not in ["SoGouSpider", "QxjSpider"]:
            # 由于微博的cookie是以spider.name管理的，这里为了统一，就都使用“SinaSpider”
            name = "SinaSpider"
            # name = crawler.spider.name
            initCookie(self.rconn, name)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings, crawler)

    def process_request(self, request, spider):
        if spider.name not in ["SoGouSpider", "QxjSpider"]:
            redisKeys = self.rconn.keys()
            while len(redisKeys) > 0:
                elem = random.choice(redisKeys)
                if "SinaSpider:Cookies" in elem:
                    cookie = json.loads(self.rconn.get(elem))
                    request.cookies = cookie
                    request.headers["User-Agent"] = random.choice(agents)
                    request.meta["accountText"] = elem.split("Cookies:")[-1]
                    break
                else:
                    redisKeys.remove(elem)
        elif spider.name == "SoGouSpider":
            request.headers["User-Agent"] = \
                "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.101 Safari/537.36"
            request.headers["Cookie"] = \
                "SUV=00244112655FBD2A5992879D6E263945; usid=ho72ZS9SHTIE-wYa; IPLOC=CN3100; CXID=4666E6F8540A10DBAF9E0CDFA365A72E; ad=SqR9yZllll2BuXxSlllllVubCOUlllllBqMd3kllllUlllllVA7ll5@@@@@@@@@@; SUID=2ABD5F655B68860A599A705100044E80; ABTEST=7|1505196809|v1; SNUID=D146A59FFBFFA30C0BDF1ABBFB69CB38; weixinIndexVisited=1; JSESSIONID=aaaycKgOj8JW_9FtVwr5v; sct=16"
        if spider.name == "QxjSpider":
            agent = random.choice(newagents)
            request.headers["User-Agent"] = agent

    def process_response(self, request, response, spider):
        if spider.name not in ["SoGouSpider", "QxjSpider"]:
            name = "SinaSpider"
            # name = spider.name
            if response.status in [300, 301, 302, 303]:
                try:
                    redirect_url = response.headers["location"]
                    if "login.weibo" in redirect_url or "login.sina" in redirect_url:  # Cookie失效
                        logger.warning("One Cookie need to be updating...")
                        updateCookie(request.meta['accountText'], self.rconn, name)
                    elif "weibo.cn/security" in redirect_url:  # 账号被限
                        logger.warning("One Account is locked! Remove it!")
                        removeCookie(request.meta["accountText"], self.rconn, name)
                    elif "weibo.cn/pub" in redirect_url:
                        logger.warning(
                            "Redirect to 'http://weibo.cn/pub'!( Account:%s )" % request.meta["accountText"].split("--")[0])
                    reason = response_status_message(response.status)
                    return self._retry(request, reason, spider) or response  # 重试
                except Exception, e:
                    raise IgnoreRequest
            elif response.status in [403, 414]:
                logger.error("%s! Stopping..." % response.status)
                os.system("pause")
            else:
                return response
        else:
            return response
