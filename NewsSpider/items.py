# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy

class SoGouItem(scrapy.Item):
    title = scrapy.Field()
    pushtime = scrapy.Field()
    url = scrapy.Field()
    id = scrapy.Field()
    fileid = scrapy.Field()
    text = scrapy.Field()
    keyword = scrapy.Field()
    flag = scrapy.Field()


class SinaContentItem(scrapy.Field):
    pushTime = scrapy.Field()
    SinaOthID = scrapy.Field()
    SinaName = scrapy.Field()
    contentId = scrapy.Field()
    SinaID = scrapy.Field()
    Vermicelli = scrapy.Field()
    content = scrapy.Field()
    flag = scrapy.Field()

class SinaCommentItem(scrapy.Field):
    userName = scrapy.Field()
    contentId = scrapy.Field()
    userId = scrapy.Field()
    comment = scrapy.Field()
    commentId = scrapy.Field()
    pushTime = scrapy.Field()


class EastItem(scrapy.Item):
    web = scrapy.Field()
    url = scrapy.Field()
    title = scrapy.Field()
    keywords = scrapy.Field()
    datetime = scrapy.Field()
    description = scrapy.Field()
    content = scrapy.Field()

class SinaInformationItem(scrapy.Field):
    Province = scrapy.Field()
    Marriage = scrapy.Field()
    City = scrapy.Field()
    Num_Follows = scrapy.Field()
    SinaName = scrapy.Field()
    Gender = scrapy.Field()
    SinaId = scrapy.Field()
    userId = scrapy.Field()
    Num_Fans = scrapy.Field()
    Birthday = scrapy.Field()
    Signature = scrapy.Field()
    Num_Tweets = scrapy.Field()
    NickName = scrapy.Field()

class SinaTweetsItem(scrapy.Field):
    like = scrapy.Field()
    SinaName = scrapy.Field()
    contentId = scrapy.Field()
    tool = scrapy.Field()
    commentNum = scrapy.Field()
    pushTime = scrapy.Field()
    content = scrapy.Field()
    transfer = scrapy.Field()
    SinaId = scrapy.Field()