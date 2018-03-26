# -*- coding: utf-8 -*-
import redis


class RedisSet(object):
    def __init__(self):
        pass
    def redisSet(self):
        rconn = redis.Redis('192.168.20.151', 6379)
        # rconn = redis.Redis('10.4.255.129', 6379)
        return rconn


    def cld(self):
        sk = self.redisSet().smembers("crawlip")
        for kd in sk:
            self.redisSet().sadd("proxy:ip", kd)

# RedisSet().cld()
