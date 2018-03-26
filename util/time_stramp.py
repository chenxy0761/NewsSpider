#! -*- coding:utf-8 -*-
import datetime
import re
import time

class Time_stamp(object):

    def __init__(self):
        self.format = '%Y-%m-%d %H:%M:%S'

    def timestamp_datetime(self, value):
        value = time.localtime(value)
        dt = time.strftime(self.format, value)
        return dt

    def datetime_timestamp(self, dt):
        s = time.mktime(time.strptime(dt, self.format))
        return int(s)

    def flag_time(self, string, limitTime):
        if type(string) != int:
            d1 = self.datetime_timestamp(string)
            strTime = string
        else:
            d1 = string
            strTime = self.timestamp_datetime(string)
        current_time = time.strftime(self.format, time.localtime(time.time()))
        d2 = self.datetime_timestamp(current_time)
        hours = (d2 - d1) / 3600
        flag = 0
        if hours > 24 * limitTime:
            flag += 2
            #elif hours <24*0:
            #    flag += 3
        else:
            flag += 1
        return strTime, flag
    
    
    def time_handle(self, string, limitTime):
        current_time = time.strftime(self.format, time.localtime(time.time()))
        now_time = datetime.datetime.now()
        flag = 0
        if type(string) == int:
            return self.flag_time(string, limitTime)
        num_list = re.findall("\d+", string)
        if u"今天" in string:
            strTime = current_time.split(" ")[0] + " " + string.split(" ")[1] + ":00"
            flag += 1
            return strTime, flag
        elif u"月" and u"日" in string:
            if str(num_list[0]) in ['01', "02", "03"]:
                strTime = "2018-" + num_list[0] + "-" + num_list[1] + " " + \
                          num_list[2] + ":" + num_list[3] + ":00"
            else:
                strTime = "2017-" + num_list[0] + "-" + num_list[1] + " " + \
                          num_list[2] + ":" + num_list[3] + ":00"
            return self.flag_time(strTime, limitTime)
        elif u"分" in string:
            stamp = now_time - datetime.timedelta(seconds=int(num_list[0]) * 60)
            strTime = stamp.strftime(self.format)
            flag += 1
            return strTime, flag
        else:
            return self.flag_time(string, limitTime)


    def time_stamp(self, string):
        format1 = "%Y-%m-%d %H:%M:%S"
        format2 = "%Y-%m-%d"
        format3 = "%Y"
        now_time = datetime.datetime.now()
        num = re.findall('\d+', string)
        if "\xe5\x88\x86\xe9\x92\x9f" in string:
            time = now_time - datetime.timedelta(seconds=int(num[0])*60)
            stamp = time.strftime(format1)
        elif "\xe4\xbb\x8a\xe5\xa4\xa9" in string:
            ymd = now_time.strftime(format2)
            stamp = ymd + " " + num[0] + ":" + num[1] + ":00"
        elif "\xe6\x9c\x88" in string:
            y = now_time.strftime(format3)
            stamp = y + "-" + num[0] + "-" + num[1] + " " + num[2] + ":" + num[3] + ":00"
        else:
            stamp = string
        try:
            unixTime = self.datetime_timestamp(str(stamp))
            stoptime = self.datetime_timestamp('2017-01-01 00:00:00')
            if unixTime >= stoptime:
                flag = True
            else:
                flag = False
        except:
            print("dsdsddsdsdssdsdfsdfsdfsdfsdfsdfsdfsdfsdfsdfsdfsdfsdfsdfsdfsd", stamp)
            flag = False
        return stamp, flag
