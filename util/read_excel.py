#! coding:utf-8


import xlrd
import redis

class ReadFile(object):
    def __init__(self):
        # self.redis = redis.Redis("192.168.20.151", 6379)
        self.redis = redis.Redis("10.4.255.129", 6379)

    def open_excel(self, file):
        try:
            data = xlrd.open_workbook(file)
            return data
        except Exception as e:
            print(str(e))

    def sogou(self, file=u'气象类微博&公众号-1113.xlsx', colnameindex=0,
                           by_name=u'Sheet1'):
        data = self.open_excel(file)
        table = data.sheet_by_name(by_name)
        nrows = table.nrows  # 行数
        keylist = []
        for rownum in range(0, nrows)[1:-5]:
            row = table.row_values(rownum)
            if row:
                line = ""
                for index in range(7, 11):
                    if row[index]:
                        if type(row[index]) == float:
                            row[index] = str(int(row[index]))
                    else:
                        row[index] = "Null"
                    line += row[index] + "--"
                lines = line.strip("--")
                self.redis.sadd("News:sogou", lines)

    def sina(self, file=u'气象类微博&公众号-1113.xlsx', colnameindex=0,
                           by_name=u'Sheet1'):
        data = self.open_excel(file)
        table = data.sheet_by_name(by_name)
        nrows = table.nrows  # 行数
        keylist = []
        for rownum in range(0, nrows)[1:]:
            row = table.row_values(rownum)
            if row:
                line = ""
                for index in range(0, 4):
                    if row[index]:
                        if type(row[index]) == float:
                            row[index] = str(int(row[index]))
                    else:
                        row[index] = "Null"
                    line += row[index] + "--"
                lines = line.strip("--")
                self.redis.sadd("News:sina", lines)

    def Tweets(self, file=u'副本微博公众号.xlsx', colnameindex=0,
                           by_name=u'公众号'):
        self.redis.delete("Tweets:sina")
        data = self.open_excel(file)
        table = data.sheet_by_name(by_name)
        nrows = table.nrows  # 行数
        keylist = []
        for rownum in range(0, nrows)[1:]:
            row = table.row_values(rownum)
            if row:
                line = row[0] + "--" + row[1] + "--" + str(int(row[2]))
                line = line.replace(".com", ".cn")
                # lines = line.strip("--")
                self.redis.sadd("Tweets:sina", line)


ReadFile().Tweets()
