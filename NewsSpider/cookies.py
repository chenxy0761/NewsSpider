# encoding=utf-8
# ------------------------------------------
#   版本：3.0
#   日期：2016-12-01
#   作者：九茶<http://blog.csdn.net/bone_ace>
# ------------------------------------------
import os
import json
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
import logging
import time
import random
import StringIO
from PIL import Image
from math import sqrt
from selenium import webdriver
from selenium.webdriver.remote.command import Command
from selenium.webdriver.common.action_chains import ActionChains
from sklearn.metrics import euclidean_distances
import numpy as np
from images import images


IDENTIFY = 1  # 验证码输入方式:
dcap = dict(DesiredCapabilities.PHANTOMJS)  # PhantomJS需要使用老版手机的user-agent，不然验证码会无法通过
dcap["phantomjs.page.settings.userAgent"] = (
    "Mozilla/5.0 (Linux; U; Android 2.3.6; en-us; Nexus S Build/GRK39F) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1"
)
logger = logging.getLogger(__name__)
logging.getLogger("selenium").setLevel(logging.WARNING)  # 将selenium的日志级别设成WARNING，太烦人


myWeiBo = [
    ('13467408430', 'aogan571'), ('13621812190', 'SWDGUOdong014'),
    ('sd4077431dicheng9@163.com', '3hlh1v'),
    ("13674136804", "ptxbgk4736m"), ("13869211734", "beimrv8758r"),
    ("13674131312", "twaeil5007u"), ("15241956054", "uycglp6237o"),
    ("13654048157", "cgkotw0672n"), ("15134092053", "dimqvy0002f"),
    ("13685343794", "ruybgk4235h"), ("15053480743", "pswafj4052e")
]


myWeiBos = [("tb_rush", "asd5588"),("usedark", "asd5588"),("ppa1028", "asd5588"),
           ("sx911107", "asd5588"), ("za000111", "asd5588"),("q306831386", "asd5588"),
           ("xun8772", "asd5588"),("nicol3040", "asd5588"),("ziyuxuan321", "asd5588"),
           ("v_carter", "asd5588"),("foxyiyi_baby", "asd5588"),("tzlijf", "asd5588")]



PIXELS = []

def getCookie(account, password):
    """ 获取一个账号的Cookie """
    try:
        browser = webdriver.PhantomJS()
        browser.set_window_size(1050, 840)
        browser.get('https://passport.weibo.cn/signin/login?entry=mweibo&r=http://weibo.cn/')
        time.sleep(1)
        name = browser.find_element_by_id('loginName')
        psw = browser.find_element_by_id('loginPassword')
        login = browser.find_element_by_id('loginAction')
        name.send_keys(account)  # 测试账号
        psw.send_keys(password)
        login.click()
        try:
            # ttype = getType(browser)  # 识别图形路径
            ttype = getType_similirity(browser)
            draw(browser, ttype)  # 滑动破解
            time.sleep(20)
            if u"我的首页" not in browser.title:
                time.sleep(4)
            if u'未激活微博' in browser.page_source:
                print u'账号未开通微博'
                return {}
        except Exception, e:
            print e
            pass
        cookie = {}
        if u"我的首页" in browser.title:
            for elem in browser.get_cookies():
                cookie[elem["name"]] = elem["value"]
            logger.warning("Get Cookie Success!( Account:%s )" % account)
        return json.dumps(cookie)
    except Exception, e:
        logger.warning("Failed account %s!" % account)
        logger.warning("Failed reason %s!" % e)
        return ""
    finally:
        try:
            browser.quit()
        except Exception, e:
            pass


def initCookie(rconn, spiderName):
    """ 获取所有账号的Cookies，存入Redis。如果Redis已有该账号的Cookie，则不再获取。 """
    for weibo in myWeiBo:
        if rconn.get("%s:Cookies:%s--%s" % (spiderName, weibo[0], weibo[1])) is None:  # 'SinaSpider:Cookies:账号--密码'，为None即不存在。
            cookie = getCookie(weibo[0], weibo[1])
            if len(cookie) > 0:
                rconn.set("%s:Cookies:%s--%s" % (spiderName, weibo[0], weibo[1]), cookie)
    cookieNum = "".join(rconn.keys()).count("SinaSpider:Cookies")
    logger.warning("The num of the cookies is %s" % cookieNum)
    if cookieNum == 0:
        logger.warning('Stopping...')
        os.system("pause")


def updateCookie(accountText, rconn, spiderName):
    """ 更新一个账号的Cookie """
    account = accountText.split("--")[0]
    password = accountText.split("--")[1]
    cookie = getCookie(account, password)
    if len(cookie) > 0:
        logger.warning("The cookie of %s has been updated successfully!" % account)
        rconn.set("%s:Cookies:%s" % (spiderName, accountText), cookie)
    else:
        logger.warning("The cookie of %s updated failed! Remove it!" % accountText)
        removeCookie(accountText, rconn, spiderName)


def removeCookie(accountText, rconn, spiderName):
    """ 删除某个账号的Cookie """
    rconn.delete("%s:Cookies:%s" % (spiderName, accountText))
    cookieNum = "".join(rconn.keys()).count("SinaSpider:Cookies")
    logger.warning("The num of the cookies left is %s" % cookieNum)
    if cookieNum == 0:
        logger.warning("Stopping...")
        os.system("pause")



def getExactly(im):
    """ 精确剪切"""
    imin = -1
    imax = -1
    jmin = -1
    jmax = -1
    row = im.size[0]
    col = im.size[1]
    for i in range(row):
        for j in range(col):
            if im.load()[i, j] != 255:
                imax = i
                break
        if imax == -1:
            imin = i

    for j in range(col):
        for i in range(row):
            if im.load()[i, j] != 255:
                jmax = j
                break
        if jmax == -1:
            jmin = j
    return (imin + 1, jmin + 1, imax + 1, jmax + 1)


def getType_similirity(browser):
    """ 识别图形路径 """
 
    time.sleep(3.5)
    im0 = Image.open(StringIO.StringIO(browser.get_screenshot_as_png()))
    box = browser.find_element_by_id('patternCaptchaHolder')
    im = im0.crop((int(box.location['x']) + 10, int(box.location['y']) + 100,
                   int(box.location['x']) + box.size['width'] - 10,
                   int(box.location['y']) + box.size['height'] - 10)).convert('L')
    newBox = getExactly(im)
    im = im.crop(newBox)
    data = list(im.getdata())
    data_vec = np.array(data)
    vectDict = {}
    for i, j in images.items():
        vect = euclidean_distances(data_vec, j)
        vectDict[i] = vect[0][0]
    sortDict = sorted(vectDict.iteritems(), key=lambda d: d[1], reverse=True)
    ttype = sortDict[-1][0]
    px0_x = box.location['x'] + 40 + newBox[0]
    px1_y = box.location['y'] + 130 + newBox[1]
    PIXELS.append((px0_x, px1_y))
    PIXELS.append((px0_x + 100, px1_y))
    PIXELS.append((px0_x, px1_y + 100))
    PIXELS.append((px0_x + 100, px1_y + 100))
    return ttype

def move(browser, coordinate, coordinate0):
    """ 从坐标coordinate0，移动到坐标coordinate """
    time.sleep(0.05)
    length = sqrt((coordinate[0] - coordinate0[0]) ** 2 + (coordinate[1] - coordinate0[1]) ** 2)  # 两点直线距离
    if length < 4:  # 如果两点之间距离小于4px，直接划过去
        ActionChains(browser).move_by_offset(coordinate[0] - coordinate0[0], coordinate[1] - coordinate0[1]).perform()
        return
    else:  # 递归，不断向着终点滑动
        step = random.randint(3, 5)
        x = int(step * (coordinate[0] - coordinate0[0]) / length)  # 按比例
        y = int(step * (coordinate[1] - coordinate0[1]) / length)
        ActionChains(browser).move_by_offset(x, y).perform()
        move(browser, coordinate, (coordinate0[0] + x, coordinate0[1] + y))


def draw(browser, ttype):
    """ 滑动 """
    if len(ttype) == 4:
        px0 = PIXELS[int(ttype[0]) - 1]
        login = browser.find_element_by_id('loginAction')
        ActionChains(browser).move_to_element(login).move_by_offset(px0[0] - login.location['x'] - int(login.size['width'] / 2), px0[1] - login.location['y'] - int(login.size['height'] / 2)).perform()
        browser.execute(Command.MOUSE_DOWN, {})

        px1 = PIXELS[int(ttype[1]) - 1]
        move(browser, (px1[0], px1[1]), px0)

        px2 = PIXELS[int(ttype[2]) - 1]
        move(browser, (px2[0], px2[1]), px1)

        px3 = PIXELS[int(ttype[3]) - 1]
        move(browser, (px3[0], px3[1]), px2)
        browser.execute(Command.MOUSE_UP, {})
    else:
        print 'Sorry! Failed! Maybe you need to update the code.'

