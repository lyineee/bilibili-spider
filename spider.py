import os
import sys
import time
import json
import logging
from pymongo import MongoClient

from lxml import etree
from grab import Grab
from grab.spider import Spider, Task
# g = Grab(transport='urllib3')
# g.go('www.baidu.com')


class BiliSpider(Spider):

    def prepare(self):
        self.baseUrl = 'http://www.bilibili.com/av{vid}'
        # can't use the api method to get the imformation
        # self.baseApiUrl='https://api.bilibili.com/x/web-interface/view?aid={vid}'
        self.count = 0
        self.startVID = 230000
        self.dataFile = open('data.json', 'w+', encoding='utf-8')
        logging.info('prepare done')

    def task_generator(self):
        logging.info('generate start')
        pass

        for vid in range(self.startVID, self.startVID+20):
            # time.sleep(2)
            yield Task('get_title', url=self.baseUrl.format(vid=vid), vid=vid)

    def task_initial(self, grab, task):
        pass

    def task_get_title(self, grab, task):
        logging.info('get the web page of av{vid}'.format(vid=task.vid))
        # html=etree.HTML(grab.text)
        try:
            title = grab.xpath('//*[@id="viewbox_report"]/h1/span').text
        except:
            logging.warning('av{vid} fail to get data,maybe the video is gone'.format(
                vid=task.vid))
        else:
            if title is not None:
                logging.info(
                    'successfully got data from av{vid}'.format(vid=task.vid))

                self.dataFile.write('av{vid}: {title}\n'.format(
                    vid=task.vid, title=title))
                yield Task('end_func', grab=grab, msg=title)

    def task_end_func(self, grab, task):
        print(task.msg)

    def update_grab_instance(self, grab):
        grab.setup(timeout=20)
        pass

    # def vid_generator():
    #     # print('in function vid gen')
    #     pass


class MongoDB:

    def __init__(self, ip, port, database):
        client = MongoClient(ip, port)
        self.db = client[database]
        self.confKeys = ['firstVid', 'dataToCollect']
        self.cacheKeys = ['presentVid']

    def insert(self, data, collection):
        # TODO add data to collection
        # self.db.collection.insert_one(data)
        try:
            self.db[collection].insert_one(data)
        except:
            pass

    def get_conf(self):
        # read the global configuration
        # TODO add more configuration

        configCollection = self.db.config
        config = dict.fromkeys(self.confKeys, -1)
        try:
            for key in config.keys():
                config[key] = configCollection[key]
        except:
            logging.critical('error when read config')
            sys.exit(0)

        return config

    def readCache(self):
        # read the cache for spider
        # TODO add more cache item

        cache = dict.fromkeys(self.cacheKeys, -1)
        cacheCollection = self.db.cache
        try:
            for key in cache.keys():
                cache[key] = cacheCollection.find_one()[key]
        except:
            logging.warning(
                'error when read cache, spider may start at the beginning')

        return cache

    def writeCache(self, data):
        post = dict.fromkeys(self.cacheKeys, -1)
        try:
            for key in self.cacheKeys:
                post[key] = data[key]
        except:
            logging.warning('error when writting cache')
        else:
            self.db.cache.insert_one(post)

    def test(self):
        post={'presentVid':23333}
        self.writeCache(post)
        data=self.readCache()
        print(data)


def init_log():

    if not os.path.exists('./log'):
        try:
            os.mkdir('./log', 755)
        except:
            print('generate dir ./log fail, exit with 0')
            sys.exit(0)
        else:
            print('generate dir ./log')

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)    # Log等级总开关

    formatter = logging.Formatter(
        "%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")

    debugLogfile = './log/debug_log'
    fh = logging.FileHandler(debugLogfile, mode='w')
    fh.setLevel(logging.DEBUG)
    fh.addFilter(lambda record: record.msg != 'Deprecation Warning\n%s')
    fh.setFormatter(formatter)

    logfile = './log/log'
    lh = logging.FileHandler(logfile, mode='w')
    lh.setLevel(logging.INFO)
    # lh.addFilter(lambda record: record.msg != 'Deprecation Warning\n%s')
    lh.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(lh)


if __name__ == "__main__":
    init_log()
    mySpider = BiliSpider(thread_number=4)
    mySpider.run()

    # db=MongoDB('localhost',27070,'test')
    # db.test()
