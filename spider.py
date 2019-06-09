import os
import sys
import time
import json
import logging
import json
import traceback

from pymongo import MongoClient

from lxml import etree
from grab import Grab
from grab.spider import Spider, Task
# g = Grab(transport='urllib3')
# g.go('www.baidu.com')


class BiliSpider(Spider):
    dataTemp = []

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36'}

    dataKeys = ['title', 'likes', 'zone',
                'author', 'description', 'view', 'danmaku']

    def prepare(self):
        # basic variable
        self.baseUrl = 'http://www.bilibili.com/video/av{vid}'
        # can't use the api method to get the imformation
        # self.baseApiUrl='https://api.bilibili.com/x/web-interface/view?aid={vid}'
        self.baseApiUrl = 'http://api.bilibili.com/archive_stat/stat?aid={vid}'
        self.successCount = 0
        self.startVID = 230000
        # self.dataFile = open('data.json', 'w+', encoding='utf-8')

        # get the latest vid
        g = Grab()
        resp = g.go('https://www.bilibili.com/newlist.html')
        self.latestVid = int(resp.grab.xpath(
            '/html/body/div[3]/div/div[2]/ul/li[1]/a[3]').attrib['href'][9:-1])

        # mongodb operate class
        self.mongodb = MongoDB('localhost', 27070, 'bili_spider')

        # loading imformation
        config = self.mongodb.read_conf(self.latestVid)
        self.startVID = int(config['firstVid'])

        logging.info('prepare done')

    def task_generator(self):
        logging.info('generate start')
        logging.info('start at av{sid}, end at {eid}'.format(
            sid=self.startVID, eid=self.latestVid))

        for vid in range(self.startVID, self.startVID+100000):
            yield Task('get_data', url=self.baseUrl.format(vid=vid), vid=vid)

    def task_initial(self, grab, task):
        pass

    def task_get_data(self, grab, task):
        logging.info('get the web page of av{vid}'.format(vid=task.vid))

        data = {}

        try:
            if grab.xpath_exists('//*[@id="app"]/div/div/div[1]/div/div[2]/div[1]'):
                yield Task('no_video', grab=grab, vid=task.vid)
            else:
                logging.info(
                    'successfully get the page av{vid}'.format(vid=task.vid))
                data['vid'] = task.vid

                # get the data
                data['title'] = grab.xpath('//*[@id="viewbox_report"]/h1/span')
                # data['likes'] = grab.xpath(
                #     '//*[@id="arc_toolbar_report"]/div[1]/span[1]')
                data['zone'] = grab.xpath(
                    '//*[@id="viewbox_report"]/div[1]/span[1]/a[1]')
                data['author'] = grab.xpath(
                    '//*[@id="v_upinfo"]/div[2]/div[1]/a[1]')
                data['description'] = grab.xpath('//*[@id="v_desc"]/div')

                # TODO-clean the data
                data['title'] = data['title'].text
                # data['likes'] = data['likes'].attrib['title'][4:]
                data['zone'] = data['zone'].text
                data['uid'] = data['author'].attrib['href'][21:]
                data['author'] = data['author'].text
                data['description'] = data['description'].text

                yield Task('get_view', url=self.baseApiUrl.format(vid=task.vid), vid=task.vid, data=data)
        except:
            print(task.vid)
            traceback.print_exc()
            logging.warning('av{vid} fail to get data,maybe the video is gone'.format(
                vid=task.vid))
        pass

    def task_get_data_fallback(self,task):
        logging.error('video got fail at {vid}'.format(vid=task.vid))
        yield Task('get_data', url=task.url,
            task_try_count=task.task_try_count + 1)

    def task_get_view_fallback(self,task):
        logging.error('video got fail at {vid}'.format(vid=task.vid))
        yield Task('get_view', url=task.url,
            task_try_count=task.task_try_count + 1)

    def task_get_view(self, grab, task):
        jsonData = grab.doc.json
        data = task.data

        data['view'] = jsonData['data']['view']
        data['danmaku'] = jsonData['data']['danmaku']
        data['like'] = jsonData['data']['like']

        self.dataTemp.append(data)
        # print(len(self.dataTemp))

        if len(self.dataTemp) > 10:
            yield Task('save_to_db', grab=grab, data=data)

    def task_save_to_db(self, grab, task):
        for i in self.dataTemp:
            self.successCount += 1
            self.mongodb.insert(i, 'bili_1')
        self.dataTemp = []

    def update_grab_instance(self, grab):
        grab.setup(headers=self.headers, proxy=self.get_proxy(), timeout=4)

    def task_no_video(self, grab, task):
        logging.warning('av{vid} not found'.format(vid=task.vid))
        pass

    # def vid_generator():
    #     # print('in function vid gen')
    #     pass

    def get_proxy(self):
        g = Grab()
        proxyResp = g.go('localhost:5010/get/')
        return str(proxyResp.body.decode())

    def shutdown(self):
        # insert all the data in buffer
        self.task_save_to_db(-1, -1)
        logging.info('total video:{tvid}, successful count{svid}'.format(
            tvid=self.latestVid-self.startVID, svid=self.successCount))


class MongoDB:

    def __init__(self, ip, port, database):
        client = MongoClient(ip, port)
        self.db = client[database]
        self.confKeys = ['firstVid']
        self.cacheKeys = ['presentVid']

    def insert(self, data, collection):
        # TODO add data to collection
        # self.db.collection.insert_one(data)
        try:
            self.db[collection].insert_one(data)
        except:
            pass

    def read_conf(self, vid=-1):
        # read the global configuration
        # TODO add more configuration

        configCollection = self.db.config
        config = dict.fromkeys(self.confKeys, -1)
        try:
            for key in config.keys():
                config[key] = configCollection.find_one()[key]
        except:
            traceback.print_exc()
            logging.critical('error when read config, rewrite config')
            self.db.config.insert_one({'firstVid': vid})
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

    def insert_crawl_data():
        '''
        insert all the page be crawl
        '''


    def test(self):
        post = {'presentVid': 23333}
        self.writeCache(post)
        data = self.readCache()
        print(data)


def init_log():

    # TODO-debug log will be delete
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
    #i think 8 is a good number?
    mySpider = BiliSpider(thread_number=12)
    mySpider.run()


#     data={}
#     headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36'
# }
#     g=Grab()
#     g.setup(headers=headers)
#     resp=g.go('www.bilibili.com/video/av230000')
#     grab=resp.grab
#     data['title'] = grab.xpath('//*[@id="viewbox_report"]/h1/span')
#     data['likes'] = grab.xpath(
#         '//*[@id="arc_toolbar_report"]/div[1]/span[1]')
#     data['zone'] = grab.xpath(
#         '//*[@id="viewbox_report"]/div[1]/span[1]/a[1]')
#     data['author'] = grab.xpath(
#         '//*[@id="v_upinfo"]/div[2]/div[1]/a[1]')
#     data['description'] = grab.xpath('//*[@id="v_desc"]/div')


#     data['title'] = data['title'].text
#     data['likes'] = data['likes'].attrib['title'][4:]
#     data['zone'] = data['zone'].text
#     data['uid'] = data['author'].attrib['href'][21:]
#     data['author'] = data['author'].text
#     data['description'] = data['description'].text

#     print(data['title'])
#     print(data['likes'])
#     print(zone)
#     print(uid)
#     print(author)
#     # print(danmaku)
#     # print(view)
#     print(description)

    # mogo=MongoDB('localhost',27070,'newdb')
    # mogo.test()
