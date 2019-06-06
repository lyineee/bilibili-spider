import os
import sys
import time
import json
import logging

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
