import os
from lxml import etree
from grab import Grab
from grab.spider import Spider, Task
import time
import json
# g = Grab(transport='urllib3')
# g.go('www.baidu.com')


class BiliSpider(Spider):

    def prepare(self):
        self.baseUrl = 'http://www.bilibili.com/av{vid}'
        #can't use the api method to get the imformation
        # self.baseApiUrl='https://api.bilibili.com/x/web-interface/view?aid={vid}'
        self.count = 0
        self.startVID = 230000
        self.dataFile=open('data.json','w+',encoding='utf-8')
        print('prepare done')

    def task_generator(self):
        # print('generate start')
        pass

        for vid in range(self.startVID, self.startVID+20):
            # time.sleep(2)
            yield Task('get_title', url=self.baseUrl.format(vid=vid),vid=vid)

    def task_initial(self, grab, task):
        pass

    def task_get_title(self, grab, task):
        # print('in function get data')

        # html=etree.HTML(grab.text)
        try:
            title=grab.xpath('//*[@id="viewbox_report"]/h1/span').text
        except:
            print('av{vid} fail to get data,maybe the video is gone'.format(vid=task.vid))
        if title is not None:
            self.dataFile.write('av{vid}: {title}\n'.format(vid=task.vid,title=title))
            yield Task('end_func',grab=grab,msg=title)

    def task_end_func(self,grab,task):
        print(task.msg)

    def update_grab_instance(self, grab):
        # print('in function update')

        grab.setup(timeout=20)
        pass

    def vid_generator():
        # print('in function vid gen')
        pass

if __name__ == "__main__":
    mySpider=BiliSpider(thread_number=4)
    mySpider.run()