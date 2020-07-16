from pymongo import MongoClient
import bson.regex
import re
import time
import numpy as np 
import pandas as pd
import jieba
# from pyecharts.render import make_snapshot
# 使用 snapshot-selenium 渲染图片


class BiliData:

    def __init__(self, ip, port):
        client = MongoClient(ip, port)
        self.db = client['bili_spider']
        self.data=self.db.bili_data
        self.zoneList=self.data.distinct('zone')

    def find(self,express):
        data=self.data.find(express)
        return data

    def aggregate(self,express):
        data=self.data.aggregate(express)
        return data

    def video_count_between(self,start,end):
        find={'pubtime':{'$gt':start,'$lt':end}}
        num=self.find(find).count()
        return num

    def add_data_between(self,start,end,key):
        total='total'+key
        key='$'+key
        match={'pubtime':{'$gt':start,'$lt':end}}
        group={'_id':'null',total:{'$sum':key}}
        data=self.aggregate([{'$match':match}, {'$group':group}])
        num=0
        for i in data:
            num=i[total]
        return num

    def view_change(self):
        data=[]
        date=[]
        for i in range(2010,2020):
            for j in range(1,12):
                d=0
                d=self.add_data_between(get_time(i,j),get_time(i,j+1),'view')
                data.append(d)
                date.append(get_time(i,j)[:-4])
        return data,date

    def video_change(self):
        data=[]
        date=[]
        for i in range(2010,2020):
            for j in range(1,12):
                d=0
                d=self.video_count_between(get_time(i,j),get_time(i,j+1))
                data.append(d)
                date.append(get_time(i,j)[:-4])
        return data,date

    def get_video_each_zone(self):
        data={}
        for key in self.zoneList:
            find={'zone':key}
            data[key]=self.find(find).count()
        return data

    def cpright_year(self):
        '''
        1-原创 2-转载 大约从12年6月开始有此标签
        '''
        count_all=self.find({'copyright':2}).count()
        count=self.data.count()
        r=count_all/count
        with open('./generate/copyright_all.txt','w') as f:
            f.write('origin: '+str(count_all)+'\n')
            f.write('total video: '+str(count)+'\n')
            f.write('percentage of origin: '+str(r)+'\n')
        data={}
        for i in range(2012,2020):
            for j in range(1,12):
                if i==2012 and j in [1,2,3,4,5]:
                    continue
                a=self.data.find({'pubtime':{'$gt':get_time(i,j),'$lt':get_time(i,j+1)},'copyright':2}).count()
                b=self.data.find({'pubtime':{'$gt':get_time(i,j),'$lt':get_time(i,j+1)}}).count()
                if a==0 and b==0:
                    continue
                data[get_time(i,j)[:-4]]=a/b
        df=pd.DataFrame.from_dict(data,orient='index')
        df.to_csv('copyright_percentage.csv')

    def average_each_zone(self):
        '''
        每个分区视频播放量与视频总数之比
        '''
        videoCount=self.get_video_each_zone()
        zoneView={}
        for key in self.zoneList:
            match={'zone':key}
            group={'_id':'null','total':{'$sum':'$view'}}
            data=self.aggregate([{'$match':match}, {'$group':group}])
            num=0
            for i in data:
                num=i['total']
            zoneView[key]=num
        df1=pd.DataFrame.from_dict(videoCount,orient='index')
        df2=pd.DataFrame.from_dict(zoneView,orient='index')
        result=pd.concat([df1,df2],axis=1)
        result.to_csv('average_each_zone.csv')
        return result

    def change_of_zone_video_year(self):
        '''
        各个分区视频数量变化
        '''
        pdList=[]
        dataTemp={}
        for key in self.zoneList:
            for i in range(2012,2020):
                for j in range(1,12):                    
                    find={'zone':key,'pubtime':{'$gt':get_time(i,j),'$lt':get_time(i,j+1)}}
                    dataTemp[get_time(i,j)]=self.find(find).count()
            df=pd.DataFrame.from_dict(dataTemp,orient='index',columns=[key])
            pdList.append(df)
        result=pd.concat(pdList,axis=1)
        result.to_csv('change_of_zone_video_each_year.csv')
        return result

    def save_zone(self):
        data=self.get_video_each_zone()
        d=pd.DataFrame.from_dict(data,orient='index')
        d.to_csv('zone.csv')

    def save_da(self):
        # db=self.data
        data,date=self.video_change()
        df=pd.DataFrame(data,index=date,columns=['video_num'])
        df.to_csv('video_change.csv')

    def get_title(self):
        #find({ zone:'鬼畜', pubtime:{$gt:'2015-1-1 00:00:00',$lt:'2016-1-1 00:00:00'} }).count()
        dataList={}
        for i in range(2009,2020):
            data=self.data.find({ 'zone':'游戏', 'pubtime':{'$gt':get_time(i,1),'$lt':get_time(i+1,1)}})
            dataTemp=[]
            for j in data:
                dataTemp.append(j['title'])
                # description=j['description']
                # if description is not None:
                #     dataTemp.append(j['description'])
            dataList[get_time(i,1)]=dataTemp
        # save=pd.DataFrame.from_dict(dataList)
        # save.to_csv('title_data.csv')
        return dataList

    def clean_title(self):
        # for i in range(2009,2020):
        #     name='title'+str(i) 
        #     self.db.drop_collection(name)
        #     self.db.drop_collection(str(i))
        pass

    def jieba_cut(self):
        self.clean_title()
        word_filter=['\n','/','-','~','，','！',',','【','】','（','）','：','☆','？','.','[',']',')','(','《','》',' ',':','。','·','#','_','!']
        dataList=self.get_title()
        for key in dataList.keys():
            for data in dataList[key]:
                words=jieba.cut_for_search(data)
                for w in words:
                    if w in word_filter:
                        continue
                    find={'word':w}
                    name='title'+key[:4]
                    f=self.db[name].find_one(find)
                    if f is None:
                        self.db[name].insert({'word':w,'count':0})
                    else:
                        self.db[name].update({'word':f['word']},{'$set':{'count':f['count']+1}})

    def generate(self):
        print('start generate')
        # self.change_of_zone_video_year()
        print('done 1')
        # self.average_each_zone()
        print('done 2')
        # self.cpright_year()
        print('done 3')
        self.save_da()
        print('done 4')
        # self.save_zone()
        print('all done')
    def get_word_dict(self,year,limit):
        # db.title2017.find({word:{"$regex": /^.{2,}$/}}).sort({count:-1})
        total=0
        wordDict={}
        reg=re.compile('/^.{2,}$/')
        name='title'+str(year)
        data=self.db[name].find({'$where':"(this.word.length > 1)"}).sort([('count',-1)]).limit(limit)
        wordDict={}
        for d in data:
            wordDict[d['word']]=d['count']
            total+=d['count']
        for key in wordDict.keys():
            wordDict[key]=wordDict[key]/total
        return wordDict





def get_time(y,m,d=1,H=0,M=0,S=0):
    timeStr='{}-{}-{} {}:{}:{}'.format(y,m,d,H,M,S)
    timeSct=time.strptime(timeStr, '%Y-%m-%d %H:%M:%S')
    t=time.strftime("%Y-%m-%d %H:%M:%S", timeSct)
    return t

def percentage_zone():
    data=pd.read_csv('zone.csv')

def test():
    db=BiliData('localhost',27017)


if __name__ == "__main__":
    db=BiliData('localhost',27017)
    db.generate()
    

