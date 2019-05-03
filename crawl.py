import requests
from multiprocessing import Pool
import pandas as pd
from lxml import etree
import time
import os
import threading
import re
class crawl:
    def __init__(self,city_name,state=1):
        self.state = state
        self.city_name = city_name
        self.flag = 1
        self.time_hour = ''
        self.headers = {'user_agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36 Edge/17.17134'}
    def get_day(self):
        for year in range(2015,2020):
            if year < 2019:
                for month in range(1,13):
                    month = "%02d" % month
                    time = str(year) + str(month)
                    url = 'http://www.tianqihoubao.com/aqi/{0}-{1}.html'.format(self.city_name,time)
                    data_pd = pd.read_html(url, header=0, encoding='gbk',index_col='日期')[0] 
                    print(data_pd)
                    self.save_data(data_pd)
                time.sleep(1)
            else:
                for month in range(1,5):
                    month = "%02d" % month
                    time = str(year) + str(month)
                    url = 'http://www.tianqihoubao.com/aqi/{0}-{1}.html'.format(self.city_name,time)
                    data_pd = pd.read_html(url, header=0, encoding='gbk',index_col='日期')[0] 
                    
                    print(data_pd)
                    self.save_data(data_pd)
                time.sleep(1)
    def get_per_hour(self):
        headers = {'user_agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36 Edge/17.17134'}
        url = 'http://www.tianqihoubao.com/aqi/{0}.html'.format(self.city_name)
        html = requests.get(url,headers=headers)
        content = html.text
        selector = etree.HTML(content)
        time_ = selector.xpath('//div[@class="desc"]/text()')[0]
        time_ =re.findall(r'数据更新时间:(.+)\(',time_)[0].strip()
        if os.path.exists(self.file_name):
            print('文件已存在，检测是否更新')
            befor_data = pd.read_csv(self.file_name,encoding='utf_8_sig')
            last = len(befor_data)
            last_time = befor_data.loc[last-1,'time']
            if last_time !=time_:
                data =pd.read_html(url, header=0, encoding='gbk')[0] 
                data['time'] = time_
                data['city'] = self.city_name
                data=data.reindex(columns=[ 'time','city','监测点', 'AQI指数', '空气质量状况', 'PM10', 'Co', 'No2', 'So2', 'O3'])
                data = data.set_index('time')
                self.save_data(data)
                self.time_hour = time_
                print('已更新')
            else:
                print('未更新')
        else:
            print('第一次创建文件')
            data =pd.read_html(url, header=0, encoding='gbk')[0] 
            data['time'] = time_
            data['city'] = self.city_name
            data=data.reindex(columns=[ 'time','city','监测点', 'AQI指数', '空气质量状况', 'PM10', 'Co', 'No2', 'So2', 'O3'])
            data = data.set_index('time')
            self.save_data(data)
            self.time_hour = time_
            
    def save_data(self,data):
        print('保存成功')
        attribute = list(data.columns)
        if not os.path.exists(self.file_name):
            data.to_csv(self.file_name, header=attribute,encoding='utf_8_sig',mode='a')
            self.flag = 0
        else:
            data.to_csv(self.file_name, header=None,encoding='utf_8_sig',mode='a')
    def start_crawl(self): 
        if self.state ==1:
            self.file_name = '空气质量-{0}_day.csv'.format(self.city_name)
            self.get_day()
        else:
            self.file_name = '空气质量-{0}_hour.csv'.format(self.city_name)
            self.get_per_hour()
if __name__ == '__main__':
    city_name = ['taian','qinhuangdao','beijing','jinan']
    for i in city_name:
        a = crawl(i,state=0)
        a.start_crawl()
