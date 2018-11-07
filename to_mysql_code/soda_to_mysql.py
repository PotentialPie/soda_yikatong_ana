
# coding: utf-8

# In[13]:


import pandas as pd
import numpy as np
import pymysql
from datetime import datetime

import sys, urllib, json




class soda_data_util:
    def __init__(self, file_path, encoding='utf-8'):
        print('========================================开始读取数据========================================')
        # 读取csv表格
        self.csv_data = pd.read_csv(file_path, 
                        encoding = encoding, 
                        header=None, 
                        names=['card_id', 'date', 'time', 'line', 'transport', 'amount', 'is_discount'])
        print('==================================数据读取完毕，数据的shape为:'+str(self.csv_data.shape)+"==================================")

'''
print(soda_data.shape)
print(soda_data[0])
print(type(soda_data[0]))

soda_list = []
soda_list.append(soda_data[0][0])
soda_list.append(soda_data[0][1] + ' ' + soda_data[0][2])
soda_list.append(soda_data[0][3])
soda_list.append(transport_dict[soda_data[0][4]])
soda_list.append(soda_data[0][5])
soda_list.append(is_discount[soda_data[0][6]])
soda_list.append(datetime.strptime(soda_list[1],'%Y-%m-%d %H:%M:%S').strftime("%w"))
#soda_list.append(get_day_type(soda_data[0][1].replace('-','')))
print(tuple(soda_list))
'''

def get_conn():
    conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='19940301', db='soda_data_schema', charset='utf8')
    return conn

def insert(cur, sql, args):
    cur.execute(sql, args)


def read_csv_to_mysql(soda_data):
    transport_dict = {'P+R停车场':0, '公交':1, '出租':2, '地铁':3, '轮渡':4}
    is_discount = {'优惠':0, '非优惠':1, '无':2}
    conn = get_conn()
    cur = conn.cursor()
    sql = 'insert into yikatong_tb (card_id, date_time, line, transport, amount, is_discount, week) values(%s,%s,%s,%s,%s,%s,%s)'
    
    
    for item in soda_data:
        soda_list = []
        soda_list.append(item[0])
        soda_list.append(item[1] + ' ' + item[2])
        soda_list.append(item[3])
        #soda_list.append('爸爸')
        soda_list.append(transport_dict[item[4]])
        soda_list.append(item[5])
        soda_list.append(is_discount[item[6]])
        soda_list.append(datetime.strptime(soda_list[1],'%Y-%m-%d %H:%M:%S').strftime("%w"))
        #soda_list.append(get_day_type(soda_data[0][1].replace('-','')))    
        args = tuple(soda_list)
        print(args)
        insert(cur, sql=sql, args=args)

        conn.commit()
    cur.close()
    conn.close()
        
def get_day_type(query):
        """
        @query a single date: string eg."20160404"
        @return day_type: 0 workday -1 holiday
        20161001:2 20161002:2 20161003:2 20161004:1 
        """
        
        url = 'http://www.easybots.cn/api/holiday.php?d=' + query 
        req = urllib.request.urlopen(url)
        content = req.read().decode("utf-8")
        day_type = json.loads(content)[query]
        day_type = int(day_type)
        print(type(day_type))
        if day_type != 0 and day_type != 1 and day_type != 2:
            return -1
        return day_type
    
import os
csv_list = []
def gci(filepath):
#遍历filepath下所有文件，包括子目录
    files = os.listdir(filepath)
    for fi in files:
        fi_d = os.path.join(filepath,fi)
        if os.path.isdir(fi_d):
            gci(fi_d)
        else:
            file_path = os.path.join(filepath,fi_d)
            if file_path.endswith('csv'):
                print(file_path)
                csv_list.append(file_path)
    return csv_list
csv_list = gci('/usr/soda_data_set/gonggongjiaotongkagufenyouxiangongsi')

for csv_path in csv_list:
    soda_data = soda_data_util(file_path= csv_path, encoding= 'GB2312')
    soda_data = soda_data.csv_data.get_values()
    read_csv_to_mysql(soda_data)
#print(get_day_type(soda_data[0][1].replace('-','')))

