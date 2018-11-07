
# coding: utf-8

# In[2]:


import pandas as pd
import numpy as np
from datetime import datetime
#from sqlalchemy import create_engine
#import pymysql
#pymysql.install_as_MySQLdb()
import os
import sys, urllib, json
import time
import pickle


# In[3]:


class soda_data_util:
    def __init__(self, file_path, encoding='utf-8'):
        print('========================================开始读取数据========================================')
        # 读取csv表格
        self.csv_data = pd.read_csv(file_path, 
                        encoding = encoding, 
                        header=None, 
                        names=['card_id', 'date', 'time', 'line', 'transport', 'amount', 'is_discount'])
        print('==================================数据读取完毕，数据的shape为:'+str(self.csv_data.shape)+"==================================")


# In[4]:


'''
host = '127.0.0.1'
port = 3306
db = 'soda_data_repo'
user = 'root'
password = '19940301'
engine = create_engine(str(r"mysql+mysqldb://%s:" + '%s' + "@%s/%s") % (user, password, host, db))
transport_dict = {'P+R停车场':0, '公交':1, '出租':2, '地铁':3, '轮渡':4}
is_discount = {'优惠':0, '非优惠':1, '无':2}
'''

# In[6]:


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
                #print(file_path)
                csv_list.append(file_path)
    return csv_list
csv_list = gci('/usr/soda_data_set/data/filtered-data/18/1801')
print(len(csv_list))
csv_list.sort()
print(csv_list)


# In[ ]:


card_id_dict = {}
for i in [0,1,2]:
    start_time = time.time()
    # 每10个一轮，第一个进行拼接
    print('第',i,'轮：',str(csv_list[i*10]))
    df = soda_data_util(file_path= str(csv_list[i*10])).csv_data
    for csv_path in csv_list[i*10+1:i*10+10]:
        print('第',i,'轮拼接开始：',str(csv_path))
        df = df.append(soda_data_util(file_path= str(csv_path)).csv_data)
    end_time = time.time()
    print('总拼接耗时：', end_time - start_time) 
    gp = df.groupby('card_id')
    end_time = time.time()
    print('group耗时：', end_time - start_time) 
    index = 0
    for name,group in gp:
        if name not in card_id_dict:
            # 顺序分别为，total_num，discount_num，line_set，4hour出行次数
            card_id_dict[name] = [0,0,set(),0,0,0,0,0,0]
        # 加上出行记录
        card_id_dict[name][0] += group.shape[0] 
        # 加上非优惠次数
        card_id_dict[name][1] += (group[group['is_discount'] == '优惠']).shape[0]
        # 加上站点
        col_line = list(pd.Categorical(group['line']).categories)
        #print(col_line)
        for s_line in col_line:
            card_id_dict[name][2].add(s_line)
        # 统计24小时，每6小时间隔数据
        time_static_dict = dict(group['time'].groupby (group['time'].map(lambda x: int(int(x[0:2])/4))).count())
        for k in time_static_dict:
            card_id_dict[name][3+k] += time_static_dict[k]
        index = index + 1
        if index%10000 ==0:
            print(index)
        if index%1000000 ==0:
            end_time = time.time()
            print('百万轮耗时：',end_time - start_time)  
            with open("saved_dict/card_id_dict_%s.pkl"%(index),"wb") as file: 
                pickle.dump(card_id_dict, file, -1)  #data转换为json数据格式并写入文件
                file.close()#关闭文件
                end_time = time.time()
                print('百万轮保存耗时：',end_time - start_time)  
    end_time = time.time()
    print('大轮总耗时：',end_time - start_time)
    print(len(card_id_dict))
    with open("saved_dict/card_id_dict_%s.pkl"%(index),"wb") as file: 
        pickle.dump(card_id_dict, file, -1)  #data转换为json数据格式并写入文件
        file.close()#关闭文件

    

