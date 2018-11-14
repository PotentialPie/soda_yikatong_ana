
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine
import pymysql
pymysql.install_as_MySQLdb()
import os
import sys, urllib, json
import time
import csv
import matplotlib.pyplot as plt 
from tensorflow import keras
from keras.models import Sequential
from keras.layers import Dense
from keras.layers.recurrent import LSTM
from keras.layers import Activation
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error
from keras.models import load_model
import pickle


# In[2]:


host = '127.0.0.1'
port = 3306
db = 'soda_data_repo'
user = 'root'
password = '19940301'
# engine = create_engine(str(r"mysql+mysqldb://%s:" + '%s' + "@%s/%s") % (user, password, host, db))

conn = pymysql.connect(host=host,port=port,user=user,password=password,database = db)
cursor = conn.cursor()


# In[3]:


def get_site(path='/usr/soda_data_set/data/predicted-data/top40.txt'):
    site_list = []
    with open(path,'r') as f:
        for line in f:
            site_list.append(line.strip('\n'))
    return site_list

# need to be finished! to get the max records at these dates
def get_totalrecords():
    return None

def get_site_curve(results,daynum=100):
    list0 = []
    
    total = len(results)
    if daynum > total: 
        preserve = total
    else:
        preserve = daynum
    
    start = total-preserve
    for row in results[start:]: # throw away day 1, that is the new year day
        list0.append(row[3:]) # throw the other information
    
    site = np.array(list0)
    return site

def get_list_from_array(a):
    tem = []
    for i in a:
        tem.append(str(i))
    return tem

def add_table_title(filename = 'predict8days.csv'):
    with open(filename,'a',newline='') as f:
        writer = csv.writer(f)
        tem = ['site','date']
        res = np.array(range(0,24))
        res = get_list_from_array(res)
        tem.extend(res)
        print(tem)
        writer.writerow(tem)

def add_records(filename='predict8days.csv',site=None,days=None):
    with open(filename,'a',newline='') as f:
        writer = csv.writer(f)
        daynum = days.shape[0]
        for (i,d) in zip(range(daynum),days):
            tem = []
            tem.append(site)
            tem.append(i)
            hour_list = get_list_from_array(d)
            tem.extend(hour_list)
            writer.writerow(tem)

def add_log_title(filename='./pqfile.csv'):
    with open(filename,'a',newline='') as f:
        writer = csv.writer(f)
        tem = ['site','hour','pvalue','qvalue']
        print(tem)
        writer.writerow(tem)
        
def add_wrong_records(filename='./pqfile.csv',site=None,p=None,q=None):
    with open(filename,'a',newline='') as f:
        writer = csv.writer(f)
        renum = len(p)
        for (i,pa,qa) in zip(range(renum),p,q):
            tem = []
            tem.append(site)
            tem.append(i)
            tem.append(pa)
            tem.append(qa)
            writer.writerow(tem)
def create_model():
    model = Sequential()
    #输入数据的shape为(n_samples, timestamps, features)
    #隐藏层设置为256, input_shape元组第二个参数1意指features为1
    #下面还有个lstm，故return_sequences设置为True
    model.add(LSTM(units=256,input_shape=(None,1),return_sequences=True))
    model.add(LSTM(units=256))
    #后接全连接层，直接输出单个值，故units为1
    model.add(Dense(units=1))
    model.add(Activation('linear'))
    model.compile(loss='mse',optimizer='adam')
    return model
# convert an array of values into a dataset matrix
def create_dataset(dataset, look_back=1):
    dataX, dataY = [], []
    for i in range(len(dataset)-look_back-1):
        a = dataset[i:(i+look_back), 0]
        dataX.append(a)
        dataY.append(dataset[i + look_back, 0])
    return np.array(dataX), np.array(dataY)

# fix random seed for reproducibility
np.random.seed(7)


# In[4]:


table16 = 'subway16_sta_by_hour_tb'
table18 = 'subway18_sta_by_hour_tb'

sites = get_site()

save_file = './lstm_predict8days.csv'
#wrong_file = './pqfile2.csv'
add_table_title(save_file)
#add_log_title(wrong_file)


# In[5]:


print(len(sites))
print(sites)


# In[6]:


data_seq = []
for s in sites: # the top 30 is enough!
    sql = "select * from %s where site='%s' order by data ASC"%(table18,s) # select a site
    
    # get the sites table
    rnum = cursor.execute(sql)
    results = cursor.fetchall()
    #print(len(results))
    # get a day a sample
    site1 = get_site_curve(results,90)  # 28 days, a day contains 24h
    site1h = site1.T # 25 lines, a line contains 28 samples of a certain hour
    #print(site1h.shape)
    data_seq.append(site1h)
data_seq = np.array(data_seq)


# In[7]:


print(data_seq.shape)


# In[8]:


dataX_site = []
for site_index in range(40):
    dataX = []
    for i in range(90):
        dataX.extend(data_seq[site_index,:,i])
    dataX = np.array(dataX)
    print(dataX.shape)
    #plt.plot(dataX[:720])
    #plt.show()
    dataX_site.append(dataX)
dataX_site = np.array(dataX_site)
print(dataX_site.shape)


# In[9]:


#将数据归一化
# scaler_minmax = MinMaxScaler()
# data = dataX.reshape(-1,1)
# print(data.shape)
# dataset = scaler_minmax.fit_transform(data)
# print(dataset[:10])


# # split into train and test sets
# train_size = int(len(dataset) * 0.67)
# test_size = len(dataset) - train_size
# train, test = dataset[0:train_size,:], dataset[train_size:len(dataset),:]
data_create = []
scaler_minmax_list = []
for i in range(40):
    dataX = dataX_site[i]
    scaler_minmax = MinMaxScaler()
    data = dataX.reshape(-1,1)
    data = scaler_minmax.fit_transform(data)
    infer_seq_length = 167#用于推断的历史序列长度

    d = []
    for i in range(data.shape[0]-infer_seq_length):
        d.append(data[i:i+infer_seq_length+1].tolist())
    d = np.array(d)
    print(d.shape)
    data_create.append(d)
    scaler_minmax_list.append(scaler_minmax)
data_create = np.array(data_create)
print(data_create.shape)


# In[ ]:

'''
predict_test = []
for i in range(40):
    d = data_create[i]
    split_rate = 0.9
    X_train, y_train = d[:int(d.shape[0]*split_rate),:-1], d[:int(d.shape[0]*split_rate),-1]
    print(X_train.shape)
    print(y_train.shape)
    model = create_model()
    #model.summary()
    model.fit(X_train, y_train, batch_size=50,epochs=50,validation_split=0.1)
    model.save('saved_model/lstm_%d.h5'%(i))
    #plt.plot()
    #plt.plot(scaler_minmax.inverse_transform(d[int(len(d)*split_rate):,-1]),label='true data')
    #plt.plot(scaler_minmax.inverse_transform(model.predict(d[int(len(d)*split_rate):,:-1])),'r:',label='predict')
    #plt.legend()
    predict_test.append(model.predict(d[int(len(d)*split_rate):,:-1]))
'''

# In[ ]:

print(len(scaler_minmax_list))
site_predicted_hour = []
for i in range(40):
    model = load_model('saved_model/lstm_%d.h5'%(i))
    d = data_create[i]
    predict_d = d[-1,1:]
    #print(predict_d)
    #print(predict_d.shape)
    for j in range(8*24):
        predicted_hour = model.predict(np.array([predict_d[-167:]]))
        predict_d = np.append(predict_d, predicted_hour)
        #print(predict_d)
        predict_d = np.reshape(predict_d, (predict_d.shape[0],1))
        print(predict_d.shape)
    print(i)
    #scaler_minmax = scaler_minmax_list[i]
    #site_predicted_hour.append(scaler_minmax.inverse_transform(predict_d[-8*24:]))
    site_predicted_hour.append(predict_d[-8*24:])


# In[ ]:


site_predicted_hour = np.array(site_predicted_hour)
print(site_predicted_hour.shape)
# look_back = 167
# trainX, trainY = create_dataset(train, look_back)
# testX, testY = create_dataset(test, look_back)
with open("saved_model/predict_40_site_8_days.pkl","wb") as file: 
    pickle.dump(site_predicted_hour, file, -1)  #data转换为json数据格式并写入文件
    file.close()#关闭文件


# In[ ]:


# print(trainX.shape, trainY.shape)
# print(testX.shape, testY.shape)


# In[ ]:


for i in range(40):
    s = sites[i]
    predictdays = np.reshape(site_predicted_hour[i],(8,24))
    print(predictdays.shape)
    add_records(filename=save_file,site=s,days=predictdays) 


# In[ ]:


#inverse_transform获得归一化前的原始数据
# plt.plot(scaler_minmax.inverse_transform(d[:,-1]),label='true data')
# plt.plot(scaler_minmax.inverse_transform(model.predict(d[:,:-1])),'r:',label='predict')
# plt.legend()


# In[ ]:


# plt.plot()
# plt.plot(scaler_minmax.inverse_transform(d[int(len(d)*split_rate):,-1]),label='true data')
# plt.plot(scaler_minmax.inverse_transform(model.predict(d[int(len(d)*split_rate):,:-1])),'r:',label='predict')
# plt.legend()


# In[ ]:


# plt.plot()
# plt.plot(scaler_minmax.inverse_transform(d[int(len(d)*split_rate):,-1]),label='true data')
# #plt.plot(scaler_minmax.inverse_transform(model.predict(d[int(len(d)*split_rate):,:-1])),'r:',label='predict')
# plt.legend()


# In[ ]:


# plt.plot()
# #plt.plot(scaler_minmax.inverse_transform(d[int(len(d)*split_rate):,-1]),label='true data')
# plt.plot(scaler_minmax.inverse_transform(model.predict(d[int(len(d)*split_rate):,:-1])),'r:',label='predict')
# plt.legend()

