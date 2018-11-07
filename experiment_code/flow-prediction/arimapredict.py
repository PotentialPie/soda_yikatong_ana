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

# ARIMA model
import pandas as pd
import numpy as np
from scipy import  stats
import matplotlib.pyplot as plt
import statsmodels.api as sm
from statsmodels.graphics.api import qqplot
from statsmodels.tsa.stattools import adfuller


host = '127.0.0.1'
port = 3306
db = 'soda_data_repo'
user = 'root'
password = '19940301'
# engine = create_engine(str(r"mysql+mysqldb://%s:" + '%s' + "@%s/%s") % (user, password, host, db))

conn = pymysql.connect(host=host,port=port,user=user,password=password,database = db)
cursor = conn.cursor()


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
            
            
def armaModel(site1h):
    results = []
    plist = []
    qlist = []
    for (i,dth) in zip(range(24),site1h):
        print('day ',i)
        pd.Series(dth)
        predict = np.zeros(9)
        try:
            (p,q) =(sm.tsa.arma_order_select_ic(dth,max_ar=5,max_ma=5,ic='aic')['aic_min_order'])
            arma_mod = sm.tsa.ARMA(dth,order=(p,q)).fit()
            predict = arma_mod.predict(27, 35, dynamic=True)
        except:
            if i==2:
                predict = np.zeros_like(predict)   
            print('no\n')
            p=-1
            q=-1
    
        results.append(predict)
        plist.append(p)
        qlist.append(q)
        
    results = np.array(results)
    resultst = results.T
    
    return resultst,plist,qlist 

table16 = 'subway16_sta_by_hour_tb'
table18 = 'subway18_sta_by_hour_tb'

sites = get_site()

save_file = './predict8days2.csv'
wrong_file = './pqfile2.csv'
add_table_title(save_file)
add_log_title(wrong_file)


for s in sites[15:]: # the top 30 is enough!
    sql = "select * from %s where site='%s' order by data ASC"%(table18,s) # select a site
    
    # get the sites table
    rnum = cursor.execute(sql)
    results = cursor.fetchall()
    
    # get a day a sample
    site1 = get_site_curve(results,28)  # 28 days, a day contains 24h
    site1h = site1.T # 24 lines, a line contains 28 samples of a certain hour
    
    predictdays,p,q = armaModel(site1h)
    
    add_records(filename=save_file,site=s,days=predictdays) 
    add_wrong_records(filename=wrong_file,site=s,p=p,q=q)