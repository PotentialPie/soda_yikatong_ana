import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine
import pymysql
pymysql.install_as_MySQLdb()
import os
import sys, urllib, json
import time

class soda_data_util:
    def __init__(self, file_path, encoding='utf-8'):
        print('========================================开始读取数据========================================')
        # 读取csv表格
        self.csv_data = pd.read_csv(file_path, 
                        encoding = encoding)
        print('==================================数据读取完毕，数据的shape为:'+str(self.csv_data.shape)+"==================================")
        

host = '127.0.0.1'
port = 3306
db = 'soda_data_repo'
user = 'root'
password = '19940301'
engine = create_engine(str(r"mysql+mysqldb://%s:" + '%s' + "@%s/%s") % (user, password, host, db))

csv_list = ['sitedata1801.csv', 'sitedata1802.csv', 'sitedata1803.csv']
print(len(csv_list))

for csv_path in csv_list:
    start_time = time.time()
    print(csv_path)
    df = soda_data_util(file_path= csv_path).csv_data                                
    print(df.head(10))
    df.to_sql('subway18_sta_by_hour_tb', con=engine, if_exists='append', index=False)
    end_time = time.time()
    print(end_time - start_time)
