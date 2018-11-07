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
                        encoding = encoding, 
                        header=None, 
                        names=['card_id', 'date', 'time', 'line', 'transport', 'amount', 'is_discount'])
        print('==================================数据读取完毕，数据的shape为:'+str(self.csv_data.shape)+"==================================")
        

host = '127.0.0.1'
port = 3306
db = 'soda_data_repo'
user = 'root'
password = '19940301'
engine = create_engine(str(r"mysql+mysqldb://%s:" + '%s' + "@%s/%s") % (user, password, host, db))
transport_dict = {'P+R停车场':0, '公交':1, '出租':2, '地铁':3, '轮渡':4}
is_discount = {'优惠':0, '非优惠':1, '无':2}

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
csv_list = gci('/usr/soda_data_set/data/subway/1801')
print(len(csv_list))

for csv_path in csv_list:
    start_time = time.time()
    #print(csv_path)
    csv_tb_name = 'soda_' + str(os.path.basename(csv_path).split('.')[0]) + '_tb'
    print('开始录表咯：', csv_tb_name)
    df = soda_data_util(file_path= csv_path).csv_data
    #print(df)
    #df['date_time'] = df['date'] + ' ' + df['time']
    #df['transport'] = df['transport'].apply(lambda x: transport_dict[x])
    df['is_discount'] = df['is_discount'].apply(lambda x: is_discount[x])
    df['week'] = df['date'].apply(lambda x: datetime.strptime(x,'%Y-%m-%d').strftime("%w"))
    df = df.drop(['transport'], axis=1)
    print(df.head(10))
    df.to_sql(csv_tb_name, con=engine, if_exists='append', index=False)
    end_time = time.time()
    print(end_time - start_time)
