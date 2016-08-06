

import pymysql
import csv
import os
from multiprocessing import Pool
import time
import pdb
import re
from logger import *


#logging.basicConfig(level=logging.INFO)

config2 = {
    'host': 'mech.palaspom.com',
    'user': 'quant_group',
    'passwd': '*****',
    'db': 'quant',
    'charset': 'utf8'
}
config = {
    'host': 'localhost',
    'user': 'jeff',
    'passwd': 'asdf63900687',
    'db': 'first_db',
    'charset': 'utf8'
}


sql = 'insert into get_api values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'


# 生成目标文件的列表
def get_csv_list(rootdir='C:\\Users\\Administrator\\PycharmProjects\\other\\data'):
    for (dirpath, dirnames, filenames) in os.walk(rootdir):
        for filename in filenames:
            yield os.path.join(dirpath, filename)

#转换''===>None
def f(i):
    if i=='':
        return None
    return i

# 处理一个csv文件
def deal_one_csv(filename):
    conn = pymysql.connect(**config)
    cur = conn.cursor()
    read = csv.reader(open(filename,'r',encoding='utf8',errors='ignore'))
    list_all = list(read)
    try:
        cur.executemany(sql,list_all[1:])#首先尝试批量插入
        conn.commit()
    except:
        del conn
        conn=pymysql.connect(**config)
        cur=conn.cursor()
        for i in list_all[1:]:
            try:
                cur.execute(sql, i)#批量插入失败后，采用单条插入
                conn.commit()
                print('!')
            except Exception as e:
                logger.info(filename)
                logger.info(e)
                del conn  #当csv中的数据发生冲突后，conn将会被阻塞，此时该conn将不可用
                conn=pymysql.connect(**config)
                cur=conn.cursor()
    cur.close()
    conn.close()

if __name__ == '__main__':
    start = time.time()
    p=Pool(6)
    for i in get_csv_list():
        deal_one_csv(i)
       #p.apply_async(deal_one_csv,args=(i,))
    p.close()
    p.join()
    stop = time.time()
    print('total run %s seconds' % (stop - start,))