import requests
import json
import re
import pymysql
from datetime import datetime
import hashlib
import datetime as dt

config={'host':'localhost','user':'nike','passwd':'huanghu','db':'hello','charset':'utf8'}
#从数据库中获取最新日期（如果有的话），返回需要抓取数据的日期生成器
def return_date_pair():
    conn=pymysql.connect(**config)
    cur=conn.cursor()
    select_str='select DatePair from eastmoney_yybph order by DatePair desc limit 1'
    cur.execute(select_str)
    if cur.fetchone()==None:
        #today=datetime.now()
        today=datetime(2016,9,20)
        end_date=datetime(2016,9,10)
        while True:
                yield today-dt.timedelta(days=92),today
                today=today-dt.timedelta(days=1)
                if (today-end_date).days==0:
                    break

    else:
        today=datetime.now()
        end_date=datetime.strptime((cur.fetchone().split('/')[1]),'%Y-%m-%d')
        if (today-end_date).days==0:
            return None
        else:
            while True:
                yield today-dt.timedelta(days=92),today
                today=today-dt.timedelta(days=1)
                if (today-end_date).days==0:
                    break


#判断网页数据的页数
def return_pages(url_pages):
    resp=requests.get(url_pages)
    resp.encoding='gbk'
    html=resp.text
    json_str=re.search(r'{.*}',html).group()
    json_dict=json.loads(json_str)
    pages=json_dict['pages']
    return pages

#处理一个确定的URL的数据
def one_url(start_date,end_date):
    url_inint='http://data.eastmoney.com/DataCenter_V3/stock2016/BusinessRanking/pagesize=100,page={},sortRule=-1,sortType=\
    ,startDate={},endDate={},gpfw=0,js=var%20data_tab_1.html'
    url_pages=url_inint.format(1,start_date,end_date)
    pages=return_pages(url_pages)
    conn=pymysql.connect(**config)
    cur=conn.cursor()
    date_pair=start_date+'/'+end_date
    for page in range(1,pages+1):
        url2=url_inint.format(page,start_date,end_date)
        resp=requests.get(url2)
        resp.encoding='gbk'
        html=resp.text
        json_str=re.search(r'{.*}',html).group()
        json_dict=json.loads(json_str)
        for record in json_dict['data']:
            push_mysql(record,cur,date_pair)
    conn.commit()
    print('&')
    cur.close()
    conn.close()

#生成uuid
def gen_uuid(tmp):
    salesname=tmp[0]
    bcount1dc=tmp[1]
    date_pair=tmp[-2]
    m=hashlib.md5()
    m.update(('{}.{}.{}'.format(salesname,date_pair,bcount1dc)).encode('utf8'))
    return m.hexdigest()

#对即将插入数据库中的数据进行none与取小数位处理
def return_None(value):
    if value=='':
        return None
    elif '.' in value:
        return re.search(r'\-?\d+\.\d{,3}',value).group()
    else:
        return value
#将数据存入mysql
def push_mysql(record,cur,date_pair):
    tmp=[record['SalesName'],record['BCount1DC'],record['AvgRate1DC'],record['UpRate1DC'],record['BCount2DC'],\
         record['AvgRate2DC'],record['UpRate2DC'],record['BCount3DC'],record['AvgRate3DC'],record['UpRate3DC'],\
         date_pair,datetime.now().strftime('%Y-%m-%d')]
    tmp.insert(0,gen_uuid(tmp))
    tmp=list(map(return_None,tmp))
    insert_str='insert into eastmoney_yybph values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    cur.execute(insert_str,tmp)



if __name__=='__main__':

    for (start_date,end_date) in return_date_pair():
        one_url(start_date.strftime('%Y-%m-%d'),end_date.strftime('%Y-%m-%d'))

