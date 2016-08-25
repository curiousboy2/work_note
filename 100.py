import pymysql
import datetime as dt
from datetime import datetime

#抓取东方财富营业部3月份排行榜数据时,对需要抓取数据的时间段进行确认的程序片段
def f(s):
    x,y=s
    return x.strftime('%Y-%m-%d'),y.strftime('%Y-%m-%d')

def get_date_for_catch():
    conn=pymysql.connect(**)
    cur=conn.cursor()
    cur.execute('select start_day,fetchtime from second order by fetchtime desc limit 1')
    x,y=cur.fetchall()
    start=datetime.strptime(x,'%Y-%m-%d')
    stop=datetime.strptime(y,'%Y-%m-%d')
    today=datetime.now()
    jiange=(today-stop).days
    l=[]
    for i in range(1,jiange+1):
        l.append((start+dt.timedelta(days=i),stop+dt.timedelta(days=i)))
    return list(map(f,l))

if __name__=='__main__':
     result=get_date_for_catch() 
     base_url='http://www.huang.net?start=%s,stop=%s'
     if len(result)==0:
         print('no need update')
     else:
         for one in result:
             url=base_url %one
             print(url)
