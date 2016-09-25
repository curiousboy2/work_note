import pymysql
import re
from datetime import datetime
import datetime as dt
from bs4 import BeautifulSoup
import gevent
from gevent.pool import Group
import requests
import hashlib
import json
from gevent.queue import Queue
import time

'''
task1:
sina_lhb_everyday
code,code_name,closePrice,value,volume,money
uuid,pubDate,comment,code,code_name,closePrice,value,volume,money,fetchTime

task2:
tradingOffice,buyMoney,sellMoney,netMoney
uuid,code,comment,pubDate,action,tradingOffice,buyMoney,sellMoney,netMoney,fetchTime
新浪龙虎榜每日详情

'''

waiting_db=Queue()
waiting_fetch=Queue()



config={'host':'localhost','user':'nike','passwd':'huanghu','db':'hello','charset':'utf8'}
url1='http://vip.stock.finance.sina.com.cn/q/go.php/vInvestConsult/kind/lhb/index.phtml?tradedate={}'
url2='http://vip.stock.finance.sina.com.cn/q/api/jsonp.php/var%20details=/InvestConsultService.getLHBComBSData?\
symbol={}&tradedate={}&type={}'

#返回数据库中数据的最新发布日期
def db_latestTime():
    conn=pymysql.connect(**config)
    cur=conn.cursor()
    select_str='select pubDate from sina_lhb_everyday order by pubDate desc limit 1'
    cur.execute(select_str)
    result=cur.fetchone()
    if result==None:
        return datetime(2016,1,1)
    else:
        return result[0]

#将数据存入sina_lhb_everyday
def push_lhb_to_db(pubDate,comment,data_list,cur):
    insert_str='insert into sina_lhb_everyday(uuid,pubDate,comment,code,code_name,closePrice,value,volume,money,fetchTime) \
    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    for record in data_list:
        record.insert(0,pubDate)
        record.insert(1,comment)
        uuid=gen_uuid_lhb(record)
        record.insert(0,uuid)
        record.append(datetime.now())
        cur.execute(insert_str,record)

#将数据存入sina_lhb_detail
def push_detail_to_db():
    conn=pymysql.connect(**config)
    cur=conn.cursor()
    num=0
    insert_str='insert into sina_lhb_detail values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    while True:
        record=waiting_db.get()
        if record=='finished':
            num+=1
            if num==2:
                break
            else:
                continue
        cur.execute(insert_str,record)
    conn.commit()
    cur.close()
    conn.close()

#生成lhb uuid
def gen_uuid_lhb(record):
    '''
    pubDate,comment,code,code_name,closePrice,value,volume,money,
    '''
    m=hashlib.md5()
    str='__filed__'.join([record[0],record[1],record[2]])
    m.update(str.encode('utf8'))
    return m.hexdigest()

#生成uuid(detail)
def gen_uuid_detail(record):
    '''
    code,comment,pubDate,action,tradingOffice,buyMoney,sellMoney,netMoney,fetchTime
    '''
    m=hashlib.md5()
    str='__filed__'.join(record[:4])
    m.update(str.encode('utf8'))
    return m.hexdigest()

def fetch_data_lhb():
    today=datetime.now()
    latest_time=db_latestTime()
    if (latest_time-today).day==0:
        return
    while True:
        conn=pymysql.connect(**config)
        cur=conn.cursor()
        data_time=latest_time+dt.timedelta(days=1)
        url=url1.format(data_time.strftime('%Y-%m-%d'))
        resp=requests.get(url)
        resp.encoding='gbk'
        html=resp.text
        soup=BeautifulSoup(html,'html.parser')
        tables=soup.find_all('table',attrs={'class':'list_table'})
        for table in tables:
            trs=table.find_all('tr',attrs={'class':'head'})
            comment=list(trs[0].stripped_strings)[0]
            data_list=[list(tr.stripped_strings)[1:-1] for tr in trs[2:]]
            for tr in trs[2:]:
                tmp_d={}
                tmp_d['code']=list(tr.stripped_strings)[1]
                tmp_d['comment']=comment
                tmp_d['pubDate']=data_time.strftime('%Y-%m-%d')
                td_last=tr.find_all('td')[0]
                #onclick="showDetail('02','600767','2016-08-01',this)
                tmp_d['type']=re.search(r'showDetail\(\'(.*?)\'',td_last.get('onclick')).group(1)
                waiting_fetch.put(tmp_d)
            push_lhb_to_db(data_time,comment,data_list,cur)
        waiting_fetch.put('finished')
        waiting_fetch.put('finished')
        conn.commit()
        cur.close()
        conn.close()

#抓取详细信息
def lhb_detail():
    '''
    {SYMBOL:"600149",type:"01",comCode:"80033832",comName:"中国银河证券股份有限公司上海新郁路证券营业部",buyAmount:"1545.1500",\
    sellAmount:"0.0000",netAmount:1545.15}

     uuid,code,comment,pubDate,tradingOffice,buyMoney,sellMoney,netMoney,fetchTime
    '''
    while True:
        record=waiting_fetch.get()
        if record=='finished':
            waiting_db.put('finished')
            break
        url=url2.format(record['code'],record['pubDate'],record['type'])
        resp=requests.get(url)
        resp.encoding='gbk'
        html=resp.text
        re_html=re.search(r'{.*}',html).group()
        data_dict=json.loads(re_html)
        for action,data_list in data_dict.items():
            '''
            code,comment,pubDate,action,tradingOffice,buyMoney,sellMoney,netMoney,fetchTime
            '''
            for item in data_list:
                tmp=[item['SYMBOL'],record['comment'],record['pubDate'],action,item['comName'],item['buyAmount'],item['sellAmount'],item['netAmount'].datetime.now()]
                uuid=gen_uuid_detail(tmp)
                tmp.insert(0,uuid)
                waiting_db.put(tmp)


if __name__ == '__main__':
    start=time.time()
    g=Group()
    lhb_greenlet=gevent.spawn(fetch_data_lhb)
    g.add(lhb_greenlet)
    g.add(gevent.spawn(lhb_detail))
    g.add(gevent.spawn(lhb_detail))
    g.add(gevent.spawn(push_detail_to_db))
    g.join()
    stop=time.time()
    print('tatol run %s' %(stop-start,))
