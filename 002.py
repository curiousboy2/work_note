from bs4 import BeautifulSoup
from urllib import request
import pymysql
import datetime
import re
import json
import time
import pdb
import threading
import random


#conn = pymysql.connect(host='mech.palaspom.com', user='quant_group', passwd='2016426', db='quant', charset='utf8')
conn = pymysql.connect(host='localhost', user='jeff', passwd='asdf63900687', db='first_db', charset='utf8')
#cur = conn.cursor()
#为longhu_bang_details抓取数据
#专门吐日期泡泡的
def fun(start,stop,num):
    d_start=datetime.datetime.strptime(start,'%Y-%m-%d')
    d_stop=datetime.datetime.strptime(stop,'%Y-%m-%d')
    b=False
    while True:
        l=[]
        for i in range(num):
            l.append(d_start.strftime('%Y-%m-%d'))
            d_start=d_start+datetime.timedelta(days=1)
            if d_start>d_stop:
                b=True
                break
        yield l
        if b:
            break
def fun2(start,stop):
    d_start=datetime.datetime.strptime(start,'%Y-%m-%d')
    d_stop=datetime.datetime.strptime(stop,'%Y-%m-%d')
    while True:
        yield d_start.strftime('%Y-%m-%d')
        d_start=d_start+datetime.timedelta(days=1)
        if d_start>d_stop:
            break
#将一个页面中的数据类型与股票代码通通取来
def f2(tb_list):
    dd={}
    for i in range(len(tb_list)):
        data_type=(tb_list[i].span.string).strip()
        #数据类型data_type
        #股票代码列表code_list
        code_list=[]
        for j in tb_list[i].select('tr[class="head"]')[2:]:
            code_list.append(list(j.stripped_strings)[1])
        #tb_list[n].select('tr[class="head"]')[2:]
        dd[data_type]=code_list
    return dd



#针对给定日期的网页的数据进行解析
def some_page_parser(url,url2,sql,today,data_date,d3):
    global conn
    cur=conn.cursor()
    #pdb.set_trace()
    with request.urlopen(url) as f:
        html1 = f.read()
    soup = BeautifulSoup(html1, "lxml")
    tb_list = soup.find_all(id='dataTable')
    if len(tb_list) == 0:
        return
    # 数据类型与股票代码通通取来
    dd=f2(tb_list)
    #dd={'data_type':[list]}#,...}
    for n in dd:
        for code_id in dd[n]:
            url3=url2 %(code_id,data_date,d3.get(n))
            with request.urlopen(url3) as f:
                html2=f.read()
            str1 = re.sub('([{,])(\w+)(:)', r'\1"\2"\3', html2.decode(encoding='gbk'))
            str2 = re.search('{.*}', str1)
            #pdb.set_trace()
            #str3=str2.group()
            #print(str2.group())
            try:
                d = json.loads(str2.group(),encoding='gbk')
            except:
                str3=re.sub(r'\\','',str2.group())
                d=json.loads(str3,encoding='gbk')
            for i in d:
                for j in d.get(i):
                    try:
                        #pdb.set_trace()
                        #print('!')
                        cur.execute(sql, [today, j['SYMBOL'],  data_date, j['comName'], float(j['buyAmount']) * 10000,
                                float(j['sellAmount']) * 10000, float(j['netAmount']) * 10000,n])
                        print('!')
                    except:
                        print('#')
            conn.commit()

    cur.close()
    #pdb.set_trace()
        #fetch_date,stock_id,data_date,business_house_name,buy,sell,net_buy


if __name__=='__main__':
    start=time.time()
    
    now = datetime.datetime.now().strftime('%Y-%m-%d')  # 抓取的时间，对应到longhu_bang_details中的fetch_date
    sql = 'insert into longhu_bang_details2 values(%s,%s,%s,%s,%s,%s,%s,%s)'
    st1='ST股票、*ST股票和S股连续三个交易日触及涨(跌)幅限制的股票'#21
    st2='ST股票、*ST股票和S股连续三个交易日触及涨幅限制的股票'#22
    st3='ST股票、*ST股票和S股连续三个交易日触及跌幅限制的股票'#23
    st4='单只标的证券的当日融资买入数量达到当日该证券总交易量的50%以上'#28
    st5='序号'#33
    st6='当日无价格涨跌幅限制的股票,其盘中交易价格较当日开盘价上涨100%以上的股票'#17
    st7='当日无价格涨跌幅限制的股票，其盘中交易价格较当日开盘价上涨30%以上的股票'#26
    st8='当日无价格涨跌幅限制的股票，其盘中交易价格较当日开盘价下跌30%以上的股票'#27
    st9='当日有涨跌幅限制的A股,连续2个交易日触及涨幅限制,在这2个交易日中同一营业部净买入股数占当日总成交'#19
    sta='当日有涨跌幅限制的A股,连续2个交易日触及跌幅限制,在这2个交易日中同一营业部净卖出股数占当日总成交'#20
    stb='振幅值达15%的证券'#03
    stc='换手率达20%的证券'#04
    std='无价格涨跌幅限制的证券'#11
    ste='涨幅偏离值达7%的证券'#01
    stf='跌幅偏离值达7%的证券'#02
    stg='连续三个交易日内，日均换手率与前五个交易日的日均换手率的比值达到30倍，且换手率累计达20%的股票'#09
    sth='连续三个交易日内，涨幅偏离值累计达20%的证券'#05
    sti='连续三个交易日内，涨幅偏离值累计达到12%的ST证券、*ST证券和未完成股改证券'#24
    stj='连续三个交易日内，涨幅偏离值累计达到15%的ST证券、*ST证券和未完成股改证券'#07
    stk='连续三个交易日内，跌幅偏离值累计达20%的证券'#06
    stl='连续三个交易日内，跌幅偏离值累计达到12%的ST证券、*ST证券和未完成股改证券'#25
    stm='连续三个交易日内，跌幅偏离值累计达到15%的ST证券、*ST证券和未完成股改证券'#08
    stn='连续三个交易日收盘价达到涨幅限制价格的ST证券、*ST证券和未完成股改证券'#15
    sto='连续三个交易日收盘价达到跌幅限制价格的ST证券、*ST证券和未完成股改证券'#16
    d2={st1:'21',st2:'22',st3:'23',st4:'28',st5:'33',st6:'17',st7:'26',st8:'27',st9:'19',sta:'20',stb:'03',stc:'04',std:'11',ste:'01',stf:'02'\
        ,sti:'24',stj:'07',stk:'06',stl:'25',\
        stm:'08',stn:'15',sto:'16'}
   # url = 'http://vip.stock.finance.sina.com.cn/q/go.php/vInvestConsult/kind/lhb/index.phtml?tradedate='
    #url2='http://vip.stock.finance.sina.com.cn/q/api/jsonp.php/var%%20details=/InvestConsultService.getLHBComBSData?symbol=%s&tradedate=%s&type=%s'
    '''
    for l in fun(start='2008-12-01',stop='2008-12-30',num=5):
        #threads=[]
        for i in range(len(l)):
            url=url+l[i]
            thread=threading.Thread(target=some_page_parser,args=(url,url2,sql,now,l[i],d2))
            threads.append(thread)
        for i in range(len(l)):
            threads[i].start()
        for i in range(len(l)):
            threads[i].join()
        #pdb.set_trace()
        threads.clear()
        #conn.commit()
    #cur.close()
    conn.close()
    stop=time.time()
    print('总共运行了%s秒'%(stop-start,))
    '''
    num=0
    threads=[]
    for i in fun2(start='2010-01-01',stop='2010-12-30'):#start='2004-06-25',stop='2015-09-28'):#!!!!!当要抓一个区间内的数据时，在这里设置日期
        url = 'http://vip.stock.finance.sina.com.cn/q/go.php/vInvestConsult/kind/lhb/index.phtml?tradedate='+ i
        url2='http://vip.stock.finance.sina.com.cn/q/api/jsonp.php/var%%20details=/InvestConsultService.getLHBComBSData?symbol=%s&tradedate=%s&type=%s'
        thread=threading.Thread(target=some_page_parser,args=(url,url2,sql,now,i,d2))
        #threads.append(thread)

        thread.start()
        time.sleep(random.random()+0.3)
        #num=num+1
        if num%60==0:
            time.sleep(random.randint(1,3))
        #if num==100:
        #    pdb.set_trace()
        #some_page_parser(url,url2,cur,sql,now,i,d2)