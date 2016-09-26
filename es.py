from elasticsearch import Elasticsearch
import gevent
from gevent.queue import Queue
from gevent.pool import Group
import pymysql
import re
from datetime import datetime
import time

'''
对ES中的数据进行及时性分析
'''

mediaID_queue=Queue()
zero_set=set()
config={'host':'','user':'','passwd':'','db':'','charset':''}

#从数据库中取得mediaID列表，将取得的数据存入队列
def mediaID_from_db():
    conn=pymysql.connect(**config)
    cur=conn.cursor()
    select_str='select mediaID from Media group by mediaName having count(channel)>1'
    cur.execute(select_str)
    mediaIDs=cur.fetchall()
    for mediaID in mediaIDs:
        mediaID_queue.put(mediaID[0])
    cur.close()
    conn.close()
    
#对es中返回数据的单条记录进行，时间间隔大小分析
def deal_item(d_item):
    try:
        pubDate=datetime.strptime(re.sub('\+.*$|\..*$','',d_item['_source']['pubDate']),'%Y-%m-%dT%H:%M:%S')
        fetchTime=datetime.strptime(re.sub('\+.*$|\..*$','',d_item['_source']['fetchTime']),'%Y-%m-%dT%H:%M:%S')
    except Exception as e:
        pass
    jiange=(fetchTime-pubDate).seconds

    #1min 、5min 、10min、30min、1h、3h、6h、12h、24h
    if jiange<60 and jiange!=0:
        return 0
    elif jiange>=60 and jiange<60*5:
        return 1
    elif jiange>=60*5 and jiange<60*10:
        return 2
    elif jiange>=60*10 and jiange<60*30:
        return 3
    elif jiange>=60*30 and jiange<60*60:
        return 4
    elif jiange>=60*60 and jiange<60*60*3:
        return 5
    elif jiange>=60*60*3 and jiange<60*60*6:
        return 6
    elif jiange>=60*60*6 and jiange<60*60*12:
        return 7
    elif jiange>=60*60*12 and jiange<60*60*24:
        return 8
    elif jiange==0:
        return -1
    else:
        return None

#以媒体为单位，进行时间间隔计数
def media_count(hits):
    mediaName=hits[0]['_source']['mediaName']
    count_list=[0]*9
    total=len(hits)
    for hit in hits:
        index=deal_item(hit)
        if index>=0 and index<=8:
            count_list[index]+=1
            
    tmp_list=[mediaName,'null']
    tmp_list.extend(count_list)
    tmp_list.apend(total)
    return tmp_list

#按照channel,进行时间间隔计数
def channel_count(hits,lock):
    global zero_set
    tmp_channel_dict={}
    tmp_total_dict={}
    for hit in hits:
        channelName=hit['_source_']['channelName']
        if channelName not in tmp_channel_dict:
            tmp_channel_dict[channelName]=[0]*9
            tmp_total_dict[channelName]=0
        index=deal_item(hit)
        if index>=0 and index<=8:
            tmp_channel_dict[channelName][index]+=1
        if index==-1:
            zero_set.add(channelName)
        tmp_total_dict[channelName]+=1
    for channel,total in tmp_total_dict:
        tmp_channel_dict[channel].append(total)
    return tmp_channel_dict

#主函数
def main(days):
    es=Elasticsearch(['url'])
    while not mediaID_queue.empty():
        mediaID=mediaID_queue.get()
        body={"filter":{
                "bool":{
                    "must":[
                        {"range":{
                            "pubDate": {"gt": days[0]},
                            "pubDate": {"lt": days[1]}
                        }},{"match_phrase":{"mediaID":mediaID}}
                    ]
                }
            }}
        res = es.search(index='palas_v2', doc_type='items', body=body, size=10000,_source_include=['fetchTime','mediaName','channelName', 'pubDate'])
        hits=res['hits']['hits']
        tmp_list=[media_count(hits)]
        channel_dict=channel_count(hits)
        for channel,count_list in channel_dict:
            count_list.insert(0,tmp_list[0][0])
            count_list.insert(1,channel)
            tmp_list.append(count_list)
        for i in range(len(tmp_list)):
            tmp_list[i].insert(2,days[0])
        conn=pymysql.connect(**config)
        cur=conn.cursor()
        cur.executemany('insert into timeliness values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',tmp_list)
        conn.commit()
        cur.close()
        conn.close()



        
if __name__ == '__main__':
    start=time.time()
    days=['2016-08-01','2016-08-31']
    mediaID_from_db()
    group=Group()
    main1_greenlet=gevent.spawn(main,days)
    main2_greenlet=gevent.spawn(main,days)
    main3_greenlet=gevent.spawn(main,days)
    group.add(main1_greenlet)
    group.add(main2_greenlet)
    group.add(main3_greenlet)
    group.join()
    stop=time.time()
    print('total run %s seconds' %(stop-start,))
    
