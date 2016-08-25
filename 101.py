import os
import json
import re
from pandas import DataFrame,ExcelWriter
import pandas as pd
import time

#
#
#对多个json文件，进行按频道数的数据统计，将结果输出到excel表



#将文件名进行清洗
#hello(1-2).json   hello-1.json hello-2.json
def f(filename):
    return re.sub(r'\(.*\)|-\d+','',filename).split('.')[0]

# 生成目标文件的列表
#{'channel':[file1,file2,..],'channel2':[File1,File2,...]}
def get_channel_file_map(rootdir):
    os.chdir(rootdir)
    d={}
    for i in os.listdir():
        s=os.path.abspath(i)
        for j in os.listdir(i):
            ss=f(j)
            if ss in d:
                d[ss].append(os.path.join(s,j))
            else:
                d[ss]=[os.path.join(s,j)]
    return d


#处理一个json文件，并将结果输出到excel中
def deal_one_json(channel,files):
    dd={}

    for file in files:
        d=json.load(open(file,'r',encoding='utf-8'))
        for j in range(len(d)):
            pub_date_str=re.match(r'\d+-\d+-\d+',d[j]['Pubdate']).group()
            if pub_date_str in dd:
                dd[pub_date_str]+=1
            else:
                dd[pub_date_str]=1
    l=[[i,j] for i,j in dd.items()]
    df = DataFrame(data=l,columns=('PubDate', 'Number'))
    df.insert(0, 'Channel', channel)
    df.sort_values(by='PubDate',kind='mergesort',inplace=True)
    return df

if __name__=='__main__':
     start=time.time()
     #d=get_channel_file_map('d:\\Result')
     d=get_channel_file_map('E:\\Fisher\\Result')
     writer=ExcelWriter('e:\\jeff.xlsx')
     for i,j in d.items():
         df=deal_one_json(i,j)
         df.to_excel(writer,index=False,sheet_name=i)
     writer.save()
     stop=time.time()
     print('total run %d seconds' %(stop-start,))

