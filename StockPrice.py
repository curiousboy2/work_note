import pymysql
import pandas.io.sql as sql
import tushare as ts
from pandas import DataFrame
import pandas as pd
from multiprocessing import Pool
"""
对比mysql中已经抓取的数据与tushare提供的数据，将数据中的价格误差在0.01以上的记录，通过excel输出
"""

config={'host':'','user':'','passwd':'','db':'','charset':'utf8'}

#获取数据库中的股票代码列表，返回的数据类型是生成器
def get_codelist():
    conn=pymysql.connect(**config)
    sql_st='SELECT distinct ShortID FROM StockPrice  ;'
    cur=conn.cursor()
    cur.execute(sql_st)
    s=cur.fetchall()
    for item in s:
        yield item[0]

#将有误差的dataframe格式的数据保存到c:/users/jeff/StockPrice目录中
def get_data(code,excel):
    conn=pymysql.connect(**config)
    sql_st="select Date as date,Open as open,High as high,Close as close,Low as low,Amount as volume, \
      AmountPrice as amount,Resumption as factor from StockPrice where Date>'2005-01-03' and\
      Date<'2016-07-15' and ShortID={} order by Date desc;".format(i)
    df1=ts.get_h_data(code=code,start='2005-01-04',end='2016-07-04',autype='hfq',drop_factor=False)
    df2=sql.read_sql_query(sql_st,conn,index_col='date')
    df3=df1-df2
    df=df3[(abs(df3.open)>=0.01)|(abs(df3.close)>=0.01)|(abs(df3.low)>=0.01)|(abs(df3.volume)>=0.01)|(abs(df3.amount)>=0.01)]
    if not df.empty:
        #对df增加一列，列的内容是股票的代码
            df.insert(len(df.columns), 'code_name', value=int(i))
            df.to_excel(excel,sheet_name=code)


if __name__=='__main__':
    p=Pool(10)
    excel=pd.ExcelWriter('c:/users/jeff/StockPrice.xlsx')
    for i in get_codelist():
        p.apply_async(get_data,args=(i,excel))
    p.close()
    p.join()
    excel.save()
