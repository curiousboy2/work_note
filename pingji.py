from pandas import DataFrame,ExcelFile
import pandas as pd

#将‘-’线转化为0
def translate_zero(df):
    for index,serie in df.iterrows():
        keys=serie.keys()
        for key in range(len(keys)):
            if df.iloc[index,key]=='-':
                df.iloc[index,key]=0
    return df

#对series条目的数据，改变格式，使之与评级列表的格式相同
def arrange(series):
    total=series.sum()
    keys=series.keys()
    for key in range(len(keys)):
        if series[key]==total:
            break
        if key==0:
            continue
        series[key]+=series[key-1]
    return series

#评级函数
def run(df,ratin_series):
    df_no_grade=df.drop('grade',axis=1,inplace=False)
    total=ratin_series.sum()
    min_score=float('inf')
    min_index=''
    df_standard=df_no_grade*total
    ratin_series=arrange(ratin_series)
    weigth_series=pd.Series(index=('1min','5min','10min','30min','1h','3h','6h','12h','24'),data=[10,9,8,7,6,5,4,3,2])
    for index,serie in df_standard.iterrows():
        tmp_s=(ratin_series-serie).abs()
        score=(tmp_s*weigth_series).sum()
        if score<min_score:
            min_score=score
            min_index=index
    print('min_score:{},grade:{}'.format(min_score,df.iloc[min_index,0]))

if __name__ == '__main__':
    excel='d:\\jeff\\pingji.xlsx'
    #grade	1min	5min	10min	30min	1h	3h	6h	12h	24h
    df=pd.read_excel(excel)
    DF=translate_zero(df)
    wait_rating_data=[[100,30,40,20,0,0,0,0,0],[10,40,30,90,0,0,2,0,0]]
    for record in wait_rating_data:
        wait_rating_series=pd.Series(index=('1min','5min','10min','30min','1h','3h','6h','12h','24'),data=record)
        run(DF,wait_rating_series)

