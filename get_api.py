# -*- coding: utf-8 -*-

'''
从提供的分析师数据API中获取数据，将数据存为csv格式
'''
import json
import time
import random
import os
import pandas as pd
from multiprocessing import Pool
from ggdataapi.ggapi import GGApi




# 2016 - 2005 finished
# 2004 - 2001
def get_data_from_api(type):
    for year in range(2000,1989,-1):
        go_on = True
        page = 1
        while go_on:
            csv_name = 'data_2/' + str(year) + '_type_' + type + '_sale_forecast_stock_' + str(page) + '.csv'
            if os.path.exists(csv_name):
                print('%s exists' % csv_name)
                page += 1
                continue
            print('GET DATA: %s' % csv_name)

            try:
                api = GGApi('mRoWczpvmnaDkAV', 'oTAZLHXtYPx9WNPfgsPfyNSbh3wQqiUF', 'ggservice.go-goal.cn')
                #data_api = 'type='+type+'&year=' + str(year) + '&page=' + str(page) + '&rows=2000'
                data_api ='stat_date='+str(year)+'&code_type='+type
                print('data api is  %s ' % data_api)
                resp = api.post('v1/stock_trust_author/get', data_api)

            except Exception as e:
                del api
                sleep_time = 5 + 3 * random.random()
                print('time out, sleep %f' % sleep_time)
                print(e)
                time.sleep(sleep_time)
                continue

            response_json = json.loads(resp)

            if response_json['code'] == 0:
                data = response_json.get('data', None)
                if data is None:
                    print('All %d data FINISHED' % year)
                    break
                df = pd.DataFrame.from_dict(data)
                df.to_csv(csv_name, encoding='utf-8')
                print('STORE CSV FINISHED: %s' % csv_name)
                # write_pct = write_db(df, 'researcher_forecasts', conn)
                # print('WRITE TABLE FINISHED: %f'%write_pct)
                page += 1
            else:
                print(response_json)
                go_on = False
            del api
            sleep_time = 3 + 2 * random.random()
            print('sleep %f' % sleep_time)
            time.sleep(sleep_time)


# conn.close()


if __name__ == '__main__':
    start=time.time()
    p=Pool(3)
    for type in ['1', '3', '4']:
       p.apply_async(get_data_from_api,args=(type,))
        #get_data_from_api(type)
    p.close()
    p.join()
    stop=time.time()
    print('total run %s seconds' %(stop-start))
