'''
test for clear data
'''
print('正在初始化...')

import os
import pandas as pd
import numpy as np
import time 

input('请按Enter开始:')

clean_code = input('请输入待清洗品种代码 :(如：510300)  ')


time_start = time.time()

csv_files_dir  = []
csv_dir = os.getcwd() + '\\data\\SH\\' 
print('正在读取数据...')
for root,dirs,files in os.walk(csv_dir):
        for file in files:
            if clean_code in file and file.endswith('.csv'):
                csv_files_dir.append(os.path.join(root,file))
print('读取数据完毕，耗时: %ss' % round(time.time() - time_start,2))

csv_files = csv_files_dir
csv_dfs = {}

for csv_file in csv_files:
    res = csv_file.split('\\')
    stock_symbol = res[-4] + '\\' +  res[-3] + '\\' + res[-2]
    
    if stock_symbol not in csv_dfs.keys():
        csv_dfs[stock_symbol] = {}
    
    csv_df = pd.read_csv(csv_file, parse_dates=True, low_memory= False)
    if 'UpdateTime' in csv_df.columns.values:
        #tick
        # csv_df.rename(columns = {'UpdateTime' : 'DataTime', 'TrdBSFlag' : 'TickerStatus'}, inplace = True)
        csv_df['Datetime'] = pd.to_datetime(csv_df['TradeDate'].map(str) + csv_df['UpdateTime'].map(str), format ='%Y%m%d%H%M%S%f')
        csv_df['Qty'] = csv_df['Volume'].cumsum()
    else:
        #snapshot
        csv_df['Datetime'] = pd.to_datetime(csv_df['TradeDate'].map(str) + csv_df['DataTime'].map(str), format ='%Y%m%d%H%M%S%f')
        csv_df['Volume'] = csv_df['Qty'].diff()
        csv_df['Netvalue'] = csv_df['Iopv'][csv_df['Iopv'] != 0].iloc[0]
    if 'snapshot' in csv_file:

        csv_dfs[stock_symbol]['snapshot']= csv_df
        
    elif 'tick' in csv_file:
        csv_dfs[stock_symbol]['tick'] = csv_df
   
for date, data in csv_dfs.items():
    print('\r正在清洗 %s 数据，已耗时: %ss' % (date, round(time.time() - time_start,2)), end = '      ')
    snapshot = csv_dfs[date]['snapshot']
    tick = csv_dfs[date]['tick']
    snap_file = [csv_file for csv_file in csv_files if date in csv_file and 'snapshot' in csv_file]
    tick_file = [csv_file for csv_file in csv_files if date in csv_file and 'tick' in csv_file]
    
    for j in snapshot.index:        
        index = np.where(tick.Qty == snapshot.Qty[j])[0]
        
        if len(index) == 0:
            snapshot.drop(axis = 0, index = j, inplace = True)
        else:
            if len(index) == 1:            
                # print('%s 替换为 %s' %(snapshot['Datetime'][j], tick['Datetime'][index[0]]))
                snapshot.loc[j,'Datetime'] = tick['Datetime'][index[0]]
            # elif len(index) > 1:
                # print( tick['Datetime'][index[0]], tick['Datetime'][index[-1]])
                
    for i in range(len(snapshot.index)-1,-1,-1):
        if snapshot[snapshot.index == snapshot.index[i]].Datetime.iloc[0] == snapshot[snapshot.index == snapshot.index[i-1]].Datetime.iloc[0]:
            # print('剔除 %s' % (snapshot.iloc[i]))
            snapshot.drop(axis = 0, index =  snapshot.index[i], inplace = True)
            
    select_volume = ['Datetime','Lastprice','Volume','Precloseprice','AvgPrice','Iopv','Netvalue']
    select_volume.extend([i for i in snapshot.columns if 'Bid[' in i or 'BidQty['  in i])
    select_volume.extend([i for i in snapshot.columns if 'Ask[' in i or 'AskQty['  in i])
            
    snapshot = snapshot[select_volume]
    snapshot.to_csv(snap_file[0],index = False)
   
    tick = tick[['Datetime', 'Price' , 'Volume']]
    tick.to_csv(tick_file[0],index = False)
    
print('已完成, 共耗时: %ss' % (round(time.time() - time_start,2)))
input('请按Enter退出:')