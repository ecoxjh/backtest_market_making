import os
import pandas as pd
import numpy as np
from qstrader import settings
'''
使用dict/list版本
'''

class CSVDataSource(object):
    def __init__(self, csv_dir, start_time = None, end_time = None, code = None, daily_start_time = None, deal_volume_time = None):
        self.csv_dir = csv_dir
        self.code = code
        self.data = self._load_csvs_into_dfs(start_time, end_time)
        self.daily_start_time = daily_start_time
        self.deal_volume_time = deal_volume_time
        self.data_info = self._get_data_info()
        self.data_info['StartTime'] = start_time
        self.data_info['EndTime'] = end_time
        
    def _obtain_stock_csv_files(self):
        
        csv_files_dir  = []
        
        for root,dirs,files in os.walk(self.csv_dir):
            for file in files:
                if self.code in file and file.endswith('.csv'):
                    csv_files_dir.append(os.path.join(root,file))
                    
        return csv_files_dir

    def _obtain_stock_symbol_from_filename(self, csv_file):
        
        # res = '%s' % csv_file.replace('\\' + self.code + '.csv', '')
        res = csv_file.split('\\')
        res = res[-4] + '-' +  res[-3] + '-' + res[-2]
        
        return res

    def _load_csv_into_df(self, csv_file, start_time = None, end_time = None):
       
        csv_df = pd.read_csv(csv_file, parse_dates = True, low_memory = False)
        csv_df['Datetime'] = pd.to_datetime(csv_df['Datetime'])
        
        if 'Precloseprice' in csv_df.columns.values:
            #snapshot
            if 'Volume' not in csv_df.columns.values:
                csv_df['Volume'] = csv_df['Qty'].diff()
                csv_df['Volume'].iloc[0] = csv_df['Qty'].iloc[0]
            if 'Datetime' not in csv_df.columns.values:
                csv_df['Datetime'] = pd.to_datetime(csv_df['TradeDate'].map(str) + csv_df['DataTime'].map(str), format ='%Y%m%d%H%M%S%f')

            select_volume = ['Datetime','Lastprice','Volume','Precloseprice','AvgPrice','Iopv','Netvalue']
            select_volume.extend([i for i in csv_df.columns if 'Bid[' in i or 'BidQty['  in i])
            select_volume.extend([i for i in csv_df.columns if 'Ask[' in i or 'AskQty['  in i])
            
            csv_df = csv_df[select_volume]
            
            csv_dict = csv_df.to_dict(orient='split')
        else:
            #tick
            # csv_df.rename(columns = {'UpdateTime' : 'DataTime', 'TrdBSFlag' : 'TickerStatus'}, inplace = True)
            if 'Datetime' not in csv_df.columns.values:
                csv_df['Datetime'] = pd.to_datetime(csv_df['TradeDate'].map(str) + csv_df['UpdateTime'].map(str), format ='%Y%m%d%H%M%S%f')
            csv_df = csv_df[['Datetime', 'Price' , 'Volume']]
            csv_dict = csv_df.to_dict(orient='split')
        # csv_dict.pop('columns')
        
        return csv_dict

    def _load_csvs_into_dfs(self, start_time, end_time):
        
        if settings.PRINT_EVENTS:
            print("读取CSV文件...")
            
        # 得到csv文件名称 

        csv_files = self._obtain_stock_csv_files()
        csv_dfs = {}
        
        for csv_file in csv_files:
            stock_symbol = self._obtain_stock_symbol_from_filename(csv_file)

            csv_df = self._load_csv_into_df(csv_file, start_time, end_time)
            
            if stock_symbol not in csv_dfs.keys():
                csv_dfs[stock_symbol] = {}
                
            if 'snapshot' in csv_file:
                csv_dfs[stock_symbol]['snapshot']= csv_df
                if settings.PRINT_EVENTS:
                    print("加载 '%s' %s snapshot数据..." % (self.code, stock_symbol))     
            elif 'tick' in csv_file:
                csv_dfs[stock_symbol]['tick'] = csv_df
                if settings.PRINT_EVENTS:
                    print("加载 '%s' %s tick数据..." % (self.code, stock_symbol))    

        return csv_dfs
    
    def _get_data_info(self):
        
        data_info = {}
        #取第一只票得到info
        data_info['Days'] = list(self.data.keys())
        #select tick
        symbol = data_info['Days'][0]
        data_info['Fields'] = {'snapshot' : list(self.data[symbol]['snapshot']['columns']),
                               'tick' : list(self.data[symbol]['tick']['columns'])}
        
        data_info['NK'] = {}
        data_info['TickIndex'] = {}
        deal_index = {}
        start_index = {}
        
        for iday in data_info['Days']:

            data_info['NK'][iday] = len(self.data[iday]['snapshot']['index'])
            list_snapshot = pd.to_datetime([x[0] for x in self.data[iday]['snapshot']['data']])
            list_tick = pd.to_datetime([x[0] for x in self.data[iday]['tick']['data']])
            
            tick_index = {}
            
            
            if self.deal_volume_time is not None:
                deal_flag = 0
                deal_volume_time =  pd.to_datetime(iday + ' ' + self.deal_volume_time)
            else:
                deal_flag = 1
            
            if self.daily_start_time is not None:
                start_flag = 0
                start_time = pd.to_datetime(iday + ' ' + self.daily_start_time)
            else:
                start_flag = 1
                
            for j in range(data_info['NK'][iday]):
                
                if deal_flag == 0 and list_snapshot[j] > deal_volume_time:
                    deal_index[iday] = j
                    deal_flag = 1
                
                if start_flag == 0 and list_snapshot[j] >= start_time:
                    start_index[iday] = j
                    start_flag = 1
                
                if len(res := np.where(list_tick <= list_snapshot[j])[0]) == 0:
                    tick_index[j] = None
                else:
                    tick_index[j] = res[-1]
                
            data_info['TickIndex'][iday] = tick_index
        data_info['DealIndex'] = deal_index
        data_info['StartIndex'] = start_index
            
        data_info['NDay'] = len(data_info['Days'])
        
        return data_info
    
    