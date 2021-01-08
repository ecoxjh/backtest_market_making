import os
import pandas as pd
import numpy as np
import vaex
from qstrader import settings
'''
使用vaex版本
'''
class CSVDataSource(object):
    def __init__(self, csv_dir, start_time = None, end_time = None, code = None):
        self.csv_dir = csv_dir
        self.code = code
        self.data = self._load_csvs_into_dfs(start_time, end_time)
        self.data_info = self._get_data_info()
        self.data_info['StartTime'] = start_time
        self.data_info['EndTime'] = end_time
    
    def _obtain_stock_hdf5_files(self):
        
        csv_files_dir  = []
        
        for root,dirs,files in os.walk(self.csv_dir):
            for file in files:
                if self.code in file and '.hdf5' in file:
                    csv_files_dir.append(os.path.join(root,file))
                    
        return csv_files_dir
        
    def _obtain_stock_csv_files(self):
        
        csv_files_dir  = []
        
        for root,dirs,files in os.walk(self.csv_dir):
            for file in files:
                if self.code in file:
                    csv_files_dir.append(os.path.join(root,file))
                    
        return csv_files_dir

    def _obtain_stock_symbol_from_filename(self, csv_file):
        
        res = '%s' % csv_file.replace('\\' + self.code + '.csv.hdf5', '')
        res = res[-6:]
        
        return res
    def _load_hdf5_into_df(self, csv_file, start_time = None, end_time = None):
        
        csv_df = vaex.open(csv_file)
        # if csv_df['Unnamed: 0']
        if 'Unnamed: 0' in csv_df:
            csv_df['datetime'] = csv_df['Unnamed: 0']
        if 'trade_price' in csv_df:
            csv_df['price'] = csv_df['trade_price']
            csv_df['volume'] = csv_df['trade_volume']
        
        return csv_df

    def _load_csvs_into_dfs(self, start_time, end_time):
        
        if settings.PRINT_EVENTS:
            print("读取CSV文件...")
            
        # 得到csv文件名称 
        hdf5_files = self._obtain_stock_hdf5_files()
        
        
        csv_dfs = {}
        
        if len(hdf5_files) == 0:
            #将csv文件转为hdf5文件
            csv_files = self._obtain_stock_csv_files()
            
            for csv_file in csv_files:
                stock_symbol = self._obtain_stock_symbol_from_filename(csv_file)
                csv_df = vaex.from_csv(csv_file, convert = True, chunk_size = 5000000)
                #暂时不知道如何修改列名
                csv_df['date_time'] = csv_df['Unnamed: 0']
                
                if stock_symbol not in csv_dfs.keys():
                    csv_dfs[stock_symbol] = {}
                    
                if 'Tick' in csv_file:
                    csv_dfs[stock_symbol]['snapshot']= csv_df
                    if settings.PRINT_EVENTS:
                        print("加载 '%s' %s snapshot数据..." % (self.code, stock_symbol))     
                elif 'Transaction' in csv_file:
                    csv_dfs[stock_symbol]['tick'] = csv_df
                    if settings.PRINT_EVENTS:
                        print("加载 '%s' %s tick数据..." % (self.code, stock_symbol))   
        else:
            for csv_file in hdf5_files:
                stock_symbol = self._obtain_stock_symbol_from_filename(csv_file)
    
                csv_df = self._load_hdf5_into_df(csv_file, start_time, end_time)
                
                if stock_symbol not in csv_dfs.keys():
                    csv_dfs[stock_symbol] = {}
                    
                if 'Tick' in csv_file:
                    csv_dfs[stock_symbol]['snapshot']= csv_df
                    if settings.PRINT_EVENTS:
                        print("加载 '%s' %s snapshot数据..." % (self.code, stock_symbol))     
                elif 'Transaction' in csv_file:
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
        data_info['Fields'] = list(self.data[symbol]['snapshot'])
        
        data_info['Index'] = {}
        data_info['NK'] = {}
        data_info['TickIndex'] = {}
        
        for iday in data_info['Days']:

            data_info['Index'][iday] = pd.to_datetime(self.data[iday]['snapshot'].datetime.values)
            data_info['NK'][iday] = [len(self.data[iday]['snapshot']['price'].values), len(self.data[iday]['tick']['price'].values)]
            
            list_tick = pd.to_datetime(self.data[iday]['tick'].datetime.values)
            
            tick_index = {}
            for jj in range(data_info['NK'][iday][0]):
                tick_index[jj] = np.where(list_tick <= data_info['Index'][iday][jj])[0][-1]
            # for jj in range(data_info['NK'][iday][0]-1,-1,-1):
            #     if jj > 0:
            #         tick_index[jj] = np.setdiff1d(tick_index[jj], tick_index[jj-1])
            
            data_info['TickIndex'][iday] = tick_index
            

        data_info['NDay'] = len(data_info['Days'])
        
        return data_info
    
    