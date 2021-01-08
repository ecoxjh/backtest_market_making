import os

import pandas as pd
import datetime
from qstrader import settings

class CSVDataSource(object):
    def __init__(self, csv_dir, start_time, end_time):
        self.csv_dir = csv_dir

        self.data = self._load_csvs_into_dfs(start_time, end_time)
        self.data_info = self._get_data_info()
        self.data_info['StartTime'] = start_time
        self.data_info['EndTime'] = end_time
        
    def _obtain_stock_csv_files(self):
        return [
            file for file in os.listdir(self.csv_dir)
            if file.endswith('.xls')
        ]

    def _obtain_stock_symbol_from_filename(self, csv_file):
        
        return '%s' % csv_file.replace('.csv', '')

    def _load_csv_into_df(self, csv_file, start_time, end_time):
       
        csv_df = pd.read_excel(
            os.path.join(self.csv_dir, csv_file),
            index_col= 0,
            parse_dates=True
        ).sort_index()
        
        csv_df = csv_df[csv_df.index >= start_time]
        
        if len(end_time) == 9 or len(end_time) == 10:
            end_time = datetime.datetime.strptime(end_time,'%Y-%m-%d') + datetime.timedelta(days = 1)
            
        csv_df = csv_df[csv_df.index <= end_time]
        
        return csv_df

    def _load_csvs_into_dfs(self, start_time, end_time):
        
        if settings.PRINT_EVENTS:
            print("读取CSV文件...")
            
        # 得到csv文件名称 

        csv_files = self._obtain_stock_csv_files()
        
        csv_dfs = {}
        
        for csv_file in csv_files:
            stock_symbol = self._obtain_stock_symbol_from_filename(csv_file)[:6]
            if settings.PRINT_EVENTS:
                print("加载 '%s'数据..." % stock_symbol)
            csv_df = self._load_csv_into_df(csv_file, start_time, end_time)
            csv_dfs[stock_symbol] = csv_df
        
        return csv_dfs
    
    def _get_data_info(self):
        csv_files = self._obtain_stock_csv_files()
        data_info = {}
        #取第一只票得到info
        symbol = self._obtain_stock_symbol_from_filename(csv_files[0])[:6]
        data_info['Fields'] = list(self.data[symbol])
        data_info['NK'] = len(self.data[symbol])
        index = list(map(datetime.datetime.date,self.data[symbol].index))
        data_info['NDay'], data_info['DayIndex'] = self._get_different_day_index_list(index)
        
        return data_info
    
    def _get_different_day_index_list(self, index):
        
        different_value_index = []
        
        for i in range(1,len(index)):
            if index[i] != index[i-1]:
                different_value_index.append(i)
        # index_dict = dict(zip(range(1,len(different_value_index)+1),different_value_index))
        if len(different_value_index) == 0:
            different_value_index = [0]
        return len(different_value_index), different_value_index
    