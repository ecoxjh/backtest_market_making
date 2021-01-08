import os
import numpy as np
import pandas as pd
import datetime
from qstrader import settings

class CSVDataSourceSelect(object):
    def __init__(self, csv_dir, start_time = None, end_time = None):
        self.csv_dir = csv_dir

        self.data = self._load_csvs_into_dfs(start_time, end_time)
        self.data_info = self._get_data_info()
        self.data_info['StartTime'] = start_time
        self.data_info['EndTime'] = end_time
        
    def _obtain_stock_csv_files(self):
        return [
            file for file in os.listdir(self.csv_dir)
            if file.endswith('.csv')
            ]

    def _obtain_stock_symbol_from_filename(self, csv_file):
        
        return '%s' % csv_file.replace('.csv', '')

    def _load_csv_into_df(self, csv_file, start_time = None, end_time = None):
       
        csv_df = pd.read_csv(
            os.path.join(self.csv_dir, csv_file),
            index_col= 0,
            parse_dates=True
        ).sort_index()
        
        if start_time is not None:
            
            csv_df = csv_df[csv_df.index >= start_time]
        
        if end_time is not None:
            
            if len(end_time) == 9 or len(end_time) == 10:
                end_time = datetime.datetime.strptime(end_time,'%Y-%m-%d') + datetime.timedelta(days = 1)
                
            csv_df = csv_df[csv_df.index <= end_time]
        
        return csv_df

    def _load_csvs_into_dfs(self, start_time, end_time):
        
        if settings.PRINT_EVENTS:
            print("读取CSV文件...")
            
        # 得到csv文件名称 

        csv_files = self._obtain_stock_csv_files()
        # csv_filters = pd.read_excel('data\\kzz.xlsx',index_col= 1,parse_dates=True).sort_index()
        csv_dfs = {}
        
        for csv_file in csv_files:
            stock_symbol = self._obtain_stock_symbol_from_filename(csv_file)
            # csv_filter = csv_filters[csv_filters['证券代码'] == stock_symbol]
            
            csv_df = self._load_csv_into_df(csv_file, start_time, end_time)
            # if csv_filter['每股净利润同比增长'][0] < 0.75 and csv_filter['每股净利润同比增长'][0] > 0.5:
            if csv_df['MarketValue'].iloc[0] <= 12 and csv_df['MarketValue'].iloc[0] >= 8:

                    
                csv_df.rename(columns = {'CLOSE':'Close','CONVPRICE':'ConversionPrice', 'Conversion_date':'ConversionDate'},
                              inplace = True)
                
                csv_dfs[stock_symbol] = csv_df
                
                if settings.PRINT_EVENTS:
                    print("加载 '%s'数据..." % stock_symbol)                
            else:
                if settings.PRINT_EVENTS:
                    print("剔除 '%s'数据..." % stock_symbol)
            
        return csv_dfs
    
    def _get_data_info(self):
        
        data_info = {}
        #取第一只票得到info
        data_info['Stocks'] = list(self.data.keys())
        symbol = data_info['Stocks'][0]
        data_info['Fields'] = list(self.data[symbol])
        data_info['NK'] = len(self.data[symbol])
        data_info['Index'] = self.data[symbol].index

        index = list(map(datetime.datetime.date,self.data[symbol].index))
        data_info['NMonth'], data_info['MonthIndex'] = self._get_different_month_index_list(index)
        
        return data_info
    
    def _get_different_month_index_list(self, index):
        
        different_value_index = [0]
        
        for i in range(1,len(index)):
            if index[i].month != index[i-1].month:
                different_value_index.append(i)

        if len(different_value_index) == 0:
            different_value_index = [0]
        return len(different_value_index), different_value_index
    