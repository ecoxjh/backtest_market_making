class BacktestDataHandler(object):

    def __init__(self, data_source=None):
        
        self.data_source = data_source
    
    def get_stock_latest_data(self, symbol, current_k):
    
        latest_data = {}
        latest_data['snapshot'] = self.data_source.data[symbol]['snapshot']['data'][current_k:current_k+1][0]
        if self.data_source.data_info['TickIndex'][symbol][current_k] is not None:
            tick_index_end = self.data_source.data_info['TickIndex'][symbol][current_k] + 1
            
            if current_k == 0 or self.data_source.data_info['TickIndex'][symbol][current_k-1] is None:
                latest_data['tick'] = self.data_source.data[symbol]['tick']['data'][:tick_index_end]
            else:
                tick_index_start = self.data_source.data_info['TickIndex'][symbol][current_k-1] + 1
                latest_data['tick'] = self.data_source.data[symbol]['tick']['data'][tick_index_start:tick_index_end]

        return latest_data

    def get_stock_slice_data(self, symbol, n_slice, current_k):
        
        slice_data = []
        tick_index_end = self.data_source.data_info['TickIndex'][symbol][current_k] + 1
        
        if self.data_source.data[symbol]['tick']['data'] is not None:
            if current_k < n_slice - 1:
                slice_data = self.data_source.data[symbol]['tick']['data'][:tick_index_end]
            else:
                slice_data = self.data_source.data[symbol]['tick']['data'][current_k - n_slice + 1 : tick_index_end]
        else:
            raise ValueError('股票数据为空')
        if slice_data is None:
            raise ValueError('切片数据为空')
        return slice_data