import numpy as np
from qstrader.signals.signal import Signal

class DTSignal(Signal):

    def __init__(self, broker, prior_day = 3, K1 = 0.35, K2 = 0.35, **kwargs):
        
        self.name = 'Dual Thrust'
        self.broker = broker
        if 'prior_day' in kwargs.keys():
            self.prior_day = kwargs['prior_day']
        else:
            self.prior_day = prior_day
        if 'K1' in kwargs.keys():
            self.K1 = kwargs['K1']
        else:    
            self.K1 = K1
        if 'K2' in kwargs.keys():
            self.K2 = kwargs['K2']
        else:
            self.K2 = K2
        self.signals = self._initial_signals()
        self.signals_list = self._initial_signals_list()
        self.strategy_result = self._initial_strategy_result()
        
    def _initial_signals(self):
        stocks = self.broker.stocks
        signals = {}
        for stock in stocks:
            signals[stock] = {'Direction' : np.nan, 'Price' : np.nan, 'Volume' : np.nan }
       
        return signals
    def _initial_signals_list(self):

        signals_list = [['index', 'stock', 'Direction', 'Price', 'Volume']]
       
        return signals_list
    
    def _initial_strategy_result(self):
        
        result = {}
        
        for stock in self.broker.stocks:
            result[stock] = [np.nan, np.nan, np.nan]
        
        return result
        
    def _get_strategy_start_k(self):
        
        prior_day = self.prior_day
        DayIndex = self.broker.data_handler.data_source.data_info['DayIndex']
        start_k = DayIndex[self.broker.current_day - prior_day]
        
        return start_k
    
    def _update_strategy_result(self):
        
        if self.broker.current_k in self.broker.data_handler.data_source.data_info['DayIndex']:
            strategy_result = self.strategy_result
            start_k = self._get_strategy_start_k()
            for stock in self.broker.stocks:
                price_buffers = self._get_stock_price_buffers(stock , self. broker.current_k - start_k + 1, self.broker.current_k)
            
                N = len(price_buffers)
                HH = max(price_buffers.high[0:N])
                LC = min(price_buffers.close[0:N])
                HC = max(price_buffers.close[0:N])
                LL = min(price_buffers.low[0:N])
            
                Range = max([HH-LC,HC-LL])
            
                Cap = price_buffers.open[-1] + self.K1 * Range
                Floor = price_buffers.open[-1] - self.K2 * Range
            
                strategy_result[stock] = [self.broker.current_day, Cap, Floor]
            
    def _update_signals(self):
        
        for stock in self.broker.stocks:
            signal = self.signals[stock]
            strategy_result = self.strategy_result[stock]
            latest_data = self.broker.data_handler.get_stock_latest_data(stock, self.broker.current_k)
            if ~np.isnan(latest_data.close[0]) and ~np.isnan(strategy_result[1]):
                if latest_data.close[0] > strategy_result[1]:
                    #buy
                    sim_volume = self.broker._simulated_volume(latest_data.volume[0])
        
                    signal['Direction'] = 1
                    signal['Price'] = latest_data.close[0]
                    signal['Volume'] = sim_volume
                    
                    # print([stock, signal['Direction'], signal['Price'], signal['Volume']])
                elif latest_data.close[0] < strategy_result[2] and self.broker.portfolio[stock]['available_volume'] > 0:
                    #sell
                    sim_volume = min(self.broker.portfolio[stock]['available_volume'], self.broker._simulated_volume(latest_data.volume[0]))
                    
                    signal['Direction'] = -1
                    signal['Price'] = latest_data.close[0]
                    signal['Volume'] = sim_volume
                    # print([stock, signal['Direction'], signal['Price'], signal['Volume']])
          
    def _execute_signals(self):
        
        for stock in self.broker.stocks:
            
            signal = self.signals[stock]
            if signal['Direction'] == 1:
                consideration = signal['Price'] * signal['Volume']
                total_commission = self.broker.fee_model.calc_total_cost(consideration)
                total_cost =  round(consideration + total_commission,4)
                self.broker.update_order(stock, 
                                         buy_price = signal['Price'],
                                         buy_volume = signal['Volume'],
                                         frozen_money = total_cost)
                
                self.broker.update_cash(cash = - signal['Price'] * signal['Volume'])
                
            elif signal['Direction'] == -1:
                self.broker.update_order(stock,
                                         sell_price = signal['Price'],
                                         sell_volume = signal['Volume'])
                self.broker.update_portfolio(stock, 
                                             available_volume = -signal['Volume'],
                                             frozen_volume = signal['Volume'])
                
        self._add_data_to_signals_list()
        #初始化signals
        self.signals = self._initial_signals()
        
    def _add_data_to_signals_list(self):
        
        signals_list = self.signals_list
        
        if len(signals_list) > 1:
            if signals_list[-1][0] > self.broker.current_time:
                raise ValueError('时间戳错误，添加数据失败')
        for stock in self.broker.stocks:       
            signals_list.append([self.broker.current_time, stock] + 
                                list(self.signals[stock].values()))
    def update(self):
        """
        update to order
        """
        
        self._update_strategy_result()
        self._update_signals()
        
        self._execute_signals()