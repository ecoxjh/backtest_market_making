import numpy as np
from scipy.stats import norm
from qstrader.signals.signal import Signal

class MHMASignal(Signal):

    def __init__(self, broker, prior_day = 15, K1 = 0.2, K2 = 0.8, **kwargs):
        
        self.name = 'MY_HMA_TREND_STRATEGY'
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
        self.median_data = self._initial_median_data()
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
            result[stock] = [np.nan, [np.nan], [np.nan]]
        
        return result
        
    def _get_strategy_start_k(self):
        
        prior_day = self.prior_day
        DayIndex = self.broker.data_handler.data_source.data_info['DayIndex']
        start_k = DayIndex[self.broker.current_day - prior_day]
        
        return start_k
    
    def _initial_median_data(self):
        if self.prior_day > self.broker.current_day:
            raise ValueError('prior_day设置错误')
        init_data = {'data_k' : self.broker.current_k}
        for stock in self.broker.stocks:      
            init_data[stock] = {}
            price_buffers = self._get_stock_price_buffers(stock, self.broker.current_k + 1, self.broker.current_k)
            init_data[stock]['avg_price'] = list((price_buffers.high + price_buffers.low + price_buffers.close)/3)
            init_data[stock]['log_price'] = self._get_init_log(init_data[stock]['avg_price'])
            init_data[stock]['price_wma'] = self._get_init_wma(init_data[stock]['log_price'])
            init_data[stock]['price_hma'] = self._get_init_hma(init_data[stock]['price_wma'])
            init_data[stock]['price_trend_score'] = self._get_init_trend_score(init_data[stock]['price_hma'])
            init_data[stock]['price_score'] = self._get_init_score(init_data[stock]['price_trend_score'])
            
            init_data[stock]['volume'] = list(price_buffers.volume)
            init_data[stock]['volume_wma'] = self._get_init_wma(list(price_buffers.volume))
            init_data[stock]['volume_hma'] = self._get_init_hma(init_data[stock]['volume_wma'])
            init_data[stock]['volume_trend_score'] = self._get_init_trend_score(init_data[stock]['volume_hma'])
            init_data[stock]['volume_score'] = self._get_init_score(init_data[stock]['volume_trend_score'])
            
        return init_data
    
    def _update_strategy_result(self):
        
        strategy_result = self.strategy_result
        
        self._update_median_data()
        
        for stock in self.broker.stocks:     
            strategy_result[stock] = [self.broker.current_k, 
                                     self.median_data[stock]['price_score'][-3:],
                                     self.median_data[stock]['volume_score'][-3:]]
            
    def _update_median_data(self):
        
        median_data = self.median_data
        
        if self.broker.current_k == median_data['data_k'] + 1:
            
            median_data['data_k'] = self.broker.current_k
            
            for stock in self.broker.stocks:
                latest_data = self.broker.data_handler.get_stock_latest_data(stock,self.broker.current_k)
                median_data[stock]['avg_price'].append(((latest_data.high + latest_data.low + latest_data.close)/3).iloc[0])
                median_data[stock]['log_price'].append(self._get_log(median_data[stock]['avg_price']))
                median_data[stock]['price_wma'].append(self._get_wma(median_data[stock]['log_price']))
                median_data[stock]['price_hma'].append(self._get_hma(median_data[stock]['price_wma']))
                median_data[stock]['price_trend_score'].append(self._get_trend_score(median_data[stock]['price_trend_score']))
                median_data[stock]['price_score'].append(self._get_score(median_data[stock]['price_trend_score']))
                
                median_data[stock]['volume'].append(latest_data.volume.iloc[0])
                median_data[stock]['volume_wma'].append(self._get_wma(median_data[stock]['volume']))
                median_data[stock]['volume_hma'].append(self._get_hma(median_data[stock]['volume_wma']))
                median_data[stock]['volume_trend_score'].append(self._get_trend_score(median_data[stock]['volume_hma']))
                median_data[stock]['volume_score'].append(self._get_score(median_data[stock]['volume_trend_score']))
        else:
            raise ValueError('数据错误 current_k :%s , data_k: %s'%(self.broker.current_k, median_data['data_k']))
        
    def _get_log(self, data):
        
        diff = np.diff([data[0]] + data)
        median_bar = np.median(diff)
        sd_bar = 1.483*np.median(abs(diff - median_bar))
        
        if sd_bar == 0:
            sd_bar = np.std(diff)
        if diff[-1] >= median_bar + 0.618*sd_bar:
            res = (((diff[-1] - median_bar)/sd_bar + 1)**1.483 - 1 + median_bar)*sd_bar
        elif diff[-1] <= median_bar - 0.618*sd_bar:
            res = (-((median_bar - diff[-1])/sd_bar + 1)**1.483 + 1 + median_bar)*sd_bar
        else:
            if diff[-1] >= median_bar:
                res = (((diff[-1] - median_bar)/sd_bar + 1)**0.618 - 1 + median_bar)*sd_bar
            else:
                res = (-((median_bar -diff[-1])/sd_bar + 1)**0.618 + 1 + median_bar)*sd_bar
                
        return res + data[-2]
    
    def _get_init_log(self, data):
        T = len(data)
        res_data = data[:]
        
        if T < 30:
            return res_data
        else:
            for i in range(30,T+1):
                
                res_data[i-1] = self._get_log(data[:i])
                
            return res_data
    
    def _get_wma(self, data):
        
        wma3 = np.average(data[-10:], weights = range(2,21,2))
        wma2 = np.average(data[-15:], weights = range(2,31,2))
        wma1 = np.average(data[-30:], weights = range(2,61,2))
        wma0 = np.average(data[-60:], weights = range(2,121,2))
            
        wma = 4*wma3 - 2*wma2 - 2*wma1 + wma0
        
        return wma
    
    def _get_init_wma(self, data):
        T = len(data)
        
        res = data[:]
        if T < 60:
            return res
        else:
            for i in range(60,T+1):
                res[i-1] = self._get_wma(data[:i])
                
        return res
    
    def _get_hma(self, data):
        
        hma = np.average(data[-4:], weights = range(2,9,2))
        
        return hma
    
    def _get_init_hma(self, data):
        T = len(data)
        
        res = data[:]
        if T < 60:
            return res
        else:
            for i in range(60,T+1):
                        
                res[i-1] = self._get_hma(data[:i])
            
        return res
    
    def _get_trend_score(self, data):
        
        median_bar = np.median(data[1:])
        sd_bar = 1.483*np.median(abs(data[1:] - median_bar))
        trend_score = norm.cdf(data[-1], median_bar, sd_bar)
        
        return trend_score
    
    def _get_init_trend_score(self, data):
        
        T = len(data)
        
        trend_score = [0.5 for i in range(T)]
        diff = np.diff([data[0]] + data)
        
        for i in range(8,T+1):
            trend_score[i-1] = self._get_trend_score(diff[:i])
            
        return trend_score
    
    def _get_score(self, data):
        
        score = np.average(data[-15:], weights = range(2,31,2))
        
        return score
    
    def _get_init_score(self, data):
        
        T = len(data)
        score = [0.5 for i in range(T)]
        
        for t in range(15,T+1):
            
            score[t-1] = self._get_score(data[:t])
            
        return score
        
    def _update_signals(self):
        
        for stock in self.broker.stocks:
            signal = self.signals[stock]
            strategy_result = self.strategy_result[stock]
            latest_data = self.broker.data_handler.get_stock_latest_data(stock, self.broker.current_k)
            if ~np.isnan(latest_data.close[0]) and ~np.isnan(strategy_result[1][-1]):
                
                if strategy_result[2][-1] > self.K2 or strategy_result[2][-1] < self.K1:
                    if strategy_result[1][-1] < self.K1:
                        #buy
                        sim_volume = self.broker._simulated_volume(latest_data.volume[0])
            
                        signal['Direction'] = 1
                        signal['Price'] = latest_data.close[0]
                        signal['Volume'] = sim_volume
                    
                        print([stock, signal['Direction'], signal['Price'], signal['Volume']])
                        
                elif strategy_result[2][-1] > self.K2 or strategy_result[2][-1] < self.K1:
                    if strategy_result[1][-1] > self.K2 and self.broker.portfolio[stock]['available_volume'] > 0:
                        #sell
                        sim_volume = min(self.broker.portfolio[stock]['available_volume'], self.broker._simulated_volume(latest_data.volume[0]))
                        
                        signal['Direction'] = -1
                        signal['Price'] = latest_data.close[0]
                        signal['Volume'] = sim_volume
                        print([stock, signal['Direction'], signal['Price'], signal['Volume']])
              
    def _execute_signals(self):
        
        for stock in self.broker.stocks:
            
            signal = self.signals[stock]
            if signal['Direction'] == 1:
                
                consideration = signal['Price'] * signal['Volume']
                total_commission = self.broker.fee_model.calc_total_cost(consideration)
                total_cost =  round(consideration + total_commission,4)
                if self.broker.cash > total_cost:
                    self.broker.update_order(stock, 
                                         buy_price = signal['Price'],
                                         buy_volume = signal['Volume'],
                                         frozen_money = total_cost)
                
                    self.broker.update_cash(cash = - total_cost)
                
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
        if self.broker.current_k != self.broker.start_k - 1 :
            self._update_strategy_result()
            
        self._update_signals()
        
        self._execute_signals()