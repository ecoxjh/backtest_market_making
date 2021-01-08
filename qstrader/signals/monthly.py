
import numpy as np
from qstrader.signals.signal import Signal

class MonthSignal(Signal):

    def __init__(self, broker, **kwargs):
        
        self.name = 'Month'
        self.broker = broker
       
        self.stock_list = self._initial_stock_list()
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
    
    def _initial_stock_list(self):
        
        stock_list = [['000951.SZ','002555.SZ','002531.SZ','300450.SZ','600309.SH','000910.SZ','300122.SZ','002602.SZ','600036.SH','300699.SZ'],
         ['002603.SZ','002223.SZ','002007.SZ','300676.SZ','002030.SZ','300450.SZ','002214.SZ','300578.SZ','002555.SZ','600036.SH'],
         ['000625.SZ','000733.SZ','300250.SZ','603686.SH','300059.SZ','300699.SZ','002352.SZ','600048.SH','002254.SZ','601186.SH'],
         ['601696.SH','601607.SH','000063.SZ','002352.SZ','300783.SZ','002007.SZ','603111.SH','002555.SZ','600600.SH','002375.SZ'],
         ['601066.SH','600585.SH','603986.SH','002129.SZ','002421.SZ','002007.SZ','600480.SH','002410.SZ','000100.SZ','600600.SH'],
         ['300783.SZ','002508.SZ','600126.SH','688086.SH','688268.SH','600161.SH','002410.SZ','002541.SZ','300363.SZ','688278.SH'],
         ['688088.SH','688369.SH','688157.SH','688020.SH','688321.SH','300118.SZ','300303.SZ','600918.SH','688266.SH','601788.SH'],
         ['601636.SH','300146.SZ','002866.SZ','600496.SH','002030.SZ','603712.SH','603733.SH','002531.SZ','300598.SZ','603987.SH'],
         ['000876.SZ','600030.SH','002151.SZ','601628.SH','300122.SZ','000001.SZ','300806.SZ','300103.SZ','600976.SH','300085.SZ'],
         ['603686.SH','002045.SZ','002625.SZ','002410.SZ','002797.SZ','300251.SZ','300760.SZ','002475.SZ','002594.SZ','300509.SZ']]
        
        return stock_list
    
    def _initial_strategy_result(self):
        
        result = {}
        
        for stock in self.broker.stocks:
            result[stock] = [np.nan, np.nan]
        
        return result
        
    
    def _update_strategy_result(self):
        
        if self.broker.current_k in self.broker.data_handler.data_source.data_info['MonthIndex']:
            strategy_result = self.strategy_result
            
            for stock in self.broker.stocks:
                if stock in self.stock_list[self.broker.current_month]:
                    strategy_result[stock] = [self.broker.current_month, 1]
                    
                elif np.isnan(strategy_result[stock][1]) == True:
                    strategy_result[stock] = [self.broker.current_month,np.nan]
                    
        elif self.broker.current_k + 1 in self.broker.data_handler.data_source.data_info['MonthIndex']:
            
            strategy_result = self.strategy_result
            
            for stock in self.broker.stocks:
                if stock in self.stock_list[self.broker.current_month]:
                    
                    #连续持有
                    if stock in self.stock_list[self.broker.current_month + 1]:
                        strategy_result[stock] = [self.broker.current_month, -2]
                    else:
                        strategy_result[stock] = [self.broker.current_month, -1]
                else:
                    strategy_result[stock] = [self.broker.current_month,np.nan]
                    
    def _update_signals(self):
        
        for stock in self.broker.stocks:
            signal = self.signals[stock]
            strategy_result = self.strategy_result[stock]
            latest_data = self.broker.data_handler.get_stock_latest_data(stock, self.broker.current_k)
            
            if ~np.isnan(latest_data.close[0]) and ~np.isnan(strategy_result[1]):
                if strategy_result[1] == 1:
                    #buy
                    equity = self.broker.get_account_total_equity()
                    sim_volume = self.broker._simulated_volume(equity/10,latest_data.amt[0]/latest_data.volume[0])
        
                    signal['Direction'] = 1
                    signal['Price'] = latest_data.amt[0]/latest_data.volume[0]
                    signal['Volume'] = sim_volume - self.broker.portfolio[stock]['total_volume']
                    
                    print([stock, signal['Direction'], signal['Price'], signal['Volume']])
                    
                    
                elif strategy_result[1] == -1 and self.broker.portfolio[stock]['available_volume'] > 0 :
                    #sell
                    sim_volume = self.broker.portfolio[stock]['available_volume']
                    
                    signal['Direction'] = -1
                    signal['Price'] = latest_data.amt[0]/latest_data.volume[0]
                    signal['Volume'] = sim_volume
                    print([stock, signal['Direction'], signal['Price'], signal['Volume']])
                    
                elif strategy_result[1] == -2 and self.broker.portfolio[stock]['available_volume'] > 0 :
                    
                    #连续持有
                    #估计买入量
                    equity = self.broker.get_account_total_equity()
                    sim_volume = self.broker._simulated_volume(equity/10, latest_data.amt[0]/latest_data.volume[0])
                        
                    if sim_volume < self.broker.portfolio[stock]['available_volume']:
                        
                        signal['Direction'] = -1
                        signal['Price'] = latest_data.amt[0]/latest_data.volume[0]
                        signal['Volume'] = self.broker.portfolio[stock]['available_volume'] - sim_volume
                        
                        print([stock, signal['Direction'], signal['Price'], signal['Volume']])
                    
                    

    def _execute_signals(self):
        
        for stock in self.broker.stocks:
            
            signal = self.signals[stock]
            
            if np.isnan(signal['Direction']) == False and (np.isnan(signal['Price']) == True or np.isnan(signal['Volume']) == True):
                # na 重新发信号
                latest_data = self.broker.data_handler.get_stock_latest_data(stock, self.broker.current_k)
                signal['Price'] = latest_data.amt[0]/latest_data.volume[0]
                
            if signal['Direction'] == 1 and np.isnan(signal['Price']) == False:
                consideration = signal['Price'] * signal['Volume']
                total_commission = self.broker.fee_model.calc_total_cost(consideration)
                total_cost =  round(consideration + total_commission,4)
                self.broker.update_order(stock, 
                                         buy_price = signal['Price'],
                                         buy_volume = signal['Volume'],
                                         frozen_money = total_cost)
                
                self.broker.update_cash(cash = - signal['Price'] * signal['Volume'])
                
                #初始化
                self.signals[stock] = {'Direction' : np.nan, 'Price' : np.nan, 'Volume' : np.nan }
                
            elif signal['Direction'] == -1 and np.isnan(signal['Price']) == False:
                self.broker.update_order(stock,
                                         sell_price = signal['Price'],
                                         sell_volume = signal['Volume'])
                self.broker.update_portfolio(stock, 
                                             available_volume = -signal['Volume'],
                                             frozen_volume = signal['Volume'])
                #初始化
                self.signals[stock] = {'Direction' : np.nan, 'Price' : np.nan, 'Volume' : np.nan }
                
        self._add_data_to_signals_list()
        
        #初始化signals
        self.strategy_result = self._initial_strategy_result()
    
        
    def _add_data_to_signals_list(self):
        
        signals_list = self.signals_list
        
        if len(signals_list) > 1:
            if signals_list[-1][0] > self.broker.current_time:
                raise ValueError('时间戳错误，添加数据失败')
        for stock in self.broker.stocks:       
            signals_list.append([self.broker.current_time, stock] + 
                                list(self.signals[stock].values()))
    def update(self):

        
        self._update_strategy_result()
        self._update_signals()
        
        self._execute_signals()