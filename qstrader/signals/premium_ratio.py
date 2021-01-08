import numpy as np
from qstrader.signals.signal import Signal

class PremiumRatioSignal(Signal):

    def __init__(self, broker, **kwargs):
        
        self.name = 'PremiumRatio'
        self.broker = broker
       
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
            result[stock] = [np.nan, np.nan]
        
        return result
        
    
    def _get_latest_premium_ratio(self):
        
        latest_premium_ratio = {}
        sorted_stock = []
        for stock in self.broker.stocks:
            latest_data = self.broker.data_handler.get_stock_latest_data(stock, self.broker.current_k)
            if np.isnan(latest_data['PremiumRatio'][0]) == False:
                latest_premium_ratio[stock] = latest_data['PremiumRatio'][0]
        
        for stock,premium_ratio in latest_premium_ratio.items():
            if premium_ratio > 1:
                sorted_stock.append(stock)
        #备注是高回报版本
        # sorted_premium_ratio = sorted(latest_premium_ratio.items(), key = lambda kv:kv[1], reverse = True)
        
        # for i in range(len(sorted_premium_ratio)):
        #     sorted_stock.append(sorted_premium_ratio[i][0])
                

        # sorted_stock = sorted_stock[:30]
        
        return sorted_stock

    
    def _update_strategy_result(self):
        
        strategy_result = self.strategy_result
        sorted_stock = self._get_latest_premium_ratio()
        
        for stock in self.broker.stocks:
            
            latest_data = self.broker.data_handler.get_stock_latest_data(stock, self.broker.current_k)
            
            if ~np.isnan(latest_data.Close[0]):
                if stock in sorted_stock:
                    strategy_result[stock]= [self.broker.current_time, 1]
                else:
                    strategy_result[stock]= [self.broker.current_time, -1]
                        
    def _get_signals_number(self):
        
        buy_list = []
        buy_add_list = []
        sell_list = []
        for stock in self.broker.stocks:
            strategy_result = self.strategy_result[stock]
            if strategy_result[1] == 1:
                buy_list.append(stock)
            elif strategy_result[1] == -1:
                sell_list.append(stock)
        
        for stock in self.broker.stocks:
            strategy_result = self.strategy_result[stock]
            if strategy_result[1] == 1:
                latest_data = self.broker.data_handler.get_stock_latest_data(stock, self.broker.current_k)
                total_sim_volume = self.broker._simulated_volume(self.broker.get_account_total_equity()/len(buy_list),latest_data.Close[0])
                if total_sim_volume > self.broker.portfolio[stock]['total_volume']:
                    buy_add_list.append(stock)
        
        return buy_list,buy_add_list,sell_list
    
    def _update_signals(self):

        buy_list,buy_add_list,sell_list = self._get_signals_number()
        
        for stock in self.broker.stocks:
            
            signal = self.signals[stock]
            strategy_result = self.strategy_result[stock]
            latest_data = self.broker.data_handler.get_stock_latest_data(stock, self.broker.current_k)            
            
            if ~np.isnan(latest_data.Close[0]) and ~np.isnan(strategy_result[1]):
                
                if strategy_result[1] == -1 and self.broker.portfolio[stock]['available_volume'] > 0:
                    #sell
                    sim_volume = self.broker.portfolio[stock]['available_volume']
                    
                    signal['Direction'] = -1
                    signal['Price'] = latest_data.Close[0]
                    signal['Volume'] = sim_volume
                    
                    print([stock, signal['Direction'], signal['Price'], signal['Volume']])
                        
                elif strategy_result[1] == 1:
                    #buy
                    total_sim_volume = self.broker._simulated_volume(self.broker.get_account_total_equity()/len(buy_list),latest_data.Close[0])
                    if stock in buy_add_list:
                        sim_volume = self.broker._simulated_volume(self.broker.cash/len(buy_add_list),latest_data.Close[0])
                        sim_volume = min(sim_volume, total_sim_volume - self.broker.portfolio[stock]['total_volume'])

                        if sim_volume > 0:
                            signal['Direction'] = 1
                            signal['Price'] = latest_data.Close[0]
                            signal['Volume'] = sim_volume
                            
                            print([stock, signal['Direction'], signal['Price'], signal['Volume']])
                            
                    elif total_sim_volume < self.broker.portfolio[stock]['total_volume']:
                        signal['Direction'] = -1
                        signal['Price'] = latest_data.Close[0]
                        signal['Volume'] = self.broker.portfolio[stock]['total_volume'] - total_sim_volume
                        
                        print([stock, signal['Direction'], signal['Price'], signal['Volume']])
                    # sim_volume = self.broker._simulated_volume(self.broker.cash/len(buy_list),latest_data.Close[0])
                    
                    # if sim_volume > 0:
                    #     signal['Direction'] = 1
                    #     signal['Price'] = latest_data.Close[0]
                    #     signal['Volume'] = sim_volume
                        
                    #     print([stock, signal['Direction'], signal['Price'], signal['Volume']])

        self._add_data_to_signals_list()
        
    def _execute_signals(self):
        
        buy_list,buy_add_list,sell_list = self._get_signals_number()
        
        for stock in self.broker.stocks:
            if stock in sell_list:
                
                signal = self.signals[stock] 
                                      
                if signal['Direction'] == -1 and np.isnan(signal['Price']) == False and self.broker.portfolio[stock]['available_volume'] > 0:
                    self.broker.update_order(stock,
                                             sell_price = signal['Price'],
                                             sell_volume = signal['Volume'])
                    self.broker.update_portfolio(stock, 
                                                 available_volume = -signal['Volume'],
                                                 frozen_volume = signal['Volume'])
                    #初始化
                    self.signals[stock] = {'Direction' : np.nan, 'Price' : np.nan, 'Volume' : np.nan }
                    
        for stock in self.broker.stocks:
            if stock in buy_list:    
                signal = self.signals[stock]
                if signal['Direction'] == 1 and np.isnan(signal['Price']) == False:
                    
                    consideration = signal['Price'] * signal['Volume']
                    total_commission = self.broker.fee_model.calc_total_cost(consideration)
                    
                    total_cost =  round(consideration + total_commission,4)
                        
                    self.broker.update_order(stock, 
                                             buy_price = signal['Price'],
                                             buy_volume = signal['Volume'],
                                             frozen_money = total_cost)
                    
                    self.broker.update_cash(cash = - total_cost)
                    
                    #初始化
                    self.signals[stock] = {'Direction' : np.nan, 'Price' : np.nan, 'Volume' : np.nan }
                

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