import numpy as np
import math
from qstrader.signals.signal import Signal

class ASSignal(Signal):

    def __init__(self, broker, **kwargs):
        
        self.name = 'AS model'
        self.broker = broker
       
        self.signals = self._initial_signals()
        self.strategy_result = self._initial_strategy_result()
        # self.signals_list = self._initial_signals_list()
        
    def _initial_signals(self):
        
        signals = {'order_type' : 'delay', 'buy_price' : 0, 'buy_volume' : 0, 'sell_price' : 0, 'sell_volume' : 0}
       
        return signals
    
    def _initial_signals_list(self):

        signals_list = [['index', 'Direction', 'Price', 'Volume']]
       
        return signals_list
    
    
    def _initial_strategy_result(self):
        
        result = [np.nan, np.nan, np.nan]
        
        return result
        
    def _get_as_model_result(self, price, position, step, T = 4803, sigma = 0.001, dt = 1, gamma = 0.01, k = 2, A = 140):
        
        reservation_price = price - position*gamma*sigma**2*(T- step*dt)/T
        
        spread = (gamma*sigma**2*(T-step*dt) + (2/gamma)*math.log(1 + gamma/k))/2
        
        if reservation_price >= price:
            ask_spread = spread + reservation_price - price
            bid_spread = spread - reservation_price + price
        else:
            ask_spread = spread - price + reservation_price
            bid_spread = spread + price - reservation_price
            
        price_ask = round(price + ask_spread,3)
        price_bid = round(price - bid_spread,3)
        
        return price_ask,price_bid
        
        
    def _update_strategy_result(self):
        
        latest_price = self.broker.latest_data['snapshot'][1]
        
        if self.broker.current_k >= 100:
            slice_data = self.broker.get_slice_data(1200)
            price_slice = [x[1] for x in slice_data]
        else:
            slice_data = []
            
        if ~np.isnan(latest_price):
            
            position = self.broker.get_account_position()
            
            if len(slice_data) >= 100:
                volatility = np.std(price_slice)
            else:
                volatility = 0.001
                
            price_ask,price_bid = self._get_as_model_result(latest_price, position, self.broker.current_k, T = self.broker.data_handler.data_source.data_info['NK'][self.broker.current_day], sigma = volatility)
            
            self.strategy_result = [self.broker.current_time, price_ask, price_bid]
    
    def _update_signals(self):
            
        signal = self.signals
        strategy_result = self.strategy_result
        
        latest_data = self.broker.latest_data['snapshot']
        if ~np.isnan(latest_data[1]) and latest_data[2] > 0:# price not na and volume > 0
        
            if ~np.isnan(strategy_result[1]) and self.broker.portfolio['available_volume'] > 0:
                #sell
                sim_volume = self.broker._simulated_volume(latest_data[2]) #volume
                # if latest_data.index[-1].hour == 9 and latest_data.index[-1].minute == 25:
                    # signal['order_type'] = 'now'
                # else:
                signal['order_type'] = 'delay'
                signal['sell_price'] = strategy_result[1]
                signal['sell_volume'] = sim_volume
                
                # print([-1, strategy_result[1], sim_volume])
                    
            if ~np.isnan(strategy_result[2]):
                #buy

                sim_volume = self.broker._simulated_volume(latest_data[2])
                # if latest_data.index[-1].hour == 9 and latest_data.index[-1].minute == 25:
                #     signal['order_type'] = 'now'
                # else:
                signal['order_type'] = 'delay'
                signal['buy_price'] = strategy_result[2]
                signal['buy_volume'] = sim_volume
                        
                # print([1, strategy_result[2], sim_volume])
                        
        # self._add_data_to_signals_list()
        
    def _execute_signals(self):
                
        signals = self.signals
            
        if signals['sell_volume'] > 0 and self.broker.portfolio['available_volume'] > 0:
            self.broker.update_order(order_type = signals['order_type'],
                                     sell_price = signals['sell_price'],
                                     sell_volume = signals['sell_volume'])
            
            # print(['signals:', signals['sell_price'], signals['sell_volume']])
            # portfolio 的更新应放在真正update到order时
            # self.broker.update_portfolio(available_volume = -signals['sell_volume'],
                                         # frozen_volume = signals['sell_volume'])
            
        if signals['buy_volume'] > 0:
            consideration = signals['buy_price'] * signals['buy_volume']
            total_commission = self.broker.fee_model.calc_total_cost(consideration)
            
            total_cost =  round(consideration + total_commission,4)
                
            self.broker.update_order(order_type = signals['order_type'],
                                     buy_price = signals['buy_price'],
                                     buy_volume = signals['buy_volume'],
                                     frozen_money = total_cost)
            
            # self.broker.update_cash(cash = - total_cost)
            
        #初始化
        self.signals = {'buy_price' : 0, 'buy_volume' : 0, 'sell_price' : 0, 'sell_volume' : 0}

                
    def _add_data_to_signals_list(self):
        
        signals_list = self.signals_list
        signals = self.signals
        if len(signals_list) > 1:
            if signals_list[-1][0] > self.broker.current_time:
                raise ValueError('时间戳错误，添加数据失败')
                
        if signals['sell_volume'] != 0:
            signals_list.append([self.broker.current_time, -1, signals['sell_price'], signals['sell_volume']])
        if signals['buy_volume'] != 0:
            signals_list.append([self.broker.current_time, 1, signals['buy_price'], signals['buy_volume']])
    
    def update(self):
        
        self._update_strategy_result()
        self._update_signals()
        
        self._execute_signals()