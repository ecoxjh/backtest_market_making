import numpy as np
import math
from qstrader.signals.signal import Signal

class RootNetSignal(Signal):
    '''
    根网 价位策略
    '''
    def __init__(self, broker, **kwargs):
        
        self.name = 'rootnet strategy'
        self.broker = broker
       
        self.signals = self._initial_signals()
        self.signals_list = self._initial_signals_list()
        self.strategy_result = self._initial_strategy_result()
        self.select = self._initial_strategy_select(**kwargs)
        
    def _initial_signals(self):
        
        signals = {'order_type' : 'delay', 'buy_price' : [], 'buy_volume' : [], 'sell_price' : [], 'sell_volume' : []}
       
        return signals
    
    def _initial_signals_list(self):

        signals_list = [['index', 'Direction', 'Price', 'Volume']]
       
        return signals_list
    
    
    def _initial_strategy_result(self):
        
        result = [np.nan, np.nan, np.nan]
        
        return result
    
    def _initial_strategy_select(self, **kwargs):
        
        select = {'bid_price':None,'bid_price_kwargs':None,
                  'ask_price':None,'ask_price_kwargs':None,
                  'bid_volume':None,'bid_volume_kwargs':None,
                  'ask_volume':None,'ask_volume_kwargs':None}
        # 默认 spread
        i_kwargs = {k:v for (k,v) in kwargs.items() if k in ['spread_level','bid_change_level','min_bid_unit']}
        select['bid_price'] = self._get_spread_model_result
        select['bid_price_kwargs'] = i_kwargs
        
        i_kwargs = {k:v for (k,v) in kwargs.items() if k in ['spread_level','ask_change_level','min_ask_unit']}
        select['ask_price'] = self._get_spread_model_result
        select['ask_price_kwargs'] = i_kwargs
        
        # 默认 signal
        j_kwargs = {k:v for (k,v) in kwargs.items() if k in ['max_bid']}
        select['bid_volume'] = self._get_signal_volume_result
        select['bid_volume_kwargs'] = j_kwargs
        
        j_kwargs = {k:v for (k,v) in kwargs.items() if k in ['max_ask']}
        select['ask_volume'] = self._get_signal_volume_result
        select['ask_volume_kwargs'] = j_kwargs
        
        if 'bid_price_type' in kwargs.keys():
            if kwargs['bid_price_type'] == 'percent':
                i_kwargs = {k:v for (k,v) in kwargs.items() if k in['percent_bid','percent_ask','bid_change_level','ask_change_level']}
                select['bid_price'] = self._get_percent_model_result
                select['bid_price_kwargs'] = i_kwargs
            elif kwargs['bid_price_type'] == 'iopv':
                i_kwargs = {k:v for (k,v) in kwargs.items() if k in['bid_change_level1', 'bid_change_level2','ask_change_level1', 'ask_change_level2']}
                select['bid_price'] = self._get_iopv_model_result
                select['bid_price_kwargs'] = i_kwargs
            elif kwargs['bid_price_type'] == 'trading':
                i_kwargs = {k:v for (k,v) in kwargs.items() if k in['bid_change_level1', 'bid_change_level2','ask_change_level1', 'ask_change_level2']}
                select['bid_price'] = self._get_trading_model_result
                select['bid_price_kwargs'] = i_kwargs
        
        if 'ask_price_type' in kwargs.keys():
            if kwargs['ask_price_type'] == 'percent':
                i_kwargs = {k:v for (k,v) in kwargs.items() if k in['percent_bid','percent_ask','bid_change_level','ask_change_level']}
                select['ask_price'] = self._get_percent_model_result
                select['ask_price_kwargs'] = i_kwargs
            elif kwargs['ask_price_type'] == 'iopv':
                i_kwargs = {k:v for (k,v) in kwargs.items() if k in['bid_change_level1', 'bid_change_level2','ask_change_level1', 'ask_change_level2']}
                select['ask_price'] = self._get_iopv_model_result
                select['ask_price_kwargs'] = i_kwargs
            elif kwargs['ask_price_type'] == 'trading':
                i_kwargs = {k:v for (k,v) in kwargs.items() if k in['bid_change_level1', 'bid_change_level2','ask_change_level1', 'ask_change_level2']}
                select['ask_price'] = self._get_trading_model_result
                select['ask_price_kwargs'] = i_kwargs
        
        if 'bid_volume_type' in kwargs.keys():
            if kwargs['bid_volume_type'] == 'multiple':
                j_kwargs = {k:v for (k,v) in kwargs.items() if k in['volume_bid']}
                select['bid_volume'] = self._get_multiple_volume_result
                select['bid_volume_kwargs'] = j_kwargs
            elif kwargs['bid_volume_type'] == 'saopan':
                j_kwargs = {k:v for (k,v) in kwargs.items() if k in['bid_percent']}
                select['bid_volume'] = self._get_multiple_volume_result
                select['bid_volume_kwargs'] = j_kwargs 
        
        if 'ask_volume_type' in kwargs.keys():
            if kwargs['ask_volume_type'] == 'multiple':
                j_kwargs = {k:v for (k,v) in kwargs.items() if k in['volume_ask']}
                select['ask_volume'] = self._get_multiple_volume_result
                select['ask_volume_kwargs'] = j_kwargs
            elif kwargs['ask_volume_type'] == 'saopan':
                j_kwargs = {k:v for (k,v) in kwargs.items() if k in['ask_percent']}
                select['ask_volume'] = self._get_multiple_volume_result
                select['ask_volume_kwargs'] = j_kwargs 
        
        return select  
    
    def _get_spread_model_result(self, direction, latest_data, kwargs):
        # spread_level 买卖盘口价差级别
        # change_level 变化量
        # min_bid/ask_unit 最小买卖价位 
        
        if direction == 1:
            keys = ['spread_level','bid_change_level','min_bid_unit']
            deafult = [0.008, 0.001, 0.001]
            values = []
            for i in range(len(keys)):
                values.append(kwargs.get(keys[i], deafult[i]))
            price_bid = []
            
            # bid[0]     7      : 2
            # bidqty[0]  8      : 2
            # ask[0]    27      : 2
            # askqty[0] 28      : 2
            # iopv 5
            
            if self.broker.lims_flag == -2:
                price_bid.append(round(latest_data[1],3))
            elif self.broker.lims_flag != 666:
                # 最新价 <= 基金净值
                if latest_data[1] <= latest_data[5]:
                    # 卖一价 - 买卖盘口价差级别
                    bid0 = round(latest_data[27] - values[0],3)
                    
                    if self.broker.lims_flag == 3:
                        current_position = self.broker.get_account_position()
                        
                        bid0 = round(bid0 - (current_position - self.broker.daily_position) * 100 * values[2], 3)
                        bid1 = round(bid0 - 2 * values[2], 3)
                        bid2 = round(bid0 - 4 * values[2], 3)
                    else:
                        bid1 = round(bid0 - values[2], 3)
                        bid2 = round(bid0 - 2 * values[2], 3)
                        
                    price_bid.append(bid0)
                    price_bid.append(bid1)
                    price_bid.append(bid2)
              
                else:
                    # 买一价 - 变化量
                    bid0 = round(latest_data[7] - values[1],3)
                    
                    if self.broker.lims_flag == 3:
                        current_position = self.broker.get_account_position()
                        
                        bid0 = round(bid0 - (current_position - self.broker.daily_position) * 100 * values[2], 3)
                        bid1 = round(bid0 - 2 * values[2], 3)
                        bid2 = round(bid0 - 4 * values[2], 3)
                    else:
                        bid1 = round(bid0 - values[2], 3)
                        bid2 = round(bid0 - 2 * values[2], 3)
                        
                    price_bid.append(bid0)
                    price_bid.append(bid1)
                    price_bid.append(bid2)
                
                
            return price_bid
        
        elif direction == -1:
            
            keys = ['spread_level','ask_change_level','min_ask_unit']
            deafult = [0.008, 0.001, 0.001]
            values = []
            for i in range(len(keys)):
                values.append(kwargs.get(keys[i], deafult[i]))
            
            price_ask = []
        
            # bid[0]     7      : 2
            # bidqty[0]  8      : 2
            # ask[0]    27      : 2
            # askqty[0] 28      : 2
            # iopv 5
        
            if self.broker.lims_flag == 2:
                price_ask.append(round(latest_data[1],3))
            elif self.broker.lims_flag != 666:
                # 最新价 <= 基金净值
                if latest_data[1] <= latest_data[5]:
                    # 卖一价 + 变化量
                    ask0 = round(latest_data[27] + values[1],3)
                    
                    if self.broker.lims_flag == -3:
                        current_position = self.broker.get_account_position()
                        ask0 = round(ask0 + (current_position - self.broker.daily_position) * 100 * values[2], 3)
                        ask1 = round(ask0 + 2 * values[2], 3)
                        ask2 = round(ask0 + 4 * values[2], 3)
                    else:
                        ask1 = round(ask0 + values[2], 3)
                        ask2 = round(ask0 + 2 * values[2], 3)
                        
                    price_ask.append(ask0)
                    price_ask.append(ask1)
                    price_ask.append(ask2)
                else:
                    # 卖一价 + 买卖盘口价差级别
                    ask0 = round(latest_data[7] + values[0],3)
                    
                    if self.broker.lims_flag == -3:
                        current_position = self.broker.get_account_position()
                        ask0 = round(ask0 + (current_position - self.broker.daily_position) * 100 * values[2], 3)
                        ask1 = round(ask0 + 2 * values[2], 3)
                        ask2 = round(ask0 + 4 * values[2], 3)
                    else:
                        ask1 = round(ask0 + values[2], 3)
                        ask2 = round(ask0 + 2 * values[2], 3)
                        
                    price_ask.append(ask0)
                    price_ask.append(ask1)
                    price_ask.append(ask2)
                    
            return price_ask
    
    def _get_percent_model_result(self, latest_data, kwargs):
        # percent_bid/ask 买卖价比例
        # change_level 变化量      
        
        keys = ['percent_bid', 'percent_ask', 'bid_change_level', 'ask_change_level']
        deafult = [0.999, 1.001, 0.001, 0.001]
        values = []
        for i in range(len(keys)):
            values.append(kwargs.get(keys[i], deafult[i]))
            
        price_bid = []
        price_ask = []
        
        # bid[0]     7      : 2
        # bidqty[0]  8      : 2
        # ask[0]    27      : 2
        # askqty[0] 28      : 2
        
        if self.broker.lims_flag == 2:
            price_ask.append(round(latest_data[1]*0.9999,3))
        elif self.broker.lims_flag == -2:
            price_bid.append(round(latest_data[1]*1.0001,3))
        elif self.broker.lims_flag != 666:
            # 最新价 * 买价比例
            bid0 = round(latest_data[1] * values[0], 3)
    
            price_bid.append(bid0)
            # 最新价 * 卖价比例
            ask0 = round(latest_data[1] * values[1], 3)
            price_ask.append(ask0)
            
            if bid0 > latest_data[27]:
                bid0 = round(latest_data[27] + values[2], 3)
            if ask0 < latest_data[7]:
                ask0 = round(latest_data[7] + values[3], 3)
            
            price_bid.append(round(bid0 - values[2], 3))
            price_bid.append(round(bid0 - 2 * values[2], 3))
            price_ask.append(round(ask0 + values[3],3))
            price_ask.append(round(ask0 + 2 * values[3],3))
        
        return price_ask,price_bid
    
    def _get_iopv_model_result(self, latest_data, kwargs):

        # change_level 变化量      
        
        keys = ['bid_change_level', 'ask_change_level']
        deafult = [[-0.002,-0.001], [0.002, 0.001]]
        values = []
        for i in range(len(keys)):
            values.append(kwargs.get(keys[i], deafult[i]))
        
        price_bid = []
        price_ask = []
        
        # bid[0]     7      : 2
        # bidqty[0]  8      : 2
        # ask[0]    27      : 2
        # askqty[0] 28      : 2
        # iopv 5
        
        if self.broker.lims_flag == 2:
            price_ask.append(round(latest_data[1]*0.9999,3))
        elif self.broker.lims_flag == -2:
            price_bid.append(round(latest_data[1]*1.0001,3))
        elif self.broker.lims_flag != 666:
            # IOPV + 偏移量
            bid0 = round(latest_data[5] + values[0][0], 3)
    
            price_bid.append(bid0)
            # 最新价 * 卖价比例
            ask0 = round(latest_data[5] + values[1][0], 3)
            price_ask.append(ask0)
            
            if bid0 > latest_data[27]:
                bid0 = round(latest_data[27] + values[0][0], 3)
            if ask0 < latest_data[7]:
                ask0 = round(latest_data[7] + values[1][0], 3)
            
            price_bid.append(round(bid0 + values[0][1], 3))
            price_ask.append(round(ask0 + values[1][1], 3))
        
        return price_ask,price_bid
    
    def _get_trading_model_result(self, latest_data, kwargs):

        # change_level 变化量   
        
        keys = ['bid_change_level', 'ask_change_level']
        deafult = [[-0.002,-0.001], [0.002, 0.001]]
        values = []
        for i in range(len(keys)):
            values.append(kwargs.get(keys[i], deafult[i]))
            
        price_bid = []
        price_ask = []
        
        # bid[0]     7      : 2
        # bidqty[0]  8      : 2
        # ask[0]    27      : 2
        # askqty[0] 28      : 2
        # iopv 5
        
        if self.broker.lims_flag == 2:
            price_ask.append(round(latest_data[1]*0.9999,3))
        elif self.broker.lims_flag == -2:
            price_bid.append(round(latest_data[1]*1.0001,3))
        elif self.broker.lims_flag != 666:
            # 买盘盘口价格 + 偏移量
            bid0 = round(latest_data[7] + values[0][0], 3)
            price_bid.append(bid0)
            
            # 卖盘盘口价格 + 偏移量
            ask0 = round(latest_data[27] + values[1][0], 3)
            price_ask.append(ask0)
                
            if bid0 > latest_data[27]:
                bid0 = round(latest_data[27] + values[0][0], 3)
            if ask0 < latest_data[7]:
                ask0 = round(latest_data[7] + values[1][0], 3)
            
            price_bid.append(round(bid0 + values[0][1], 3))
            price_ask.append(round(ask0 + values[1][1], 3))
        
        return price_ask,price_bid
    
    def _get_signal_volume_result(self, direction, latest_data, n_ask, n_bid, kwargs):
        if direction == 1:
            keys = ['max_bid_volume']
            deafult = [10000]*n_bid
            values = []
            for i in range(len(keys)):
                values.append(kwargs.get(keys[i], deafult[i]))
            
            return values
        elif direction == -1:
            
            keys = ['max_ask_volume']
            deafult = [10000]*n_ask
            values = []
            for i in range(len(keys)):
                values.append(kwargs.get(keys[i], deafult[i]))
            
            return values
    
    def _get_multiple_volume_result(self, direction, latest_data, n, kwargs):
        if direction == 1:
            keys = ['volume_bid']
            deafult = [2000,1000,1000,1000]
            values = []
            for i in range(len(keys)):
                values.append(kwargs.get(keys[i], deafult))
            volume_bid = values[0][:n]
            
            return volume_bid
        
        elif direction == -1:
            
            keys = ['volume_ask']
            deafult = [2000,1000,1000,1000]
            values = []
            for i in range(len(keys)):
                values.append(kwargs.get(keys[i], deafult))
        
            volume_ask = values[0][:n]
        
            return volume_ask
    
    def _get_saopan_volume_result(self, latest_data, n_ask, n_bid, kwargs):
        # bid[0]     7      : 2
        # bidqty[0]  8      : 2
        # ask[0]    27      : 2
        # askqty[0] 28      : 2
        # iopv 5
        
        keys = ['ask_percent', 'bid_percent']
        deafult = [[0.001, 0.002, 0.003, 0.004], [0.001, 0.002, 0.003, 0.004]]
        values = []
        for i in range(len(keys)):
            values.append(kwargs.get(keys[i], deafult[i]))
        
        
        volume_ask = []
        volume_bid = []
        
        for i in range(n_ask):
            volume_ask.append(math.ceil(latest_data[28+i] * values[0][i] / 100) * 100)
        for i in range(n_bid):
            volume_bid.append(math.floor(latest_data[8+i] * values[1][i] / 100) * 100)
    
        return volume_ask,volume_bid
    
    def _check_min_spread(self, price_bid, price_ask):
        
        latest_price = round(self.broker.latest_data['snapshot'][1], 3)
        min_spread = math.floor(latest_price * 0.01 * 1000) / 1000
        
        if len(price_ask) > 0 and len(price_bid) > 0:
            if (spread := price_ask[-1] - price_bid[-1] - min_spread) > 0:
                if self.broker.lims_flag == -3:
                    
                    price_bid = [i+spread for i in price_bid]
    
    
                elif self.broker.lims_flag == 3:
                    price_ask = [i - spread for i in price_ask]
    
                else:
                    spread = math.ceil(spread * 1000 / 2) / 1000
                    for i in range(len(price_bid)):
                        price_bid[i] = round(price_bid[i] + spread, 3)
                    for i in range(len(price_ask)):
                        price_ask[i] = round(price_ask[i] - spread, 3)

                for i in range(len(price_bid)):
                    if price_bid[i] >= latest_price:
                        price_bid[i] = round(latest_price - 0.001 * (i + 1), 3)
                    else:
                        break
                for i in range(len(price_ask)):
                    if price_ask[i] <= latest_price:
                        price_ask[i] = round(latest_price + 0.001 * (i + 1), 3)
                    else:
                        break
            
        return price_bid,price_ask
    
    def _update_strategy_result(self):
        
        latest_data = self.broker.latest_data['snapshot']
        
        if ~np.isnan(latest_data[1]):
            
            price_bid = self.select['bid_price'](1, latest_data, self.select['bid_price_kwargs'])
            price_ask = self.select['ask_price'](-1, latest_data, self.select['bid_price_kwargs'])
            
            price_bid, price_ask = self._check_min_spread(price_bid, price_ask)
            
            volume_bid = self.select['bid_volume'](1, latest_data, len(price_bid), self.select['bid_volume_kwargs'])
            volume_ask = self.select['ask_volume'](-1, latest_data, len(price_ask), self.select['ask_volume_kwargs'])

            self.strategy_result = [self.broker.current_time, price_ask, price_bid, volume_ask, volume_bid]
    
    def _update_signals(self):
            
        signal = self.signals
        strategy_result = self.strategy_result
        
        latest_data = self.broker.latest_data['snapshot']
        if ~np.isnan(latest_data[1]) and latest_data[2] > 0:# price not na and volume > 0
            for i in range(len(strategy_result[1])):
                if ~np.isnan(strategy_result[1][i]) and (available_volume := self.broker.portfolio['available_volume']) > 0:
                    #sell
                    sim_volume = min(available_volume, strategy_result[3][i])
                    # sim_volume = self.broker._simulated_volume(latest_data[2]) #volume
                    # if latest_data.index[-1].hour == 9 and latest_data.index[-1].minute == 25:
                        # signal['order_type'] = 'now'
                    # else:
                    # signal['order_type'].append('delay')
                    # signal['order_time'].append(strategy_result[0])
                    signal['sell_price'].append(strategy_result[1][i])
                    signal['sell_volume'].append(sim_volume)
                    
                    # print([-1, strategy_result[1], sim_volume])
            for i in range(len(strategy_result[2])):        
                if ~np.isnan(strategy_result[2][i]):
                    #buy
                    sim_volume = strategy_result[4][i]
                    # sim_volume = self.broker._simulated_volume(latest_data[2])
                    # if latest_data.index[-1].hour == 9 and latest_data.index[-1].minute == 25:
                    #     signal['order_type'] = 'now'
                    # else:
                    # signal['order_type'] = 'delay'
                    
                    signal['buy_price'].append(strategy_result[2][i])
                    signal['buy_volume'].append(sim_volume)
                            
                    # print([1, strategy_result[2], sim_volume])
                        
        # self._add_data_to_signals_list()
        
    def _execute_signals(self):
                
        signals = self.signals
        
        for i in range(len(signals['sell_volume'])):
            if signals['sell_volume'][i] > 0 and self.broker.portfolio['available_volume'] > 0:
                self.broker.update_order(order_type = signals['order_type'],
                                         sell_price = signals['sell_price'][i],
                                         sell_volume = signals['sell_volume'][i])
                
                # print(['signals:', signals['sell_price'], signals['sell_volume']])
                # portfolio 的更新应放在真正update到order时
                # self.broker.update_portfolio(available_volume = -signals['sell_volume'],
                                             # frozen_volume = signals['sell_volume'])
        for i in range(len(signals['buy_volume'])):
            if signals['buy_volume'][i] > 0:
                consideration = signals['buy_price'][i] * signals['buy_volume'][i]
                total_commission = self.broker.fee_model.calc_total_cost(consideration)
                
                total_cost =  round(consideration + total_commission,4)
                    
                self.broker.update_order(order_type = signals['order_type'],
                                         buy_price = signals['buy_price'][i],
                                         buy_volume = signals['buy_volume'][i],
                                         frozen_money = total_cost)
                
                # self.broker.update_cash(cash = - total_cost)
                
        #初始化
        
        self.signals = {'order_type' : 'delay', 'buy_price' : [], 'buy_volume' : [], 'sell_price' : [], 'sell_volume' : []}
        
    
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