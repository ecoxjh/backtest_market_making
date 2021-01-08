import math
import numpy as np
import datetime
import pandas as pd
import copy 
from collections import deque
from qstrader import settings
from qstrader.broker.broker import Broker
from qstrader.broker.fee_model.fee_model import FeeModel
from qstrader.broker.fee_model.percent_fee_model import PercentFeeModel


class SimulatedBroker(Broker):

    def __init__(
        self,
        start_day,
        data_handler,
        time_delta = 100,
        keep_order_time = 10, #(s)
        deal_volume_time = '14:30:00',
        account_id = '000001',
        initial_cash = 10000000.0,
        initial_position = 0.5,
        cac_stat = True,
        fee_model = PercentFeeModel(),
        slippage_model = None,
        market_impact_model = None,
    ):
        
        self.data_handler = data_handler
        self.start_day,self.start_k,self.start_time = self._get_start_time(start_day)
        self.current_k = self.start_k - 1
        self.current_time = None
        self.current_day = 'initial'
        self.prior_day = 'initial'
        self.account_id = account_id
        self.time_delta = time_delta
        self.keep_order_time = keep_order_time
        self.deal_volume_time = deal_volume_time
        self.lims_flag = 0
        self.initial_position = initial_position
        self.cac_stat = cac_stat
        self._set_initial_portfolio_cash(initial_cash, initial_position)
        
        self.fee_model = self._set_fee_model(fee_model)
        self.market_making_statistic = self._set_initial_market_making_statistic()
        self.slippage_model = None  # TODO: Implement
        self.market_impact_model = None  # TODO: Implement
        self.daily_position = self.initial_position 
        self.latest_data = None
        
        self.cash = self.initial_cash
        self.portfolio = copy.copy(self.initial_portfolio)
        self.cash_list = self._set_initial_cash_list()
        self.portfolio_list = self._set_initial_portfolio_list()
        
        self.order = self._set_initial_order()
        self.order_list = self._set_initial_order_list()
        
        self.update_order_delay_list = {'bid': deque(), 'ask': deque()}
        self.cancel_order_delay_list = {'bid': deque(), 'ask': deque()}
        # self.cancel_order_list_total = [self.order_list[0]]
        
        if settings.PRINT_EVENTS:
            print('初始化 broker "%s"...' % self.account_id)
            
    def _get_start_time(self,start_day):
        
        start_k = 0
        start_day = self.data_handler.data_source.data_info['Days'][0]
        start_time = self.data_handler.data_source.data[start_day]['snapshot']['data'][0][-1]
        
        return start_day,start_k,start_time
    
    def _set_initial_portfolio_cash(self,initial_cash,initial_position):
        
        if initial_cash < 0.0:
            raise ValueError("初始资金为负数：'%s' " % initial_cash)
            
        initial_portfolio = {'total_volume': 0,'available_volume' : 0, 'frozen_volume' : 0}   
        
        if initial_position == 0:
            
            return initial_portfolio,initial_cash
        
        elif initial_position < 0:
            
            # 后面再考虑负仓位
            raise ValueError('初始仓位为负数：%s', initial_position)
            
        else:
            #以最新价成交
            latest_data = self.data_handler.get_stock_latest_data(self.start_day, self.start_k)
            current_price = latest_data['snapshot'][3]
            
            initial_volume = math.floor(initial_cash * initial_position / current_price / 100) * 100
            initial_portfolio['total_volume'] = initial_portfolio['available_volume'] = initial_volume
            initial_cash -= initial_volume * current_price
            
            self.initial_portfolio = initial_portfolio
            self.initial_cash = initial_cash
            self.daily_portfolio = {'initial':[current_price,initial_volume]}

    def _set_fee_model(self, fee_model):

        if issubclass(fee_model.__class__, FeeModel):
            return fee_model
        else:
            raise TypeError("提供的fee_model '%s' 不是FeeModel类的子类" % fee_model.__class__)

    def _set_initial_market_making_statistic(self):
        
        # count每秒查询 挂单市值 
        market_making_statistic = {i : {'count' :{'bid' : deque(), 'ask' : deque()}, 'result' : None} for i in self.data_handler.data_source.data_info['Days']}
        
        return market_making_statistic
    
    def _set_initial_cash_list(self):
        
        cash_list = [['index','cash']]
        
        return cash_list

    def _set_initial_portfolio_list(self):
        
        portfolio_list = [
        ['index','total_volume','available_volume','frozen_volume']]

        return portfolio_list
    
    def _set_initial_order(self):
        #添加多单情形
        # order_time direction price volume (frozen_money)
        order = {'bid':deque(), 'ask': deque()}
        return order

    def _set_initial_order_list(self):
        
        if self.cac_stat:
            # order time order_bid order_ask cancel_order_bid cancel_order_ask
            #order_list = [['index', 'buy_price','buy_volume','frozen_money','sell_price','sell_volume']]
            order_list = {i : deque([]) for i in self.data_handler.data_source.data_info['Days']}

            return order_list
    
    def _add_data_to_cash_list(self):
        """
        add_func 更新对应字段后使用
        """

        cash_list = self.cash_list
        
        if len(cash_list) > 1:
            if cash_list[-1][0] > self.current_time:
                raise ValueError('时间戳错误，添加数据失败')
                
        cash_list.append([self.current_time, 
                          self.cash])
    
    def _add_data_to_portfolio_list(self):
   
        portfolio_list = self.portfolio_list
        if len(portfolio_list) > 1:
            if portfolio_list[-1][0] > self.current_time:
                raise ValueError('时间戳错误，添加数据失败')

        portfolio_list.append([self.current_time] + 
                          list(self.portfolio.values()))
    
    def _add_data_to_order_list(self, update_time):
        
        if self.cac_stat:
            # order_list 时间以[0]为准
            self.order_list[self.current_day].append([update_time, self.order['bid'].copy(), self.order['ask'].copy(),
                                                      self.cancel_order_delay_list['bid'].copy(),
                                                      self.cancel_order_delay_list['ask'].copy()]
                                                     )
    
    def _check_order_list(self):
        order_list = self.order_list
        
        
        for i_day,i_list in order_list.items(): 
            temp_list = deque()
            for i in range(len(i_list)-1, 0, -1):
                if i_list[i][0] != i_list[i-1][0]:
                    temp_list.append(i_list[i])
            temp_list.reverse()
            order_list[i_day] = temp_list
        
    def get_market_making_statistic(self):
        
        market_making_statistic = {i : {'count' :{'bid' : deque(), 'bid_count' : 0, 'ask' : deque(), 'ask_count' : 0}} for i in self.data_handler.data_source.data_info['Days']}
        statistic = {i : [0,0] for i in self.data_handler.data_source.data_info['Days']}

        self._check_order_list()
        order_list = self.order_list
        
        for iday,stat in market_making_statistic.items():
            
            start_time = pd.to_datetime(iday + ' 09:30:00')
            end_time = pd.to_datetime(iday+ ' 15:00:00')
            
            current_list = order_list[iday]
            
            market_making_statistic[iday]['count']['bid'].append([start_time,0])
            market_making_statistic[iday]['count']['ask'].append([start_time,0])
            for i in current_list:
                if i[0] >  market_making_statistic[iday]['count']['bid'][-1][0]:
                    bid = sum([j[1] * j[2] for j in i[1]] + [j[1] * j[2] for j in i[3]]) 
                    ask = sum([j[1] * j[2] for j in i[2]] + [j[1] * j[2] for j in i[4]]) 
                    market_making_statistic[iday]['count']['bid'].append([i[0],bid])
                    market_making_statistic[iday]['count']['ask'].append([i[0],ask])
            market_making_statistic[iday]['count']['bid'].append([end_time,200000])
            market_making_statistic[iday]['count']['ask'].append([end_time,200000])
            
            start_temp = None
            for i in market_making_statistic[iday]['count']['bid']:
                if start_temp is None and i[1] < 200000:
                    start_temp = i
                else:
                    if start_temp is not None:
                        time_delta = i[0] - start_temp[0]
                        if time_delta.seconds >= 0:
                            market_making_statistic[iday]['count']['bid_count'] += (time_delta.total_seconds())
                        else:
                            raise ValueError('时间差错误：%s' % time_delta)
                        start_temp = None
            statistic[iday][0]= round(100 - market_making_statistic[iday]['count']['bid_count']/144, 3) 
            
            start_temp = None
            for i in market_making_statistic[iday]['count']['ask']:
                if start_temp is None and i[1] < 200000:
                    start_temp = i
                else:
                    if start_temp is not None:
                        time_delta = i[0] - start_temp[0]
                        market_making_statistic[iday]['count']['ask_count'] += time_delta.seconds
                        start_temp = None           
            statistic[iday][1] = round(100 - market_making_statistic[iday]['count']['ask_count']/144, 3)
        
        stat_list = list(statistic.values())
        statistic['result'] = [np.mean([i[0] for i in stat_list]), np.mean([i[1] for i in stat_list])]

        return statistic

    def get_account_total_market_value(self):
        """
        返回账号总市值，不包括cash
        """
        tmv = 0
        
        latest_price = self.latest_data['snapshot'][1]
        
        if self.portfolio['total_volume'] != 0:
            tmv += latest_price * self.portfolio['total_volume']
            
        return tmv

    def get_account_total_equity(self):
        """
        返回账号总资产，包括cash
        """
        
        tmv = self.get_account_total_market_value()
        
        # 加上 order 或 cancel_order 中冻结的 cash
        for itiem in self.order['bid']:
            if itiem[3] > 0:
                tmv += itiem[3]
                
        if len(self.cancel_order_delay_list['bid']) > 0:
            for itiem in self.cancel_order_delay_list['bid']:
                if itiem[3] > 0:
                    tmv += itiem[3]
                
        equity = round(self.cash + tmv,4)
        
        return equity 
    
    def get_account_market_making_equity(self):
        '''
        返回做市收益 : 减去初始仓位对应的市值
        '''
        latest_price = self.latest_data['snapshot'][1]

        equity = self.get_account_total_equity()

        daily_portfolio = self.daily_portfolio['initial']
        # daily_portfolio = self.daily_portfolio[self.prior_day]
        
        market_making_equity = equity + (daily_portfolio[0] - latest_price) * daily_portfolio[1]
        
        return market_making_equity
        
    def get_account_position(self):
        
        volume_ratio = round(self.get_account_total_market_value() / self.get_account_market_making_equity(), 4)
    
        return volume_ratio
    
    def _get_latest_data(self):
        
        latest_data = self.data_handler.get_stock_latest_data(self.current_day, self.current_k)
        
        return latest_data
    
    def get_slice_data(self, n_slice):
        
        slice_data  =self.data_handler.get_stock_slice_data(self.current_day, n_slice, self.current_k)
        
        return slice_data
    
    def update_cash(self, **kwargs):
        
        if 'cash' not in kwargs:
            raise ValueError('未提供增减金额数值')
        
        self.cash += kwargs['cash']
        
        if self.cash < 0:
            raise ValueError['现金为负']
    
    def update_order(self, **kwargs):
        """
        由signals调用
        """
        
        order = self.order
        
        if kwargs['order_type'] == 'delay':
            if self.lims_flag == 2:
                if 'sell_price' in kwargs.keys():
                    # 先撤买单
                    if (n := len(order['bid'])) != 0:
                        for i in range(n-1,-1,-1):
                            self._cancel_order_delay(1, self.current_time, order['bid'][i][1:])
                            order['bid'].remove(order['bid'][i])
                    #再更新卖单
                    
                    kwargs['sell_volume']  = min(kwargs['sell_volume'], self.portfolio['total_volume'] - self.initial_portfolio['total_volume'])
                    self._update_order_delay(-1, [kwargs['sell_price'], kwargs['sell_volume']])
            elif self.lims_flag == -2:
                if 'buy_price' in kwargs.keys():
                    #先撤卖单
                    if (n := len(order['ask'])) != 0:
                        for i in range(n-1,-1,-1):
                                self._cancel_order_delay(-1, self.current_time, order['ask'][i][1:])
                                order['ask'].remove(order['ask'][i])
                    #再更新买单
                    kwargs['buy_volume'] = min(kwargs['buy_volume'], self.initial_portfolio['total_volume'] - self.portfolio['total_volume'])
                        
                    consideration = kwargs['buy_price'] * kwargs['buy_volume']
                    total_commission = self.fee_model.calc_total_cost(consideration)
                    kwargs['frozen_money'] =  round(consideration + total_commission,4)
                    
                    self._update_order_delay(1, [kwargs['buy_price'], kwargs['buy_volume'], kwargs['frozen_money']])
            
            elif self.lims_flag != 666:
                if 'buy_price' in kwargs.keys() and self.lims_flag != 1: # lims_flag = 1 多单限制
                    
                    #先撤奇异单
                    if (n := len(order['ask'])) != 0:
                        for i in range(n-1,-1,-1):
                            if kwargs['buy_price'] >= order['ask'][i][1]:
                            
                                self._cancel_order_delay(-1, self.current_time, order['ask'][i][1:])
                                order['ask'].remove(order['ask'][i])
                    self._update_order_delay(1, [kwargs['buy_price'], kwargs['buy_volume'], kwargs['frozen_money']])
                                                    
                if 'sell_price' in kwargs.keys() and self.lims_flag != -1: # lims_flag = -1 空单限制
                    # 先撤奇异单
                    if (n := len(order['bid'])) != 0:
                        for i in range(n-1,-1,-1):
                            if kwargs['sell_price'] <= order['bid'][i][1]:
                        
                                self._cancel_order_delay(1, self.current_time, order['bid'][i][1:])
                                order['bid'].remove(order['bid'][i])
                    self._update_order_delay(-1, [kwargs['sell_price'], kwargs['sell_volume']])
                    
        # 集合竞价即时发单,暂时用不到
        # elif kwargs['order_type'] == 'now':
        #     if 'buy_price' in kwargs.keys() and self.lims_flag != 1:
        #          # 如果集合竞价时存在反向单，不考虑交易
        #          if order['sell_volume'] == 0:
        #              if order['buy_volume'] != 0 and order['buy_price'] != kwargs['buy_price']:
        #                  print('不该出现')
        #                  #集合竞价时，如果挂两次不同价的单最终以均价确定是否成交
        #                  #集合竞价发出信号代表买价低于最新价，可以成交，正常情况下仅发出一次信号
        #                  #正常情况下，不会出现该种情形
        #                  order['buy_price'] = (kwargs['buy_price']*kwargs['buy_volume'] + order['buy_price']*kwargs['buy_volume'])/(kwargs['buy_volume'] + order['buy_volume'])
        #                  order['buy_volume'] += kwargs['buy_volume']
        #              else:
        #                  order['buy_price'] = kwargs['buy_price']
        #                  order['buy_volume'] += kwargs['buy_volume']
                         
        #     if 'sell_price' in kwargs.keys() and self.lims_flag != -1:
        #         # 如果集合竞价时存在反向单，不考虑交易
        #         if order['buy_volume'] == 0:
        #             if order['sell_volume'] != 0 and order['sell_price'] != kwargs['sell_price']:
        #                 print('不该出现')
        #                 order['sell_price'] = (kwargs['sell_price']*kwargs['sell_volume'] + order['sell_price']*kwargs['sell_volume'])/(kwargs['sell_volume'] + order['sell_volume'])
        #                 order['sell_volume'] += kwargs['sell_volume']
        #             else:
        #                 order['sell_price'] = kwargs['sell_price']
        #                 order['sell_volume'] += kwargs['sell_volume']
        else:
            raise ValueError('order\'s type error')
        self.order = order
             
    def update_portfolio(self, **kwargs):
        
        portfolio = self.portfolio
        
        for key,value in kwargs.items():
            if key not in portfolio.keys():
                raise ValueError('输入不明字段，请检查')
            portfolio[key] += value
            
    def _update_order_delay(self, direction, *args):
        '''
        挂单函数,将order挂至update_order_list
        '''
        update_order_delay_list = self.update_order_delay_list
        
        if direction == 1:
            if len(args[0]) != 3:
                raise ValueError('输入参数个数错误')
            update_order_delay_list['bid'].append([self.current_time]+args[0])
        elif direction == -1:
            if len(args[0]) != 2:
                raise ValueError('输入参数个数错误')
            update_order_delay_list['ask'].append([self.current_time]+args[0])
        else:
            raise ValueError('direction 参数输入错误仅能为 1 或 - 1: %s' % direction)
            
    def _check_order_delay(self, order_type, order_delay_list):
        '''
        execute_update/cancel_order调用
        '''
        
        if (n := len(order_delay_list['bid'])) > 0:
            for i in range(n-1, -1, -1):
                if order_delay_list['bid'][i][2] == 0:
                    order_delay_list['bid'].remove(order_delay_list['bid'][i])
                    
        if order_type == 'cancel':
            order_delay_list['bid'] = deque(sorted(order_delay_list['bid'], key = lambda kv : (kv[1],kv[0]), reverse = True))
        
        if (n := len(order_delay_list['ask'])) > 0:
            for i in range(n-1, -1, -1):
                if order_delay_list['ask'][i][2] == 0:
                    order_delay_list['ask'].remove(order_delay_list['ask'][i])
        if order_type == 'cancel':
            order_delay_list['ask'] = deque(sorted(order_delay_list['ask'], key = lambda kv : (kv[1],kv[0]), reverse = False))

        return order_delay_list
    
    def _exec_update_order_delay(self):
        """
        time_detla(ms)延迟
        """
        
        latest_data = self.latest_data['tick'][::-1]
        update_order_delay_list = self.update_order_delay_list 
        
        update_order_delay_list = self._check_order_delay('update', update_order_delay_list)
        order = self.order
        
        if (n := len(update_order_delay_list['bid'])) > 0:
            for i in range(n):
                if (order_time := update_order_delay_list['bid'][i][0] + datetime.timedelta(milliseconds = self.time_delta)) <= self.current_time:
                    if update_order_delay_list['bid'][i][2] != 0:
                        for j_tick in latest_data: # latest_data 已逆序
                            if  j_tick[0] <= update_order_delay_list['bid'][i][0]:
                                # 仅运行一次
                                if j_tick[1] <= update_order_delay_list['bid'][i][1]:
                                    sim_volume = min(update_order_delay_list['bid'][i][2], self._simulated_volume(j_tick[2]))
                                    
                                    buy_consideration = round(j_tick[1] * sim_volume, 4)
                                    buy_total_commission = self.fee_model.calc_total_cost(buy_consideration, self)
                                    est_buy_total_cost =  round(buy_consideration + buy_total_commission, 4)
                                    
                                    self.update_cash(cash = - est_buy_total_cost)
                                    self.update_portfolio(total_volume = sim_volume, available_volume = sim_volume)
                                    
                                    # 不论是否完全成交 该单均撤销
                                    update_order_delay_list['bid'][i] = [0] * 4

                                    if settings.PRINT_EVENTS:
                                        print("(%s) - 执行update买单: 数量: %s, 价格: %0.3f, "
                                              "股票市值: %0.2f, 佣金成本: %0.2f, 总费用: %0.2f" % 
                                              (self.current_time, sim_volume, j_tick[1],
                                               buy_consideration, buy_total_commission, est_buy_total_cost))
                    
                                    break
                        
                        if update_order_delay_list['bid'][i][2] != 0:
                            update_order_delay_list['bid'][i][0] = order_time
                            order['bid'].append(update_order_delay_list['bid'][i])
                        
                            self._add_data_to_order_list(order_time)
                        
                            self.update_cash(cash = - update_order_delay_list['bid'][i][3])
                        
                            update_order_delay_list['bid'][i] = [0] * 4
                else:
                    break
                
        if (n := len(update_order_delay_list['ask'])) > 0:
            for i in range(n):
                if (order_time := update_order_delay_list['ask'][i][0] + datetime.timedelta(milliseconds=self.time_delta)) <= self.current_time:
                    if update_order_delay_list['ask'][i][2] != 0:
                        for j_tick in latest_data: # latest_data已逆序
                            if j_tick[0] <= update_order_delay_list['ask'][i][0]:
                                # 仅运行一次
                                if j_tick[1] >= update_order_delay_list['ask'][i][1]:
                                    
                                    sim_volume = min(update_order_delay_list['ask'][i][2], self._simulated_volume(j_tick[2]))
                                    
                                    sell_consideration = round(j_tick[1] * sim_volume, 4)
                                    sell_total_commission = self.fee_model.calc_total_cost(sell_consideration, self)
                                    est_sell_total_earn = round(sell_consideration - sell_total_commission, 4)
                                    
                                    self.update_cash(cash = est_sell_total_earn)
                                    self.update_portfolio(total_volume = -sim_volume)
                                   
                                    # 不论是否完全成交 该单均撤销
                                    update_order_delay_list['bid'][i] = [0] * 4

                                    if settings.PRINT_EVENTS:
                                        print("(%s) - 执行卖单:  数量: %s, 价格: %0.3f, "
                                              "股票市值: %0.2f, 佣金成本: %0.2f, 总收益: %0.2f" % 
                                              (self.current_time, sim_volume, j_tick[1],
                                               sell_consideration, sell_total_commission, est_sell_total_earn))
                                        
                                    break
                        
                        if update_order_delay_list['ask'][i][2] != 0:
                            update_order_delay_list['ask'][i][0] = order_time
                            order['ask'].append(update_order_delay_list['ask'][i])
                            
                            self._add_data_to_order_list(order_time)

                            self.update_portfolio(available_volume = - update_order_delay_list['ask'][i][2],
                                                  frozen_volume = update_order_delay_list['ask'][i][2])
                            
                            update_order_delay_list['ask'][i] = [0] * 3
                else:
                    break
                        
        update_order_delay_list = self._check_order_delay('update',update_order_delay_list)
        
        # self.update_order_delay_list = update_order_delay_list
        
    def _cancel_order_delay(self, direction, cancel_order_time, *args):
        """
        撤单函数，将order撤至cancel_order_list
        """
        cancel_order_delay_list = self.cancel_order_delay_list
        
        if direction == 1:
            if len(args[0]) != 3:
                raise ValueError('输入参数个数错误')
            # if args[2] == 0:
            #     raise ValueError('error')
            cancel_order_delay_list['bid'].append([cancel_order_time] + args[0])
        elif direction == -1:
            if len(args[0]) != 2:
                raise ValueError('输入参数个数错误')
            cancel_order_delay_list['ask'].append([cancel_order_time] + args[0])
        else:
            raise ValueError('direction 参数输入错误仅能为 1 或 - 1: %s' % direction)
    
    def _exec_cancel_order_delay(self):
        """
        time_detla(ms)延迟
        """
        cancel_order_delay_list = self.cancel_order_delay_list
        
        cancel_order_delay_list = self._check_order_delay('cancel', cancel_order_delay_list)
        
        latest_data = self.latest_data['tick']

        for j in range(len(latest_data)):  
            if (m := len(cancel_order_delay_list['bid'])) > 0:  
                for i in range(m):
                    # 延迟内
                    if cancel_order_delay_list['bid'][i][0] + datetime.timedelta(milliseconds = self.time_delta) >= self.current_time:
                        # 买量不为0，买价 >= 最新价   [1] price  [2] volume
                        if cancel_order_delay_list['bid'][i][2] != 0 and cancel_order_delay_list['bid'][i][1] >= latest_data[j][1]:
                            if cancel_order_delay_list['bid'][i][1] == latest_data[j][1]:
                                sim_volume = min(cancel_order_delay_list['bid'][i][2], self._simulated_volume(latest_data[j][2]))
                            else:
                                sim_volume = min(cancel_order_delay_list['bid'][i][2], latest_data[j][2])
                        
                            buy_consideration = round(cancel_order_delay_list['bid'][i][1] * sim_volume, 4)
                            buy_total_commission = self.fee_model.calc_total_cost(buy_consideration, self)
                            est_buy_total_cost = round(buy_consideration + buy_total_commission, 4)
                            
                            if abs(est_buy_total_cost - cancel_order_delay_list['bid'][i][3]) < 0.01:
                                est_buy_total_cost = cancel_order_delay_list['bid'][i][3]
                                
                            if est_buy_total_cost > cancel_order_delay_list['bid'][i][3]:
                                raise ValueError('冻结资金不足以成交')
                            
                            self.update_portfolio(total_volume = sim_volume, available_volume = sim_volume)
                            cancel_order_delay_list['bid'][i][2] -= sim_volume
                            cancel_order_delay_list['bid'][i][3] -= est_buy_total_cost
                            
                            if settings.PRINT_EVENTS:
                                print("(%s) - 执行cancel买单: 数量: %s, 价格: %0.3f, "
                                      "股票市值: %0.2f, 佣金成本: %0.2f, 总费用: %0.2f" % 
                                      (latest_data[j][0], sim_volume, cancel_order_delay_list['bid'][i][1],
                                      buy_consideration, buy_total_commission, est_buy_total_cost))
                            
                            self._add_data_to_order_list(latest_data[j][0])

                    else:
                        if cancel_order_delay_list['bid'][i][2] != 0:
                            # cancel_order frozen_money 转入 cash
                            self.update_cash(cash = cancel_order_delay_list['bid'][i][3])
                            cancel_order_delay_list['bid'][i][2:4] = [0,0]
                            self._add_data_to_order_list(cancel_order_delay_list['bid'][i][0] + datetime.timedelta(milliseconds = self.time_delta))
                            
                cancel_order_delay_list = self._check_order_delay('cancel', cancel_order_delay_list)
            elif (m := len(cancel_order_delay_list['ask'])) > 0:  
                for i in range(m):
                    # 延迟内
                    if cancel_order_delay_list['ask'][i][0] + datetime.timedelta(milliseconds = self.time_delta) >= self.current_time:
                        # 卖量不为0, 卖价 <= 最新价    [4] price  [5] volume
                        if cancel_order_delay_list['ask'][i][2] != 0 and cancel_order_delay_list['ask'][i][1] <= latest_data[j][1]:                    
                            if cancel_order_delay_list['ask'][i][1] == latest_data[j][1]:
                                sim_volume = min(cancel_order_delay_list['ask'][i][2], self._simulated_volume(latest_data[j][2]))
                            else:
                                sim_volume = min(cancel_order_delay_list['ask'][i][2],latest_data[j][2])
                        
                            sell_consideration = round(cancel_order_delay_list['ask'][i][1] * sim_volume,4)
                            sell_total_commission = self.fee_model.calc_total_cost(sell_consideration, self)
                            est_sell_total_earn = round(sell_consideration - sell_total_commission,4)
                        
                            self.update_cash(cash = est_sell_total_earn)
                            self.update_portfolio(total_volume = -sim_volume, frozen_volume = -sim_volume)
                       
                            cancel_order_delay_list['ask'][i][2] -= sim_volume
                            
                            if settings.PRINT_EVENTS:
                                print("(%s) - 执行cancel卖单:  数量: %s, 价格: %0.3f, "
                                      "股票市值: %0.2f, 佣金成本: %0.2f, 总收益: %0.2f" % 
                                      (latest_data[j][0], sim_volume, cancel_order_delay_list['ask'][i][1],
                                      sell_consideration, sell_total_commission, est_sell_total_earn))
                            self._add_data_to_order_list(latest_data[j][0])    
                                
                    # 延迟外，cancel_order成功撤单
                    else:
                        if cancel_order_delay_list['ask'][i][2] != 0:
                            # portfolio available_volume/frozen_volume +/- cancel_order sell_volume
                            self.update_portfolio(available_volume = cancel_order_delay_list['ask'][i][2], 
                                                   frozen_volume = -cancel_order_delay_list['ask'][i][2])
                            cancel_order_delay_list['ask'][i][2] = 0
                            self._add_data_to_order_list(cancel_order_delay_list['ask'][i][0] + datetime.timedelta(milliseconds = self.time_delta))

                cancel_order_delay_list = self._check_order_delay('cancel', cancel_order_delay_list)

            else:
                break
                
            
    def _simulated_volume(self, current_volume, volume_ratio = 0.02): 
        
        if current_volume  < 0:
            raise ValueError('当前成交量错误 %s', current_volume)
        elif current_volume == 0:
            return 0
        else:
            if current_volume * volume_ratio < 100:
                volume_max = 100
            else:
                volume_max = math.floor(np.random.normal(current_volume*volume_ratio*0.5 + 50,
                                               (current_volume*volume_ratio - 100)/4,1)/100)*100
                volume_max = max(min(volume_max,
                                     math.floor(current_volume*volume_ratio/100)*100),
                                 100)
            return volume_max   
        
    def _check_order_volume(self, order):
        if (n := len(order['bid'])) > 0:
            for i in range(n-1, -1, -1):
                if order['bid'][i][2] == 0:
                    order['bid'].remove(order['bid'][i])
        # 价格优先、时间优先
        order['bid'] = deque(sorted(order['bid'], key = lambda kv : (-kv[1], kv[0])))
        
        if (n := len(order['ask'])) > 0:
            for i in range(n-1, -1, -1):
                if order['ask'][i][2] == 0:
                    order['ask'].remove(order['ask'][i])
        
        order['ask'] = deque(sorted(order['ask'], key = lambda kv : (-kv[1], kv[0]), reverse = True))

        return order
    
    def _execute_order(self):
        """
        每update一次,执行一次
        """

        latest_data = self.latest_data['tick']
        order = self.order
        
        for i in range(len(latest_data)):
            order = self._check_order_volume(order)
            
            if (m := len(order['bid'])) > 0:
                for j in range(m):
                    # 买量 ！= 0 ， 买价 >= 最新价
                    if order['bid'][j][2] != 0 and round(latest_data[i][1],3) <= order['bid'][j][1]:
                        # 买价 = 最新价
                        if order['bid'][j][1] == round(latest_data[i][1],3):
                            sim_volume = min(order['bid'][j][2], self._simulated_volume(latest_data[i][2]))
                        else:
                            #买价 > 最新价 如何仿真扫单时的情形
                            sim_volume = min(order['bid'][j][2], latest_data[i][2])
                                
                        buy_consideration = round(order['bid'][j][1] * sim_volume,4)
                        buy_total_commission = self.fee_model.calc_total_cost(buy_consideration, self)
                        est_buy_total_cost =  round(buy_consideration + buy_total_commission, 4)
                    
                        if abs(est_buy_total_cost - order['bid'][j][3]) < 0.01:
                            est_buy_total_cost = order['bid'][j][3]
                        
                        if est_buy_total_cost > order['bid'][j][3]:
                            raise ValueError('冻结资金不足以成交')
                    
                        self.update_portfolio(total_volume = sim_volume, available_volume = sim_volume)
                        
                        order['bid'][j][2] -= sim_volume
                        order['bid'][j][3] -= est_buy_total_cost
                    
                        if settings.PRINT_EVENTS:
                            print("(%s) - 执行买单: 数量: %s, 价格: %0.3f, "
                                  "股票市值: %0.2f, 佣金成本: %0.2f, 总费用: %0.2f" % 
                                  (latest_data[i][0], sim_volume, order['bid'][j][1],
                                  buy_consideration, buy_total_commission, est_buy_total_cost))
                        
                        self._add_data_to_order_list(latest_data[i][0])

            elif (m := len(order['ask'])) > 0:
                for j in range(m):
                    # 卖量 != 0, 卖价 <= 最新价
                    if order['ask'][j][2] != 0 and latest_data[i][1] >= order['ask'][j][1]:
                        # 卖价 = 最新价
                        if order['ask'][j][1] == latest_data[i][1]:
                            sim_volume = min(order['ask'][j][2], self._simulated_volume(latest_data[i][2]))
                        else:
                            # 卖价 < 最新价
                            sim_volume = min(order['ask'][j][2], latest_data[i][2])
                        
                        sell_consideration = round(order['ask'][j][1] * sim_volume, 4)
                        sell_total_commission = self.fee_model.calc_total_cost(sell_consideration, self)
                        est_sell_total_earn = round(sell_consideration - sell_total_commission, 4)
                    
                        self.update_cash(cash = est_sell_total_earn)
                        self.update_portfolio(total_volume = -sim_volume, frozen_volume = -sim_volume)
                    
                        order['ask'][j][2] -= sim_volume
        
                        if settings.PRINT_EVENTS:
                            print("(%s) - 执行卖单:  数量: %s, 价格: %0.3f, "
                                  "股票市值: %0.2f, 佣金成本: %0.2f, 总收益: %0.2f" % 
                                  (self.current_time, sim_volume, order['ask'][j][1],
                                  sell_consideration, sell_total_commission, est_sell_total_earn))
                        
                        self._add_data_to_order_list(latest_data[i][0])

            else:
                break
            self.order = self._check_order_volume(order)
            
    def _update_current_time(self, **kwargs):
        
        self.current_time = pd.to_datetime(self.data_handler.data_source.data[self.current_day]['snapshot']['data'][self.current_k][0])
        self.latest_data = self._get_latest_data()
        if self.current_k == self.data_handler.data_source.data_info['NK'][self.current_day] - 1:
            self.daily_portfolio[self.current_day] = [self.latest_data['snapshot'][1],self.portfolio['total_volume']]
            self.daily_position = self.get_account_position()
        self._check_order_time(**kwargs)
        
    def _check_order_time(self, **kwargs):
        
        keep_order_time = self.keep_order_time
        order_delay_time = self.time_delta/1000

        order = self.order
        if (n := len(order['bid'])) > 0:
            for i in range(n-1, -1, -1):
                if (order_time := order['bid'][i][0] + datetime.timedelta(seconds = keep_order_time + order_delay_time)) < self.current_time:
                    # 直接撤单
                    self.update_cash(cash = order['bid'][i][3])
                    order['bid'].remove(order['bid'][i])
                    self._add_data_to_order_list(order_time)

                elif (cancel_time := order['bid'][i][0] + datetime.timedelta(seconds = keep_order_time)) <= self.current_time:
                    # 撤单延迟
                    self._cancel_order_delay(1,
                                             cancel_time,
                                             order['bid'][i][1:])
                    order['bid'].remove(order['bid'][i])
                    self._add_data_to_order_list(cancel_time)
        
        if (n := len(order['ask'])) > 0:
            for i in range(n-1, -1, -1):
                if (order_time := order['ask'][i][0] + datetime.timedelta(seconds = keep_order_time + order_delay_time)) < self.current_time:
                    #直接撤单
                    self.update_portfolio(available_volume = order['ask'][i][2], 
                                          frozen_volume = -order['ask'][i][2])
                    order['ask'].remove(order['ask'][i])
                    self._add_data_to_order_list(order_time)

                elif (cancel_time := order['ask'][i][0] + datetime.timedelta(seconds = keep_order_time)) <= self.current_time:
                    #撤单延迟
                    self._cancel_order_delay(-1, 
                                             cancel_time,
                                             order['ask'][i][1:])
                    order['ask'].remove(order['ask'][i])
                    
                    self._add_data_to_order_list(cancel_time)

        self.order = order
        
    def _check_order_portfolio(self):
        
        order = self.order
        portfolio = self.portfolio
        
        for stock in self.stocks:
            if order[stock]['buy_price'] * order[stock]['buy_volume'] > order[stock]['frozen_money']:
                raise ValueError('(%s) order error: buy_price: %s; buy_volume: %s; frozen_money: %s'  %(stock,
                                                                      order[stock]['buy_price'],
                                                                      order[stock]['buy_volume'],
                                                                      order[stock]['frozen_money']))
            if portfolio[stock]['total_volume'] != portfolio[stock]['available_volume'] + portfolio[stock]['frozen_volume'] or portfolio[stock]['frozen_volume'] < 0:
                 raise ValueError('(%s) portfolio error: total_volume: %s; available_volume: %s; frozen_volume: %s'  %(stock,
                                                                      portfolio[stock]['total_volume'],
                                                                      portfolio[stock]['available_volume'],
                                                                      portfolio[stock]['frozen_volume']))
            if order[stock]['sell_volume'] != portfolio[stock]['frozen_volume']:
                raise ValueError('冻结股份不匹配')
                
    def update(self):
        # 1.挂撤单序列
        
        # self._check_order_portfolio()
        self._exec_update_order_delay()
        self._exec_cancel_order_delay()
        
        # self._add_data_to_order_list()
        
        self._execute_order()
        
        # 2.更新list
        
        # self._add_data_to_cash_list()
        # self._add_data_to_portfolio_list()
        
