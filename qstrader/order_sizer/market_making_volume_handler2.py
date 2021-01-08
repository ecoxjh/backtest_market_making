from qstrader.order_sizer.volume_handler import VolumeHandler

class MarketMakingVolumeHandler(VolumeHandler):

    def __init__(self, broker,
        data_handler, lims_daily = [-0.02, 0.02], lims_total = [ 0.2, 0.8]):
        
        self.broker = broker
        self.data_handler = data_handler
        self.lims_daily = self._check_set_lims_daily(lims_daily)
        self.lims_total = self._check_set_lims_total(lims_total)
        # self.daily_position = [self.broker.current_time, self.broker.initial_position]
        
    def _check_set_lims_daily(self, lims_daily):

        if (lims_daily[0] >= 0.0):
            raise ValueError('日持仓变动幅度下限设置错误: "%s" .' % lims_daily[0])
        elif(lims_daily[1] <= 0.0):
            raise ValueError('日持仓变动幅度上限设置错误: "%s" .' % lims_daily[1])
        else:
            return {'min': lims_daily[0], 'max' : lims_daily[1]}

    def _check_set_lims_total(self, lims_total):

        return {'min': lims_total[0], 'max' : lims_total[1]}
        
        
    def check_volume_change(self):
        """
        每条k ,check一次
        """
        
        current_volume_ratio = self.broker.get_account_position()
        volume_changed = current_volume_ratio - self.broker.daily_position
        
        if self.broker.deal_volume_time is None:
            if current_volume_ratio <= self.lims_total['min']:
                self.broker.lims_flag = -2
            elif current_volume_ratio >= self.lims_total['max']:
                self.broker.lims_flag = 2   
            elif volume_changed < self.lims_daily['min']:
                self.broker.lims_flag = -3
            elif volume_changed > self.lims_daily['max']:
                self.broker.lims_flag = 3
            else:
                self.broker.lims_flag = 0
            
        else:
            if self.broker.current_k < self.broker.data_handler.data_source.data_info['DealIndex'][self.broker.current_day]:
                if volume_changed < self.lims_daily['min']:
                    self.broker.lims_flag = -1
                elif volume_changed > self.lims_daily['max']:
                    self.broker.lims_flag = 1
                else:
                    self.broker.lims_flag = 0
            else:
                if self.broker.portfolio['total_volume'] - self.broker.initial_portfolio['total_volume'] > 100:
                    self.broker.lims_flag = 2
                elif self.broker.portfolio['total_volume'] - self.broker.initial_portfolio['total_volume'] < 100:
                    self.broker.lims_flag = -2
                else:
                    self.broker.lims_flag = 666
            
        