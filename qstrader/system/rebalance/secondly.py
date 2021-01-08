import pandas as pd
import datetime

from qstrader.system.rebalance.rebalance import Rebalance


class SecondlyRebalance(Rebalance):

    def __init__(self, start_date, end_date):

        self.start_date = start_date
        self.end_date = end_date
        self.rebalances = self._generate_rebalances()

    def _generate_daily_rebalances(self):
        part1 = pd.date_range(start = '09:30:00', end = '11:30:00', freq = '3S')
        part2 = pd.date_range(start = '13:00:00', end = '15:00:00', freq = '3S')
        
        daily_list = [' 09:25:00']
        daily_list.extend([str(x)[10:] for x in part1])
        daily_list.extend([str(x)[10:] for x in part2])
        
        return daily_list
        
    def _generate_rebalances(self):
        start_date = datetime.datetime.strptime(self.start_date,'%Y-%m-%d')
        end_date = datetime.datetime.strptime(self.end_date, '%Y-%m-%d')
        
        delta_day = end_date - start_date
        daily_list = self._generate_daily_rebalances()
        
        rebalances_list = []
        for i in range(delta_day.days+1):
            current_date = str(start_date + datetime.timedelta(days = i))[:10]
            rebalances_list.extend([current_date + i for i in daily_list])
            
        rebalances = [datetime.datetime.strptime(i,'%Y-%m-%d %H:%M:%S') for i in rebalances_list]

        return rebalances
