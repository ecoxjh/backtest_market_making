import pandas as pd
import os
import time
from qstrader.data.csv_data_source import CSVDataSource
from qstrader.data.backtest_data_handler import BacktestDataHandler
from qstrader.broker.simulated_broker import SimulatedBroker
from qstrader.trading.trading_session import TradingSession
from qstrader.system.qts import QuantTradingSystem
from qstrader.signals.rootnet import RootNetSignal as StrategySignals
# from qstrader import settings

class BacktestTradingSession(TradingSession):
    
    def __init__(self, code = None, start_time = None, end_time = None, daily_start_time = '09:30:00', deal_volume_time = None, keep_order_time = 10,
                 initial_cash = 1e7, initial_position = 0.5, burn_in_days = 0, cac_stat = True, **kwargs):
        self.time_start = time.time()
        self.code = code
        self.start_time = start_time
        self.end_time = end_time
        self.daily_start_time = daily_start_time
        self.deal_volume_time = deal_volume_time
        self.keep_order_time = keep_order_time
        self.initial_cash = initial_cash
        self.initial_position = initial_position
        self.burn_in_days = burn_in_days
        self.cac_stat = cac_stat
        
        self.start_day = self._get_start_day()
        
        self.data_handler = self._create_data_handler()
        self.broker = self._create_broker()
        self.risk_model = None
        
        self.qts = self._create_quant_trading_system()
        self.signals = self._create_strategy_signals(**kwargs)
        self.equity_curve = []
        
    def _get_start_day(self):
            
        if self.burn_in_days is None:
            start_day = 0
                
            return start_day
        else:
            start_day = self.burn_in_days
                
            return start_day
                
    def _create_data_handler(self):
            
        csv_dir = os.getcwd() + '\\data\\SH\\' 
        data_source = CSVDataSource(csv_dir, self.start_time, self.end_time, code = self.code,
                                    daily_start_time = self.daily_start_time, deal_volume_time = self.deal_volume_time)
        data_handler = BacktestDataHandler(data_source)
            
        return data_handler
    
    def _create_broker(self):
            
        broker = SimulatedBroker(self.start_day, self.data_handler,
                                 keep_order_time = self.keep_order_time, deal_volume_time = self.deal_volume_time,
                                 initial_cash = self.initial_cash, initial_position = self.initial_position, cac_stat = self.cac_stat)
            
        return broker

    def _create_quant_trading_system(self, **kwargs):

        qts = QuantTradingSystem(self.broker,
                                 self.data_handler,
                                 self.risk_model)
            
        return qts
    
    def _create_strategy_signals(self, **kwargs):
            
        signals = StrategySignals(self.broker, **kwargs)
        
        return signals
    
    def _update_equity_curve(self):

        self.equity_curve.append((self.broker.current_time, 
                                  self.broker.get_account_market_making_equity(),
                                  self.broker.get_account_position()))
            
    def output_holdings(self):
            
        self.broker.holdings_to_console()
            
    def get_equity_curve(self):
            
        equity_df = pd.DataFrame(self.equity_curve, columns=['DateTime', 'Equity', 'Position']).set_index('DateTime')
        equity_df.index = equity_df.index.strftime('%Y-%m-%d')
        # equity_df.index = pd.to_datetime(equity_df.index, format = '%Y-%m-%d %H:%M:%S')
        # equity_df.index = pd.to_datetime(equity_df.index).date
        # tt = t.loc[t.groupby(['frequency'])['DateTime'].idxmax()]
        return equity_df    
    
    def get_period(self):
        
        period = 240 * sum(self.broker.data_handler.data_source.data_info['NK'].values())//len(self.broker.data_handler.data_source.data_info['NK'])
        
        return period
    
    def get_benchmark_equity(self,benchmark_id):
        bench_dir = os.getcwd() + '\\data\\' + benchmark_id + '.csv'
        csv_df = pd.read_csv(bench_dir,index_col = 0,parse_dates = True).sort_index()
        csv_df['Equity'] = self.initial_cash
        csv_df['Position'] = 1
        
        if self.start_time is not None:
            csv_df = csv_df[csv_df.index >= self.start_time]
        if self.end_time is not None:
            csv_df = csv_df[csv_df.index <= self.end_time]
            
        volume  = csv_df['Equity'].iloc[0]/csv_df['Close'].iloc[0]
        
        for i in range(len(csv_df)):
            csv_df['Equity'].iloc[i] = csv_df['Close'].iloc[i] * volume
        benchmark_equity_df = csv_df.drop(['Close'],axis = 1)
        
        return benchmark_equity_df
        
    def run(self, results = False):
        
        # if settings.PRINT_EVENTS:
        print("Beginning backtest simulation...")
        
        time_start = self.time_start
        for iday in self.data_handler.data_source.data_info['Days']:
            self.broker.prior_day  = self.broker.current_day
            self.broker.current_day = iday

            acomplished =  round(self.broker.data_handler.data_source.data_info['Days'].index(iday)/self.broker.data_handler.data_source.data_info['NDay'] * 100,2)
            print('\r现在进行到 %s 已完成:%s%%, 耗时:%ss ' % (iday, acomplished,round(time.time() - time_start,2)), end = '')

            for i in range(self.data_handler.data_source.data_info['NK'][iday]) :
                
                self.broker.current_k = i
                
                if i < self.broker.data_handler.data_source.data_info['StartIndex'][iday]:
                    continue
                
                if self.broker.data_handler.data_source.data_info['TickIndex'][iday][i] is None:
                    continue
                
                self.broker._update_current_time()
                    
                if len(self.broker.latest_data['tick']) == 0:
                    continue
                
                # 更新lims_flag
                self.qts()
                
                self.signals.update()
                
                self.broker.update()
                
                # 更新order等账户信息
                
                self._update_equity_curve()
                # print(self.equity_curve[-1])
                
            if results:
                self.output_holdings()
    
        # if settings.PRINT_EVENTS:
            
        print('')
        print("Ending backtest simulation.") 
        print('已完成, 耗时:%ss ' % (round(time.time() - time_start,2)), end = '')
