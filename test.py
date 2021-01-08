from qstrader.trading.backtest import BacktestTradingSession
from qstrader.statistics.tearsheet import TearsheetStatistics

if __name__ == "__main__":
    '''
    price_type  :  spread    价位策略   params : 
                   percent   比例策略
                   iopv      IOPV策略
                   trading   盘口策略
    volume_type :  signal    单笔数量
                   mupltiple 分档数量
                   saopan    扫盘策略
    '''
    
    test = BacktestTradingSession(code = '510300',
                                  initial_cash = 5e7, 
                                  initial_position = 0.4,
                                  cac_stat = True,
                                  daily_start_time = '09:30:00',
                                  deal_volume_time = None, # None 表示不平仓
                                  keep_order_time = 10,
                                  bid_price_type = 'spread',
                                  ask_price_type = 'spread',
                                  bid_volume_type = 'multiple',
                                  ask_volume_type = 'multiple',
                                  spread_level = 0.01,
                                  min_bid_unit = 0.001,
                                  min_ask_unit = 0.001,
                                  volume_bid = [50000,60000,1500000],
                                  volume_ask = [50000,60000,80000])
    
    test.run()
    stat = test.broker.get_market_making_statistic()
    
    # Performance Output
    tearsheet = TearsheetStatistics(
        strategy_equity = test.get_equity_curve(),
        # benchmark_equity = benchmark_equity_df,
        title='',
        periods = test.get_period()
        # market_making_stat = stat['result']
        )
    tearsheet.plot_results()
    print('做市义务：')
    print(stat['result'])


