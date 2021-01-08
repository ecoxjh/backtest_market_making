from qstrader.order_sizer.market_making_volume_handler2 import MarketMakingVolumeHandler
from qstrader.execution.execution_handler import ExecutionHandler

class QuantTradingSystem(object):

    def __init__(
        self,
        broker,
        data_handler,
        risk_model=None,
        *args,
        **kwargs
    ):
        self.broker = broker

        self.data_handler = data_handler
        self.risk_model = risk_model
        self.execution_handler = self._initialize_models(**kwargs)

    def _create_volume_handler(self):
        
        volume_handler = MarketMakingVolumeHandler(self.broker, self.data_handler)

        return volume_handler

    def _initialize_models(self, **kwargs):

        volume_handler = self._create_volume_handler()

        execution_handler = ExecutionHandler(self.broker,
                                                  volume_handler,
                                                  data_handler=self.data_handler)
        return execution_handler
    
    def _check_volume_ratio(self):
        """
        k线开始时运行
        """
            
        self.execution_handler.volume_handler.check_volume_change()
        
        
    def __call__(self):  
        
        self._check_volume_ratio()