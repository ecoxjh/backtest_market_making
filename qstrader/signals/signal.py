class Signal(object):

    def __init__(self, broker):
        self.broker = broker
        self.buffers = self._get_stock_price_buffers()

    def _get_stock_price_buffers(self, stock, prior_k, current_k):
        
        price_buffers = self.broker.data_handler.get_stock_slice_data(stock, prior_k, current_k)
            
        return price_buffers

