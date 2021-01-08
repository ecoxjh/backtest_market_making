
class ExecutionHandler(object):

    def __init__(
        self,
        broker,
        volume_handler,
        data_handler=None
    ):
        self.broker = broker
        self.volume_handler = volume_handler
        self.data_handler = data_handler

