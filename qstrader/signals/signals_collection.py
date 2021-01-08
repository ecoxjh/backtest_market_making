class SignalsCollection(object):


    def __init__(self, signals, data_handler):
        self.signals = signals
        self.data_handler = data_handler
        self.warmup = 0  # Used for 'burn in'

    def __getitem__(self, signal):

        return self.signals[signal]

    def update(self, dt):

        for name, signal in self.signals.items():
            self.signals[name].update_assets(dt)

        # Update all of the signals with new prices
        for name, signal in self.signals.items():
            stocks = signal.assets
            for asset in assets:
                price = self.data_handler.get_asset_latest_mid_price(dt, asset)
                self.signals[name].append(asset, price)
        self.warmup += 1
