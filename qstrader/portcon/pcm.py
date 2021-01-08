from qstrader import settings
from qstrader.execution.order import Order


class PortfolioConstructionModel(object):

    def __init__(
        self,
        broker,
        broker_portfolio_id,
        volume_handler,
        optimizer,
        alpha_model=None,
        risk_model=None,
        cost_model=None,
        data_handler=None,
    ):
        self.broker = broker
        self.broker_portfolio_id = broker_portfolio_id
        self.volume_handler = volume_handler
        self.optimizer = optimizer
        self.alpha_model = alpha_model
        self.risk_model = risk_model
        self.cost_model = cost_model
        self.data_handler = data_handler

    def _obtain_full_stock_list(self, dt):

        broker_portfolio = self.broker.get_portfolio_as_dict(
            self.broker_portfolio_id
        )
        broker_stocks = list(broker_portfolio.keys())
        return broker_stocks

    def _generate_target_portfolio(self, dt, weights):

        return self.volume_handler(dt, weights)

    def _obtain_current_portfolio(self):

        return self.broker.get_portfolio_as_dict(self.broker_portfolio_id)

    def __call__(self, i_k, stats=None):


        # Obtain current Broker account portfolio
        current_portfolio = self._obtain_current_portfolio()

        # Create rebalance trade Orders
        rebalance_orders = self._generate_rebalance_orders(
            dt, target_portfolio, current_portfolio
        )
        # TODO: Implement cost model

        return rebalance_orders
