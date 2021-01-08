from collections import OrderedDict

from qstrader.broker.portfolio.position import Position


class PositionHandler(object):


    def __init__(self):

        self.positions = OrderedDict()

    def transact_position(self, transaction):

        stock_id = transaction.stock_id
        if stock_id in self.positions:
            self.positions[stock_id].transact(transaction)
        else:
            position = Position.open_from_transaction(transaction)
            self.positions[stock_id] = position

        # If the position has zero quantity remove it
        if self.positions[stock_id].net_quantity == 0:
            del self.positions[stock_id]

    def total_market_value(self):

        return sum(
            pos.market_value
            for stock_id, pos in self.positions.items()
        )

    def total_unrealised_pnl(self):

        return sum(
            pos.unrealised_pnl
            for stock_id, pos in self.positions.items()
        )

    def total_realised_pnl(self):

        return sum(
            pos.realised_pnl
            for stock_id, pos in self.positions.items()
        )

    def total_pnl(self):

        return sum(
            pos.total_pnl
            for stock_id, pos in self.positions.items()
        )
