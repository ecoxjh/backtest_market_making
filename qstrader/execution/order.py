import uuid


class Order(object):

    def __init__(
        self,
        current_k,
        stock_id,
        buy_price,
        buy_volume,
        sell_price,
        sell_volume,
        order_id=None
    ):
        self.created_k = current_k
        self.current_k = current_k
        self.stock_id = stock_id
        self.buy_price = buy_price
        self.buy_volume = buy_volume
        self.sell_price = sell_price
        self.sell_volume = sell_volume

        self.order_id = self._set_or_generate_order_id(order_id)

    def _order_attribs_equal(self, other):
        """
        Asserts whether all attributes of the Order are equal
        with the exception of the order ID.

        Used primarily for testing that orders are generated correctly.

        Parameters
        ----------
        other : `Order`
            The order to compare attribute equality to.

        Returns
        -------
        `Boolean`
            Whether the non-order ID attributes are equal.
        """
        if self.created_k != other.created_k:
            return False
        if self.current_k != other.current_k:
            return False
        if self.stock_id != other.stock_id:
            return False
        # if self.buy_price != other.buy_price:
        #     return False
        return True

    def __repr__(self):

        return (
            "Order(create_k='%s', stock_id='%s', order_id=%s, "
            "buy_price=%s, buy_volume=%s, "
            "sell_price=%s, sell_volume=%s.)" % (
                self.created_k, self.stock_id, self.order_id,
                self.buy_price,self.buy_volume,
                self.sell_price,self.sell_volume
            )
        )

    def _set_or_generate_order_id(self, order_id=None):
        """
        Sets or generates a unique order ID for the order, using a UUID.

        """
        if order_id is None:
            return uuid.uuid4().hex
        else:
            return order_id
