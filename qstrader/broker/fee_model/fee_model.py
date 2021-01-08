from abc import ABCMeta, abstractmethod


class FeeModel(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def _calc_commission(self, consideration, broker=None):
        raise NotImplementedError(
            "Should implement _calc_commission()"
        )

    @abstractmethod
    def _calc_tax(self, consideration, broker=None):
        raise NotImplementedError(
            "Should implement _calc_tax()"
        )

    @abstractmethod
    def calc_total_cost(self, consideration, broker=None):
        raise NotImplementedError(
            "Should implement calc_total_cost()"
        )
