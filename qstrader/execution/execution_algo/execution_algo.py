from abc import ABCMeta, abstractmethod


class ExecutionAlgorithm(object):


    __metaclass__ = ABCMeta

    @abstractmethod
    def __call__(self, dt, initial_orders):
        raise NotImplementedError(
            "Should implement __call__()"
        )
