from abc import ABCMeta, abstractmethod


class Broker(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def subscribe_funds_to_account(self, amount):
        raise NotImplementedError(
            "Should implement subscribe_funds_to_account()"
        )

    @abstractmethod
    def withdraw_funds_from_account(self, amount):
        raise NotImplementedError(
            "Should implement withdraw_funds_from_account()"
        )

    @abstractmethod
    def get_account_cash_balance(self, currency=None):
        raise NotImplementedError(
            "Should implement get_account_cash_balance()"
        )

    @abstractmethod
    def get_account_total_non_cash_equity(self):
        raise NotImplementedError(
            "Should implement get_account_total_non_cash_equity()"
        )

    @abstractmethod
    def get_account_total_equity(self):
        raise NotImplementedError(
            "Should implement get_account_total_equity()"
        )

    @abstractmethod
    def create_portfolio(self, portfolio_id, name):
        raise NotImplementedError(
            "Should implement create_portfolio()"
        )

    @abstractmethod
    def list_all_portfolios(self):
        raise NotImplementedError(
            "Should implement list_all_portfolios()"
        )

    @abstractmethod
    def subscribe_funds_to_portfolio(self, portfolio_id, amount):
        raise NotImplementedError(
            "Should implement subscribe_funds_to_portfolio()"
        )

    @abstractmethod
    def withdraw_funds_from_portfolio(self, portfolio_id, amount):
        raise NotImplementedError(
            "Should implement withdraw_funds_from_portfolio()"
        )

    @abstractmethod
    def get_portfolio_cash_balance(self, portfolio_id):
        raise NotImplementedError(
            "Should implement get_portfolio_cash_balance()"
        )

    @abstractmethod
    def get_portfolio_total_non_cash_equity(self, portfolio_id):
        raise NotImplementedError(
            "Should implement get_portfolio_total_non_cash_equity()"
        )

    @abstractmethod
    def get_portfolio_total_equity(self, portfolio_id):
        raise NotImplementedError(
            "Should implement get_portfolio_total_equity()"
        )

    @abstractmethod
    def get_portfolio_as_dict(self, portfolio_id):
        raise NotImplementedError(
            "Should implement get_portfolio_as_dict()"
        )

    @abstractmethod
    def submit_order(self, portfolio_id, order):
        raise NotImplementedError(
            "Should implement submit_order()"
        )
