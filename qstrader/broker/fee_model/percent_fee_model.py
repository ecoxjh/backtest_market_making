from qstrader.broker.fee_model.fee_model import FeeModel

class PercentFeeModel(FeeModel):

    def __init__(self, commission_pct=0.00002, tax_pct=0.0):
        super().__init__()
        self.commission_pct = commission_pct
        self.tax_pct = tax_pct

    def _calc_commission(self, consideration, broker = None):
        
        return self.commission_pct * abs(consideration)

    def _calc_tax(self, consideration, broker = None):

        return self.tax_pct * abs(consideration)

    def calc_total_cost(self, consideration, broker=None):

        commission = self._calc_commission(consideration, broker)
        tax = self._calc_tax( consideration, broker)
        total = round(commission + tax,4)
        
        return total
