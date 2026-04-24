from collections import Counter
from decimal import Decimal


def get_decimal_counter():
    return Counter({Decimal("1.5"): 2, Decimal("2.5"): 3})
