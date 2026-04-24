from decimal import Decimal


def get_frozenset_decimal():
    return {frozenset([Decimal("1"), Decimal("2")]): "value"}
