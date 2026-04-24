from decimal import Decimal


def get_decimal_key_dict():
    return {
        Decimal("1.5"): "a",
        Decimal("2.25"): "b",
        (Decimal("3.0"), "x"): "tuple",
    }
