from decimal import Decimal

result = get_decimal_key_dict()
expected = {
    Decimal("1.5"): "a",
    Decimal("2.25"): "b",
    (Decimal("3.0"), "x"): "tuple",
}
assert result == expected
