from decimal import Decimal

# Decimal("1") == 1 in Python
d, i = compare_decimal_int()
assert d == i
assert d == 1
assert Decimal("5") == 5
