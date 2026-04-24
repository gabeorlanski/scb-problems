from decimal import Decimal

# In Python, Decimal("0.1") != 0.1 due to float representation
d, f = compare_decimal_float()
assert d != f
assert d == Decimal("0.1")
