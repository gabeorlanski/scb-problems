from decimal import Decimal

# abs with Decimal values
a, b = get_decimal_pair()
assert abs(a - b) < Decimal("0.2")
