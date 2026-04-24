from decimal import Decimal

# Decimal should participate in tolerance comparisons
assert get_decimal_approx() == Decimal("3.145")
