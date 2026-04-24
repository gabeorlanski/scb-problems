from decimal import Decimal

result = get_neg_zero()
# Decimal("-0") is signed but equals Decimal("0")
assert result == Decimal("0")
assert result == Decimal("-0")
