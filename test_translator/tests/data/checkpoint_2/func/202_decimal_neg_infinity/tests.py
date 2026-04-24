from decimal import Decimal

result = get_decimal_neg_inf()
assert result == Decimal("-Infinity")
assert result.is_infinite()
assert result < Decimal("0")
