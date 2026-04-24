from decimal import Decimal

result = get_decimal_inf()
assert result == Decimal("Infinity")
assert result.is_infinite()
