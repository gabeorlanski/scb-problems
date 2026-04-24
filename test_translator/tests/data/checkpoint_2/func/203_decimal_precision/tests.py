from decimal import Decimal

result = get_high_precision()
assert result == Decimal("0.123456789012345678901234567890")
