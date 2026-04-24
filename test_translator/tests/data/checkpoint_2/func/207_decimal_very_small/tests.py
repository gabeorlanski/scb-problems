from decimal import Decimal

result = get_very_small()
assert result == Decimal("1E-100")
assert result > Decimal("0")
