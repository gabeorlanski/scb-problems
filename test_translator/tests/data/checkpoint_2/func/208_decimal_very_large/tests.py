from decimal import Decimal

result = get_very_large()
assert result == Decimal("1E100")
