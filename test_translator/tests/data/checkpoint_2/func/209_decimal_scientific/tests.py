from decimal import Decimal

result = get_scientific()
assert result == Decimal("1.5E2")
assert result == Decimal("150")
