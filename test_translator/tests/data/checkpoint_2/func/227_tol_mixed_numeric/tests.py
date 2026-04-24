from decimal import Decimal

# Mixed float and Decimal in same structure
result = get_mixed_numeric()
assert result == [1.0, Decimal("2.0"), 3]
