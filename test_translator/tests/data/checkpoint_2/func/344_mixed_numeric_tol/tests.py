from decimal import Decimal

# Mixed numeric types with tolerance
result = get_mixed_numbers()
assert result == [1, 1.0, Decimal("1")]
