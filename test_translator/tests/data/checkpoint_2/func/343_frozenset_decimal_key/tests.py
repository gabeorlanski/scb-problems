from decimal import Decimal

# Dict with frozenset containing Decimal as key
assert get_frozenset_decimal() == {frozenset([Decimal("1"), Decimal("2")]): "value"}
