from collections import Counter
from decimal import Decimal

# Counter with Decimal keys
assert get_decimal_counter() == Counter({Decimal("1.5"): 2, Decimal("2.5"): 3})
