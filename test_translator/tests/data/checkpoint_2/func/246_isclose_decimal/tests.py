import math
from decimal import Decimal

# isclose with Decimal arguments
assert math.isclose(get_decimal_isclose(), Decimal("3.14"), rel_tol=Decimal("0.01"))
