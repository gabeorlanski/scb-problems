import math

# isclose(0, 0) should always be True
assert math.isclose(get_zero(), 0, rel_tol=0.01)
