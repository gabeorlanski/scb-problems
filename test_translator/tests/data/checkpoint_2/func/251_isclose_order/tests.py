import math

# isclose(a, b) should equal isclose(b, a)
a, b = get_pair()
assert math.isclose(a, b, abs_tol=0.01)
assert math.isclose(b, a, abs_tol=0.01)
