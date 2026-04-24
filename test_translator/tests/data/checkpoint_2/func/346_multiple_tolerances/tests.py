import math

# Multiple assertions with different tolerance types
v1, v2, v3 = get_multi_values()
assert math.isclose(v1, 1.0, abs_tol=0.01)
assert abs(v2 - 2.0) < 0.05
assert v3 == 3  # Exact match
