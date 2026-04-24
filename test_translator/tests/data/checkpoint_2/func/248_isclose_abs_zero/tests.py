import math

# abs_tol=0 means only rel_tol matters
assert math.isclose(get_abs_zero_val(), 100.0, rel_tol=0.01, abs_tol=0)
