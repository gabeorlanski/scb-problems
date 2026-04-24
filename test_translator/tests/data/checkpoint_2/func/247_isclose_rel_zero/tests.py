import math

# rel_tol=0 means relative tolerance is disabled, only abs_tol matters
assert math.isclose(get_rel_zero_val(), 1.0, rel_tol=0, abs_tol=0.01)
