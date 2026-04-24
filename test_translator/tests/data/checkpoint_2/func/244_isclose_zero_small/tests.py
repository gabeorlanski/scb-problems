import math

# Comparing to zero with rel_tol alone fails - need abs_tol
# rel_tol * 0 = 0, so only abs_tol matters
assert math.isclose(get_small(), 0, abs_tol=0.001)
