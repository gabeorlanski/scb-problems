import math

# isclose with infinity - inf == inf
assert math.isclose(get_inf_val(), float("inf"))
