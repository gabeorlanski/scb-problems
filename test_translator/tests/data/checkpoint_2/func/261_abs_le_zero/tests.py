# abs(a - b) <= 0 is true only when a == b exactly
a, b = get_equal_values()
assert abs(a - b) <= 0  # Only true when exactly equal
