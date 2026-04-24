# Nested abs: abs(abs(a) - b)
a, b = get_nested_abs()
assert abs(abs(a) - b) < 0.01
