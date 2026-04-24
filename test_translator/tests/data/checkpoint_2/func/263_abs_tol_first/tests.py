# tol > abs(a - b) is equivalent to abs(a - b) < tol
a, b, tol = get_values_tol()
assert tol > abs(a - b)
