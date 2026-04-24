# abs(b - a) < tol is same as abs(a - b) < tol
a, b = get_pair()
tol = 0.01
assert abs(b - a) < tol
assert abs(a - b) < tol
