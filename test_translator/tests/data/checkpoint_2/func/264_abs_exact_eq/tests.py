# abs(a - b) == tol (exact equality, not < or <=)
a, b, tol = get_boundary_pair()
assert abs(a - b) == tol
