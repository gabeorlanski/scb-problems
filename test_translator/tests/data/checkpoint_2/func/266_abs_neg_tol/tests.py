# Edge case: negative tolerance (abs is always >= 0, so always fails)
a, b, tol = get_with_neg_tol()
assert not abs(a - b) < tol  # Should be False when tol < 0
