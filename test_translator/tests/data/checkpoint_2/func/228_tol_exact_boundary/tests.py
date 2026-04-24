# Value exactly at tolerance boundary
# 1.0 + 0.001 = 1.001, should pass with tol=0.001
assert get_boundary() == 1.001
