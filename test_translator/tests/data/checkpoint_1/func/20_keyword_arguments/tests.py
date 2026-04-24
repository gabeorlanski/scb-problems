# Test keyword arguments in function calls
assert add(a=1, b=2) == 3
assert add(b=5, a=10) == 15
assert add(a=0, b=0) == 0
assert add(a=-5, b=5) == 0
