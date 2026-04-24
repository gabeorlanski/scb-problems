# Test parenthesized expressions - valid Python syntax variations
assert (add(1, 2) == 3)
assert (add(0, 0) == 0)
assert (add(1, 2)) == 3
assert (add(1, 1))
assert not (add(0, 0))
