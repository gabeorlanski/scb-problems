# abs(a - b) < 0 is always false - can catch bugs
a, b = get_values()
assert not abs(a - b) < 0  # This should always be False
