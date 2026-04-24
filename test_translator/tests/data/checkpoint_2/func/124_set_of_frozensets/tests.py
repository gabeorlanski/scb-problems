assert get_set_of_frozensets() == {frozenset([1, 2]), frozenset([3, 4])}
assert contains_frozenset({frozenset([1])}, frozenset([1])) == True
