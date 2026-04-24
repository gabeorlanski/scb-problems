# Set equality should not depend on insertion order
assert get_ordered_set() == {3, 2, 1}
assert {1, 2, 3} == {3, 1, 2}
assert get_ordered_set() == {1, 2, 3}
