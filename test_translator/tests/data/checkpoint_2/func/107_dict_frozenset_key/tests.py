assert get_frozenset_key_dict() == {frozenset([1, 2]): "one_two", frozenset([3]): "three"}
assert lookup_frozenset({frozenset(["a", "b"]): "found"}, frozenset(["a", "b"])) == "found"
