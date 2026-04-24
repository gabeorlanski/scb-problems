assert get_nested_tuple_dict() == {((1, 2), 3): "nested", ((4, 5), (6, 7)): "deep"}
assert lookup_nested({((1,), (2,)): "value"}, ((1,), (2,))) == "value"
