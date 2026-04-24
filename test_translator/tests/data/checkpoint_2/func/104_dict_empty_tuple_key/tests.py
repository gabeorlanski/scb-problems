assert get_empty_tuple_dict() == {(): "empty_tuple", (1,): "single"}
assert lookup_tuple({(): "found"}, ()) == "found"
