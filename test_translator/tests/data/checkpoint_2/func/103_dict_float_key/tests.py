assert get_float_key_dict() == {1.5: "one_half", 2.25: "two_quarter", -0.5: "neg_half"}
assert lookup_float({3.14: "pi"}, 3.14) == "pi"
