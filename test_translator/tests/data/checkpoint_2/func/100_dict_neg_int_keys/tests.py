assert get_neg_int_dict() == {-1: "neg_one", -2: "neg_two", -100: "neg_hundred"}
assert lookup_neg({-5: "five", -10: "ten"}, -5) == "five"
