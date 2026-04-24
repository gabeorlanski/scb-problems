assert get_large_int_dict() == {10**18: "quintillion", 10**15: "quadrillion"}
assert lookup_large({999999999999999999: "big"}, 999999999999999999) == "big"
