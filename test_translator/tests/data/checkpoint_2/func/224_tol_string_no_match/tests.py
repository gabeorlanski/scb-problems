# String "3.14" should NOT match float 3.14 even with tolerance
s, f = get_string_float()
assert s == "3.14"
assert f == 3.14
assert s != f
