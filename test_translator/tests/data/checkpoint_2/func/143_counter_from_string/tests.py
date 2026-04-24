from collections import Counter

assert count_string("aaa") == Counter({"a": 3})
assert count_string("abba") == Counter({"a": 2, "b": 2})
assert count_string("") == Counter()
