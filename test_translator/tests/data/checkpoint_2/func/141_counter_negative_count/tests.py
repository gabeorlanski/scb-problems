from collections import Counter

# Counter can have negative counts
result = get_negative_counter()
assert result == Counter({"a": -1, "b": 2, "c": -5})
assert result["a"] == -1
