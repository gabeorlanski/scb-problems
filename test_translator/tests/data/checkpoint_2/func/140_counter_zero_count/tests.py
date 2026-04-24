from collections import Counter

# Counter with zero counts - element exists but with 0 count
result = get_zero_counter()
assert result == Counter({"a": 0, "b": 1})
assert result["a"] == 0
