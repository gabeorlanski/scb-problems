from collections import Counter

# Counter compares equal to regular dict with same values
result = counter_equals_dict()
assert result == {"a": 1, "b": 2}
assert Counter({"x": 5}) == {"x": 5}
