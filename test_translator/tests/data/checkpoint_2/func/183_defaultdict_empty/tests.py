from collections import defaultdict

result = get_empty_defaultdict()
assert result == defaultdict(int)
assert result == {}
assert len(result) == 0
