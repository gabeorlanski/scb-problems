from collections import deque

result = get_maxlen_one()
assert result == deque([1], maxlen=1)
assert len(result) == 1
