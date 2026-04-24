from collections import deque

result = get_empty_maxlen()
assert result == deque([], maxlen=5)
assert len(result) == 0
