from collections import deque

# Deque with maxlen=0 is always empty
result = get_maxlen_zero()
assert result == deque([], maxlen=0)
assert len(result) == 0
