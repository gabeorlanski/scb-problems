from collections import deque


def compare_maxlen():
    # Same content, different maxlen - should be equal
    return deque([1, 2], maxlen=2), deque([1, 2], maxlen=10)
