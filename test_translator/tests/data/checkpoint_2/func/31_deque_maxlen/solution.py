from collections import deque


def make_bounded_deque():
    d = deque([1, 2, 3, 4, 5], maxlen=3)
    return d  # Last 3 elements: [3, 4, 5]
