from collections import deque


def make_bounded_deque():
    return deque([3, 4, 6], maxlen=3)  # Wrong element
