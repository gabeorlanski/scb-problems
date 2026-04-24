from collections import deque


def get_nested_deque():
    return deque([deque([1, 2]), deque([3])])
