from collections import Counter


def get_mixed_counter():
    return Counter({1: 2, "a": 3, (1, 2): 1})
