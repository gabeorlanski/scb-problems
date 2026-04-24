from collections import defaultdict


def compare_factories():
    # Same values, different factories
    return defaultdict(int, {"a": 1}), defaultdict(str, {"a": 1})
