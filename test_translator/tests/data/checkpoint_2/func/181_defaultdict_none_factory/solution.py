from collections import defaultdict


def get_none_factory():
    return defaultdict(None, {"a": 1, "b": 2})
