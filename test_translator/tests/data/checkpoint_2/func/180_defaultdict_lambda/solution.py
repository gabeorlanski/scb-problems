from collections import defaultdict


def get_lambda_defaultdict():
    return defaultdict(lambda: "default", {"a": "value"})
