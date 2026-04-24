from collections import defaultdict

result = get_lambda_defaultdict()
assert result == defaultdict(lambda: "default", {"a": "value"})
assert result["a"] == "value"
