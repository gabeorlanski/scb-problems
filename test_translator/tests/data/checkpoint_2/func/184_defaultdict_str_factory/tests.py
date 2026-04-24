from collections import defaultdict

result = get_str_defaultdict()
assert result == defaultdict(str, {"a": "hello", "b": "world"})
assert result["a"] == "hello"
