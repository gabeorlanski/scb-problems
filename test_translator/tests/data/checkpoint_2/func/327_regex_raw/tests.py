import re

# Raw string vs non-raw for regex
try:
    raise_with_digits()
    assert False
except ValueError as e:
    assert re.search(r"\d+", str(e))  # raw string
