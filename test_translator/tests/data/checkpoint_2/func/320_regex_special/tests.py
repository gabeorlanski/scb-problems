import re

# Regex with special characters . * ?
try:
    raise_special_regex()
    assert False
except ValueError as e:
    assert re.search(r"error.*occurred", str(e))
