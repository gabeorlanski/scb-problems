import re

# Case-insensitive regex match
try:
    raise_case_insensitive()
    assert False
except ValueError as e:
    assert re.search(r"error", str(e), re.IGNORECASE)
