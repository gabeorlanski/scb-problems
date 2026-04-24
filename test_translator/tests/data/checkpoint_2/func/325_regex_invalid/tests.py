import re

# Valid regex pattern (testing that pattern works)
try:
    raise_for_invalid()
    assert False
except ValueError as e:
    assert re.search(r"\[error\]", str(e))
