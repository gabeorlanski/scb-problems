import re

# Using re.search with third flags argument
try:
    raise_multiword()
    assert False
except ValueError as e:
    assert re.search(r"ERROR", str(e), re.IGNORECASE)
