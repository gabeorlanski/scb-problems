import re

# MULTILINE flag - ^ and $ match at line boundaries
try:
    raise_multiline()
    assert False
except ValueError as e:
    assert re.search(r"^line2", str(e), re.MULTILINE)
