import re

# Regex anchors ^error and error$
try:
    raise_anchors()
    assert False
except ValueError as e:
    assert re.search(r"^Error", str(e))
    assert re.search(r"failed$", str(e))
