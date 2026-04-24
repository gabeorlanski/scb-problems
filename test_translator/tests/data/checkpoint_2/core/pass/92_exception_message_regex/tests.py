import re

try:
    parse("bad")
    assert False
except ValueError as e:
    assert re.search(r"invalid.*format", str(e))
