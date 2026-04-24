import re

# .* matches anything including empty string
try:
    raise_anything()
    assert False
except ValueError as e:
    assert re.search(r".*", str(e))
