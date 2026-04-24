# Nested try/except blocks
try:
    nested_exceptions()
    assert False
except RuntimeError as e:
    assert "outer" in str(e)
