# Multiple "in" checks
try:
    raise_multi()
    assert False
except ValueError as e:
    assert "error" in str(e) and "value" in str(e)
