# Unicode characters in message
try:
    raise_unicode()
    assert False
except ValueError as e:
    assert "failed" in str(e)
