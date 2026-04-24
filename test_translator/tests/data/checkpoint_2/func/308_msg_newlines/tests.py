# Newlines in message
try:
    raise_newlines()
    assert False
except ValueError as e:
    assert "\n" in str(e)
    assert "line1" in str(e)
