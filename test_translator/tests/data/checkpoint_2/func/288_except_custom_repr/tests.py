# Exception with custom __str__
try:
    raise_custom()
    assert False
except ValueError as e:
    assert "custom" in str(e)
