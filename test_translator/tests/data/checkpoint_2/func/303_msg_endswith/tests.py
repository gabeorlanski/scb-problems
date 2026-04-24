# Check message ends with substring
try:
    raise_ends()
    assert False
except ValueError as e:
    assert str(e).endswith("failed")
