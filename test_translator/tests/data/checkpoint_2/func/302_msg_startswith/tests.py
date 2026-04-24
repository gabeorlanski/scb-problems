# Check message starts with substring
try:
    raise_starts()
    assert False
except ValueError as e:
    assert str(e).startswith("Error:")
