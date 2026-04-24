# Empty string is always "in" any string
try:
    raise_any()
    assert False
except ValueError as e:
    assert "" in str(e)
