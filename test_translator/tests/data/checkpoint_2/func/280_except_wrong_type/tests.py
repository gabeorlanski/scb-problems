# Test expects TypeError, solution raises TypeError
try:
    raise_type_error()
    assert False
except TypeError:
    pass
