# KeyError is a subclass of LookupError
# catching LookupError should catch KeyError
try:
    raise_key_error()
    assert False
except LookupError:
    pass
