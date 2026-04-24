# Empty exception message
try:
    raise_empty_msg()
    assert False
except ValueError as e:
    assert str(e) == ""
