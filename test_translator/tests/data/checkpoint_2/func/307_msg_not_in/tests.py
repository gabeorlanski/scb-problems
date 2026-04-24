# "not in" pattern
try:
    raise_not_in()
    assert False
except ValueError as e:
    assert "success" not in str(e)
