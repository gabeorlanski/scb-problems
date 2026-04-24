# Special characters in message
try:
    raise_special()
    assert False
except ValueError as e:
    assert "'" in str(e)
    assert '"' in str(e)
