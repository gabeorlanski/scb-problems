# Case-sensitive matching - "Error" != "error"
try:
    raise_case()
    assert False
except ValueError as e:
    assert "Error" in str(e)
    assert "error" not in str(e)
