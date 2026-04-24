try:
    validate(-1)
    assert False
except ValueError as e:
    assert "negative" in str(e)
