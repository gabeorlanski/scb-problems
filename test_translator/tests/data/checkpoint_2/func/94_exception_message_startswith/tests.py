try:
    validate(None)
    assert False
except Exception as e:
    assert str(e).startswith("Error:")
