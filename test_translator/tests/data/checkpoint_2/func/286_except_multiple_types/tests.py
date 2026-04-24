# Catch multiple exception types
try:
    raise_one_of_many()
    assert False
except (ValueError, TypeError):
    pass
