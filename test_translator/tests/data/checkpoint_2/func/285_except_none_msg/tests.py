# Exception with no arguments - str(e) is empty string
try:
    raise_no_args()
    assert False
except ValueError as e:
    assert str(e) == ""
