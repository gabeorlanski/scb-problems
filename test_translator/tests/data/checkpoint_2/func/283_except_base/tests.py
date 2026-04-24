# BaseException catches everything including KeyboardInterrupt
try:
    raise_keyboard_interrupt()
    assert False
except BaseException:
    pass
