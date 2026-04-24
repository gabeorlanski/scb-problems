try:
    parse("bad")
    assert False
except Exception as e:
    msg = str(e)
    assert "parse" in msg
    assert "failed" in msg
