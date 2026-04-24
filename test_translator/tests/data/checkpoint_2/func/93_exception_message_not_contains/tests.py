try:
    process("bad")
    assert False
except Exception as e:
    assert "success" not in str(e)
