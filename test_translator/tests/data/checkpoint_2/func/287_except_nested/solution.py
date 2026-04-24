def nested_exceptions():
    try:
        raise ValueError("inner")
    except ValueError:
        raise RuntimeError("outer error")
