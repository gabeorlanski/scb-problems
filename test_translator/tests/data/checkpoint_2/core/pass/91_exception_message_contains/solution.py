def validate(x):
    if x < 0:
        raise ValueError("negative value not allowed")
