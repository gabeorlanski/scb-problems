def get_empty_tuple_dict():
    return {(): "empty_tuple", (1,): "single"}

def lookup_tuple(d, key):
    return d[key]
