def get_nested_tuple_dict():
    return {((1, 2), 3): "nested", ((4, 5), (6, 7)): "deep"}

def lookup_nested(d, key):
    return d[key]
