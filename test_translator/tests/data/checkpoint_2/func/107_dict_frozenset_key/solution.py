def get_frozenset_key_dict():
    return {frozenset([1, 2]): "one_two", frozenset([3]): "three"}

def lookup_frozenset(d, key):
    return d[key]
