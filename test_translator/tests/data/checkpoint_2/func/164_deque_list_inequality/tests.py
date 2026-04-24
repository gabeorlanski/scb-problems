from collections import deque

# In Python, deque([1,2]) != [1,2] - they are different types
d, l = deque_not_list()
assert d != l
assert d == deque([1, 2])
