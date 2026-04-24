import math

# isclose with NaN is always False, even NaN vs NaN
result = get_nan_val()
assert math.isnan(result)
assert not math.isclose(result, float("nan"))
