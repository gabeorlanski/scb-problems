from decimal import Decimal

result = get_decimal_nan()
# NaN is not equal to anything, including itself
assert result.is_nan()
assert result != result  # NaN != NaN
