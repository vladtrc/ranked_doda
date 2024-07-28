from pyspark.sql.functions import col, udf
from pyspark.sql.types import FloatType
from spark_common import spark

@udf(returnType=FloatType()) 
def estimate_deviation(value, min_from, max_from, min_to, max_to):
    distance_to_min_from = abs(value - min_from)
    distance_to_max_from = abs(value - max_from)
    length_from = abs(max(min_from, max_from) - min(min_from, max_from))
    if any((distance_to_min_from > length_from, distance_to_max_from > length_from)):
        if distance_to_min_from < distance_to_max_from:
            return min_to
        else:
            return max_to
    length_to = abs(min_to - max_to)
    p = (value - min(min_from, max_from)) / length_from
    offset = p * length_to
    reversed = (min_from < max_from) ^ (min_to < max_to)
    return max(min_to, max_to) - offset if reversed else offset + min(min_to, max_to)

estimate_deviation_udf = udf(estimate_deviation)
spark.udf.register("estimate_deviation", estimate_deviation)

def assertion(cond):
    if not cond:
        raise AssertionError("cond false")

# assertion(1.5 == estimate_deviation(-100, 10, 2, 0.5, 1.5))
# assertion(1.5 == estimate_deviation(0, 10, 2, 0.5, 1.5))
# assertion(1.125 == estimate_deviation(5, 10, 2, 0.5, 1.5))
# assertion(1.0 == estimate_deviation(6, 10, 2, 0.5, 1.5))
# assertion(0.875 == estimate_deviation(7, 10, 2, 0.5, 1.5))
# assertion(0.5 == estimate_deviation(11, 10, 2, 0.5, 1.5))
# assertion(0.5 == estimate_deviation(100, 10, 2, 0.5, 1.5))


# assertion(0.5  == estimate_deviation(-100, 10, 2, 1.5, 0.5))
# assertion(0.5  == estimate_deviation(0, 10, 2, 1.5, 0.5))
# assertion(0.875 == estimate_deviation(5, 10, 2, 1.5, 0.5))
# assertion(1.0 == estimate_deviation(6, 10, 2, 1.5, 0.5))
# assertion(1.125 == estimate_deviation(7, 10, 2, 1.5, 0.5))
# assertion(1.5 == estimate_deviation(11, 10, 2, 1.5, 0.5))
# assertion(1.5 == estimate_deviation(100, 10, 2, 1.5, 0.5))


# assertion(0.5 == estimate_deviation(-100, 2, 10, 0.5, 1.5))
# assertion(0.5 == estimate_deviation(0, 2, 10, 0.5, 1.5))
# assertion(0.875 == estimate_deviation(5, 2, 10, 0.5, 1.5))
# assertion(1.0 == estimate_deviation(6, 2, 10, 0.5, 1.5))
# assertion(1.125 == estimate_deviation(7, 2, 10, 0.5, 1.5))
# assertion(1.5 == estimate_deviation(11, 2, 10, 0.5, 1.5))
# assertion(1.5 == estimate_deviation(100, 2, 10, 0.5, 1.5))
