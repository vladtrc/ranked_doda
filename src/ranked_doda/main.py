from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("app") \
    .getOrCreate()

print(1)

for e in ["hero", "user_result", "user", "match"]:
    spark.read.options(header=True).csv(f"../../data/{e}.csv").createTempView(e)

spark.sql("select * from hero").show()
