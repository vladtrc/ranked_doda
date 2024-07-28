from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("app") \
    .config("spark.driver.memory", "10g") \
    .getOrCreate()


def read_data(name):
    spark.read.options(header=True).csv(f"../data/{name}.csv").createOrReplaceTempView(name)
    return spark.sql(f"select * from {name}")


def view(view_name: str, select: str):
    res = spark.sql(select)
    res.createOrReplaceTempView(view_name)
    return res
