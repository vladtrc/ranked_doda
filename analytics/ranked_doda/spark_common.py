from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("app") \
    .config("spark.driver.memory", "10g") \
    .getOrCreate()


def read_data(name):
    return (spark
            .read
            .options(header=True)
            .csv(f"../data/{name}.csv")
            )


def view(view_name: str, select: str):
    res = spark.sql(select)
    res.createOrReplaceTempView(view_name)
    return res
