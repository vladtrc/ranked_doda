from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("app") \
    .getOrCreate()


def read_data(name):
    return (spark
            .read
            .options(header=True)
            .csv(f"../../data/{name}.csv")
            )


def view(view_name: str, select: str):
    spark.sql(select).createOrReplaceTempView(view_name)
