from copy import deepcopy

from pyspark.sql.functions import col
from pyspark.sql.functions import max
from pyspark.sql.types import StructField, StringType, BooleanType, TimestampType, LongType, StructType, DateType
from repository.repository import UserRepository, MatchRepository
from repository.repository import UserResultRepository
from spark_common import read_data, spark

table_schemas = {}


def load_default_in_memory_database():
    types = {
        'date': DateType(),
        'string': StringType(),
        'bool': BooleanType(),
        'dttm': TimestampType(),
        'long': LongType(),
    }
    for e in ["hero", "user_result", "user", "match"]:
        schema = read_data(e + "_schema")
        schema = [StructField(r.column_name, types[r.type], True) for r in schema.rdd.collect()]
        # schema = [StructField(r.column_name, types[r.type], True) for r in schema.rdd.collect()]
        schema = StructType(schema)
        table_schemas[e] = schema
        empty_rdd = spark.sparkContext.emptyRDD()
        df = spark.createDataFrame(empty_rdd, schema)
        df.createTempView(e)


load_default_in_memory_database()


def append_to_df(table_name: str, data: dict):
    table_df = spark.sql(f"select * from {table_name}")
    schema = table_schemas[table_name]
    new_data = spark.createDataFrame([data], schema)
    table_df.union(new_data).createOrReplaceTempView(table_name)


def get_or_create_df(table_name: str, keys: dict, defaults: dict, id_field: str = None) -> int:
    table_df = spark.sql(f"select * from {table_name}")
    df_core = table_df
    for k, v in keys.items():
        df_core = df_core.filter(f"{k}='{v}'")
    if df_core.count():
        return df_core.select(col(id_field)).head()[0]
    defaults_copy = deepcopy(keys)
    defaults_copy.update(defaults)
    max_id = table_df.select(col(id_field)).agg(max(id_field))
    new_id = (max_id.head()[0] or 0) + 1
    defaults_copy.update({id_field: new_id})
    schema = table_schemas[table_name]
    new_data = spark.createDataFrame([defaults_copy], schema)
    table_df.union(new_data).createOrReplaceTempView(table_name)
    return new_id


class InMemoryUserResultRepository(UserResultRepository):
    def create_user_result(self, result):
        append_to_df('user_result', result)


class InMemoryUserRepository(UserRepository):
    def get_or_create_user_by_name(self, name, defaults=None) -> int:
        if defaults is None:
            defaults = {}
        return get_or_create_df(
            'user',
            {'name': name},
            defaults,
            'user_id'
        )


class InMemoryMatchRepository(MatchRepository):
    def get_or_create_match_by_date(self, finished_at: str, defaults=None) -> int:
        if defaults is None:
            defaults = {}
        return get_or_create_df(
            'match',
            {'finished_at': finished_at},
            defaults,
            'match_id'
        )
