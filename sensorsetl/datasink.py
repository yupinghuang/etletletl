from pyspark.sql import SparkSession, DataFrame

# TODO: Seperate query and write. 

"""
Classes that implement the sink interfaces for the ETL pipeline.
"""
class StatisticsSink:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.parquet_path = '/home/jovyan/statistics.parquet'
        self.db_view_name = 'statistics'
        print('Table name for StatisticsSink: ', self.db_view_name)

    def write(self, statistics: DataFrame):
        statistics.write.mode('append').partitionBy(['run_uuid']).save(self.parquet_path)

    def query(self, sql: str, mergeSchema: bool = False):
        self.spark.read.option("mergeSchema", mergeSchema).load(self.parquet_path).createOrReplaceTempView(self.db_view_name)
        return self.spark.sql(sql)


class DerivedSink:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.parquet_path = '/home/jovyan/derived.parquet'
        self.db_view_name = 'derived'
        print('Table name for DerivedSink: ', self.db_view_name)

    def write(self, derived: DataFrame):
        derived.write.mode('append').partitionBy(['run_uuid']).save(self.parquet_path)

    def query(self, sql: str, mergeSchema: bool = False):
        self.spark.read.option("mergeSchema", mergeSchema).load(self.parquet_path).createOrReplaceTempView(self.db_view_name)
        return self.spark.sql(sql)
