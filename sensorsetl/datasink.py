from pyspark.sql import SparkSession, DataFrame

# TODO: Seperate query and write. 

class StatisticsSink:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.parquet_path = '/home/jovyan/statistics.parquet'
        self.spark.read.load('/home/jovyan/derived.parquet').createOrReplaceTempView("statistics")

    def write(self, statistics: DataFrame):
        statistics.write.mode('append').partitionBy(['run_uuid']).save(self.parquet_path)

    def query(self, sql: str):
        self.spark.read.load(self.parquet_path).createOrReplaceTempView("statistics")
        return self.spark.sql(sql)


class DerivedSink:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.parquet_path = '/home/jovyan/derived.parquet'

    def write(self, derived: DataFrame):
        derived.write.mode('append').partitionBy(['run_uuid']).save(self.parquet_path)

    def query(self, sql: str):
        self.spark.read.load(self.parquet_path).createOrReplaceTempView("derived")
        return self.spark.sql(sql)
