from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from datetime import timedelta
from typing import List
import logging

logger = logging.getLogger(__name__)    

def findTimeIntervals(spark: SparkSession, df: DataFrame, fieldList: List[str]) -> timedelta:
    df = df.filter(df.field.isin(fieldList))
    window = Window.partitionBy("run_uuid", "field", "robot_id").orderBy("time")
    df = df.withColumn("dt", (df.time - F.lag(df.time).over(window)))
    df = df.groupBy("run_uuid", "field", "robot_id").agg(F.approx_percentile("dt", 0.9).alias('interval'))
    max_interval = df.agg(F.max('interval')).collect()[0][0]
    return max_interval