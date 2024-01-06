import logging
from typing import List
import copy
from datetime import timedelta
from functools import reduce

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.types import StringType, LongType
import pyspark.sql.functions as F

import sensorsetl.pipelineconf as pplConf
from sensorsetl.columnfuncs import dydx, d2ydx2, norm
from sensorsetl.utils import checkColumnSchema, alert, dropNaAndUpdateSchema
from sensorsetl.datasink import StatisticsSink, DerivedSink

logger = logging.getLogger(__name__)


def parquetSource(spark: SparkSession, fn) -> DataFrame:
    df = spark.read.parquet(fn)
    df = (df.withColumn("run_uuid", df.run_uuid.astype(LongType()))
          .withColumn("time", F.to_timestamp(df.time)))
    for oldColNames in pplConf.column_name_map:
        if oldColNames in df.columns:
            df = df.withColumnRenamed(oldColNames, pplConf.column_name_map[oldColNames])
    return df


def cleanStringDropNa(spark: SparkSession, df: DataFrame) -> DataFrame:
    for field in df.schema.fields:
        if field.dataType == StringType():
            col = df[field.name]
            df = df = df.withColumn(field.name, F.lower(F.regexp_replace(F.trim(col), " ", "_")))
        fn = field.name.rstrip().strip().lower().replace(" ", "_")
        if fn != field.name:
            df = df.withColumnRenamed(field.name, fn)

    filtered = df.filter(df.robot_id.isin(pplConf.robot_ids))
    required_fields = [f.name for f in pplConf.input_schema if not f.nullable]
    drop_na = dropNaAndUpdateSchema(spark, filtered, required_fields) 
    na_count = df.count() - drop_na.count()
    if na_count > 0:
        msg = f"Dropped {na_count} invalid rows."
        logger.warning(msg)
        alert(msg)
    return drop_na

    
def downSampleAndPivot(df: DataFrame, interval: timedelta,
               fieldList: List[str]) -> DataFrame:
    df = df.filter(df.field.isin(fieldList))
    pivot = df.groupBy([F.window("time", f'{interval.total_seconds()} second')
                 , df.robot_id, df.run_uuid]) \
        .pivot("field", fieldList) \
        .agg(F.last_value('value', ignoreNulls=True).alias("value"))
    if pplConf.DEBUG:
        agg_exprs = [F.avg(reduce(lambda a, b: a + b,
                                  [F.when(pivot[f].isNull(), 1).otherwise(0)
                                   for f in fieldList])).alias('avg_null_field')]
        pivot.groupBy('run_uuid', 'robot_id').agg(*agg_exprs).show()
    pivot = pivot.withColumn("time", pivot.window.start)
    return pivot

def addDerivedFeatures(pivot: DataFrame) -> DataFrame:
    win = Window.partitionBy("run_uuid", "robot_id").orderBy("time")
    # TODO with with NULLs better by using bigger windows
    time_ms = F.unix_millis(pivot.time)
    df = pivot.withColumn('vx', dydx(pivot.x, time_ms, win)) \
        .withColumn('vy', dydx(pivot.y, time_ms, win)) \
        .withColumn('vz', dydx(pivot.z, time_ms, win)) \
        .withColumn('ax', d2ydx2(pivot.x, time_ms, win)) \
        .withColumn('ay', d2ydx2(pivot.y, time_ms, win)) \
        .withColumn('az', d2ydx2(pivot.z, time_ms, win)) \
        .withColumn('f', norm(pivot.fx, pivot.fy, pivot.fz))

    df = df.withColumn('v', norm(df.vx, df.vy, df.vz)) \
        .withColumn('a', norm(df.ax, df.ay, df.az))

    return df

def squashRobotId(derived: DataFrame, fieldList: List[str]) -> DataFrame:
    squash = derived.groupBy('window', 'run_uuid') \
        .pivot('robot_id', pplConf.robot_ids) \
        .agg(
            *[F.last_value(f, ignoreNulls=True).alias(f) for f in fieldList ]
            )
    for f in fieldList:
        for r in pplConf.robot_ids:
            squash = squash.withColumnRenamed('_'.join([str(r), f]), '_'.join([f, str(r)]))
    squash = squash.withColumn("time", squash.window.start)
    return squash

def calcRuntimeStats(derived: DataFrame) -> DataFrame:
    win = Window.partitionBy("run_uuid", "robot_id").orderBy("time")
    # TODO with with NULLs better by using bigger windows
    time_ms = F.unix_millis(pivot.time)
    derived = derived.withColumn('delta_x_norm',
            F.sqrt((F.lag(derived.x).over(win) - derived.x)**2) + 
            F.sqrt((F.lag(derived.y).over(win) - derived.y)**2) +
            F.sqrt((F.lag(derived.z).over(win) - derived.z)**2))
    statsDf = derived.groupBy('run_uuid', 'robot_id').agg(
        F.min('time').alias('start_time'),
        F.max('time').alias('stop_time'),
        (F.max('time') - F.min('time')).alias('total_runtime'),
        F.sum(F.filter('delta_x_norm', F.isnotnull)).alias('total_distance')
    )
    return statsDf

if __name__ == '__main__':
    spark = SparkSession.builder.appName("Sensors ETL Pipeline").getOrCreate()
    spark.sparkContext.setLogLevel(pplConf.spark_log_level)

    source = parquetSource(spark, "sample.parquet")
    checkColumnSchema(source, pplConf.input_schema)
    cleaned = cleanStringDropNa(spark, source)

    if cleaned.count() == 0:
        logger.warning("No valid rows in input dataset.")
        exit(0)

    if pplConf.DEBUG:
        cleaned.groupBy("run_uuid", "robot_id").pivot('field', pplConf.pivot_field_list).count().fillna(0).show()
    pivot = downSampleAndPivot(cleaned, pplConf.downsample_interval, pplConf.pivot_field_list)
    derived = addDerivedFeatures(pivot) 

    statsDf = calcRuntimeStats(derived)
    squashDf = squashRobotId(derived, pplConf.pivot_field_list +
                             ['vx', 'vy', 'vz', 'ax', 'ay', 'az', 'f', 'v', 'a']) \
                             .drop('window')
    newSquashSchema = copy.deepcopy(squashDf.schema)
    newSquashSchema['time'].nullable = False
    squashDf = spark.createDataFrame(squashDf.rdd, newSquashSchema)
    statsDf.printSchema()
    statsDf.show(4)
    squashDf.printSchema()
    squashDf.show(4)

    derivedSink = DerivedSink(spark).write(squashDf)
    statisticsSink = StatisticsSink(spark).write(statsDf)