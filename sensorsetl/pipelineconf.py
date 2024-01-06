import datetime
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, LongType, DoubleType


input_schema = StructType([
    StructField("time", TimestampType(), nullable=False),
    StructField("run_uuid", LongType(), nullable=False),
    StructField("field", StringType(), nullable=False),
    StructField("robot_id", LongType(), nullable=False),
    StructField("value", DoubleType(), nullable=False),
    StructField("sensor_type", StringType(), nullable=True)
])


# In case column names in the input schema change
column_name_map = {'sensorType': 'sensor_type'}

pivot_field_list = ['x', 'y', 'z', 'fx', 'fy', 'fz']

robot_ids = [1, 2]


"""
Pipeline run parameters
"""
DEBUG = False

spark_log_level = 'WARN'
downsample_interval = datetime.timedelta(seconds=0.01)


# Output
statistics_sink_path = '/home/jovyan/statistics.parquet'
derived_sink_path = '/home/jovyan/derived.parquet'