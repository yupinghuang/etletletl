from tests.helpers import PySparkTestCase

from sensors_etl_pipeline import downSampleAndPivot
from datetime import datetime, timedelta

from pyspark.sql.types import StructType, StructField, TimestampType, StringType, LongType, DoubleType

class TestUtils(PySparkTestCase):
    def setUp(self):
        self.cadence = timedelta(seconds=5)
        self.fieldList = ['x', 'y', 'fx']
        self.nuuid = 3
        self.nrobots = 2

        data = []
        basetime = datetime(2000, 1, 1, 0, 0, 0)
        offset = timedelta(seconds=1)
        fields = ['x', 'y']
        data.append([basetime, 0, 'fx', 1, 0.1, ''])
        for f in fields:
            basetime += offset
            for i in range(self.nuuid):
                t =  basetime + self.cadence * i
                for r in range(self.nrobots):
                    data.append([t, i, f, r, 0.5, ''])
        self.df = self.spark.createDataFrame(
            data=data,
            schema=StructType([
    StructField("time", TimestampType(), nullable=False),
    StructField("run_uuid", LongType(), nullable=False),
    StructField("field", StringType(), nullable=False),
    StructField("robot_id", LongType(), nullable=False),
    StructField("value", DoubleType(), nullable=False),
    StructField("sensor_type", StringType(), nullable=True)
]))

    def testDownSampleAndPivot(self):
        df = downSampleAndPivot(self.df, self.cadence, self.fieldList).drop('window')
        self.assertEqual(df.count(), 6)
        self.assertSetEqual(set(df.columns), set(['time', 'robot_id', 'run_uuid', 'x', 'y', 'fx']))