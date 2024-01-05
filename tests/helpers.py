from pyspark.sql import SparkSession

import unittest

class PySparkTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("unit test").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()