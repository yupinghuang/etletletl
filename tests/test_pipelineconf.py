import unittest
from pyspark.sql.types import StructType

class TestPipelineConf(unittest.TestCase):
    def testImport(self):
        import sensorsetl.pipelineconf as pplConf
        self.assertIsNotNone(pplConf.__file__)

    def testInputSchemaExists(self):
        import sensorsetl.pipelineconf as pplConf
        self.assertIsInstance(pplConf.input_schema, StructType)
