from .helpers import PySparkTestCase

from pyspark.sql.types import StructType

from sensorsetl.utils import checkColumnSchema, dropNaAndUpdateSchema

class TestUtils(PySparkTestCase):
    def setUp(self):
        self.df = self.spark.createDataFrame(
            data=[(1, 2), (3, None)],
            schema=StructType().add("field1", "integer").add("field2", "integer", nullable=True))

    def testCheckColumnSchemaMissing(self):
        schema = StructType().add("field3", "integer")
        self.assertRaisesRegex(ValueError, "missing.*field3", checkColumnSchema, self.df, schema)
    
    def testCheckColumnSchemaTypeMismatch(self):
        schema = StructType().add("field1", "integer").add("field2", "string")
        self.assertRaisesRegex(ValueError, "type.*field2", checkColumnSchema, self.df, schema)

    def testCheckColumnSchemaNullableMismatch(self):
        schema = StructType().add("field1", "integer").add("field2", "integer", nullable=False)
        self.assertRaisesRegex(ValueError, "nullable.*field2", checkColumnSchema, self.df, schema, True)

    def testCheckColumnSchemaPass(self):
        schema = StructType().add("field1", "integer").add("field2", "integer", nullable=True)
        checkColumnSchema(self.df, schema, nullable_check=True)

    def TestDropNaAndUpdateSchema(self):
        ans = dropNaAndUpdateSchema(self.spark, self.df, ["field2"])
        self.assertEqual(ans.count(), 1)
        self.assertEqual(ans.schema["field2"].nullable, False)