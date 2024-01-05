import logging
import copy

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)

def checkColumnSchema(df: DataFrame, required_schema: StructType, nullable_check=False) -> None:
    """
    Enforce column schema on dataframe: check for missing column and correct datatype
    """
    missing_fields = [field.name for field in required_schema if 
                      (field.name not in df.schema.fieldNames())]
    if len(missing_fields) > 0:
        raise ValueError(f"Data frame has missing coulmns: {missing_fields}.")

    type_mismatch_fields = [field.name for field in required_schema if df.schema[field.name].dataType != field.dataType]
    if len(type_mismatch_fields) > 0:
        raise ValueError(f"Columns have incorrect data type: {type_mismatch_fields}")
    if nullable_check:
        nullable_mistach = [field.name for field in required_schema if
                            not (field.nullable) and df.schema[field.name].nullable]
        if len(nullable_mistach) > 0:
            raise ValueError(f"Columns should not be nullable: {nullable_mistach}")

def dropNaAndUpdateSchema(spark, filtered, required_fields):
    drop_na = filtered.dropna(subset=required_fields)
    new_schema = copy.deepcopy(drop_na.schema)

    for field in new_schema.fields:
        if field.name in required_fields:
            field.nullable = False

    drop_na = spark.createDataFrame(drop_na.rdd, new_schema)
    logger.debug(f"Updated schema: {drop_na.schema}")
    return drop_na

def alert(*args):
    """
    Stub for sending an alert message for human attention.
    """
    pass