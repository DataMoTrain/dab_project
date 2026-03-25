import os
import sys
import pytest

sys.path.append(os.getcwd())

# default scope of a fixture: function level
@pytest.fixture()
def spark():
    try:
        from databricks.connect import  DatabricksSession
        spark = DatabricksSession.builder.getOrCreate()
    except ImportError:
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()
        except:
            raise ImportError("Neither Databricks or local Spark Session could be imported.")
    return spark