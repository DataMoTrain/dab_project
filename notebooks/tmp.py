from databricks.connect import DatabricksSession
# from pyspark.sql import SparkSession --> not to be confused with a databricks session

spark = DatabricksSession.builder.getOrCreate()
# alternatively, if you want to provide the cluster-id in the script instead of using the databricks profile config
# (adding cluster_id = 0316-134722-polbcbke as a line under the DEFAULT profile after host and token)
#spark = DatabricksSession.builder.remote(cluster_id="0316-134722-polbcbke").getOrCreate()
spark.sql("SELECT 1").show()