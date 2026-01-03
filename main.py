import pyspark 
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MySparkApp").getOrCreate()

spark.range(10).explain()
spark.range(10).show()

