import pyspark
from pyspark.sql import SparkSession

# Create a Spark session (Control center of Spark functionality)
spark = SparkSession.builder.appName("MySparkApp").getOrCreate()

spark.range(10).explain()
spark.range(10).show()

df = spark.read.csv(
    "201508_trip_data.csv",
    header=True,
    inferSchema=True
)

df.show(5)


print(spark.conf.get("spark.app.name"))
print(spark.conf.get("spark.sql.shuffle.partitions"))
print(spark.conf.get("spark.sql.files.maxPartitionBytes"))
print(spark.version)

# Stop the Spark session - good practice to free up resources on low-RAM machines
spark.stop()
