import findspark
findspark.init()
import datetime  
import pyspark.sql.functions as F

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("finder").getOrCreate()


rdd = spark.sparkContext.parallelize([
 (1, 2., "string1", datetime.date(2000, 1, 1), datetime.datetime(2000, 1, 1, 12, 0)),
 (2, 3., "string2", datetime.date(2000, 2, 1), datetime.datetime(2000, 1, 2, 12, 0)),
 (3, 4., "string3", datetime.date(2000, 3, 1), datetime.datetime(2000, 1, 3, 12, 0))
])
df = spark.createDataFrame(rdd, schema=["a", "b", "c", "d", "e"])
df.show()