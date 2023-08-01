


import pyspark
import pyspark.sql.functions
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('sparkdf').enableHiveSupport().getOrCreate()
df=spark.sql("select * from forex.eurusd")
df1=df.withColumn("time", when(col("time").isNull(), 'N/A').otherwise(col("time")))\
      .withColumn("open", when(col("open").isNull(), 'N/A').otherwise(col("open")))\
      .withColumn("high", when(col("high").isNull(), 'N/A').otherwise(col("high")))\
      .withColumn("low", when(col("low").isNull(), 'N/A').otherwise(col("low")))\
      .withColumn("close", when(col("close").isNull(), 'N/A').otherwise(col("close")))
df1.count()
df1.write.parquet("/user/bigdatacloudxlab27228/hdfs_forex_curated")
