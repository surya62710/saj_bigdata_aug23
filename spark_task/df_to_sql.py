pyspark --driver-class-path /usr/share/java/mysql-connector-java.jar
from pyspark import SparkConf, SparkContext
>>> from pyspark.sql import SQLContext
>>> conf = SparkConf().setAppName("ImportData")
>>> sc = SparkContext.getOrCreate(conf=conf)
>>> sqlContext = SQLContext(sc)
>>> url = "jdbc:mysql://cxln2:3306/retail_db"
>>> table = "bike_sharing"
>>> user = "sqoopuser"
>>> password = "NHkkP876rp"
>>> df = spark.read.csv("/user/bigdatacloudxlab14968/saj_3_14_2023/Divvy_Trips_2019_Q1.csv")
23/03/20 16:40:09 WARN DataSource: Error while looking for metadata directory.
>>> jdbc_properties = {
     "user": user,
    "password": password,
     "driver": "com.mysql.jdbc.Driver"
 }
>>> df.write.jdbc(url=url, table=table, mode='overwrite', properties=jdbc_properties)


df.select(concat(col("date"),lit(','),col("time")).as("datetime")).show(false)

  df.withColumn("FullName",concat(col("date"),lit(','),col("time"))).show(false)
