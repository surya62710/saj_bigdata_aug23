1.Create an rdd on saj_data_file.txt
2.Read the above created Rdd
3.Partition the rdd using filters 
4.save the partition rdd in another txt file


==================================================================
import pyspark
# importing sparksession from pyspark.sql module
from pyspark.sql import SparkSession
# creating sparksession and giving an app name
spark = SparkSession.builder.appName('sparkdf').getOrCreate()
myrdd=spark.sparkContext.textFile("/user/bigdatacloudxlab39242/adithya/saj_data_file.txt")
print(type(myrdd))
print("No.of elemnts:",myrdd.count())
print("No.of partiotions:",myrdd.getNumPartitions())
rdd=myrdd.filter(lambda x: x[1] > 0)
rdd2 = rdd.repartition(10)
print("No.of second partitions=",rdd2.getNumPartitions())
rdd2.saveAsTextFile("/user/bigdatacloudxlab39242/adithya/surya_partition_data_file.txt")