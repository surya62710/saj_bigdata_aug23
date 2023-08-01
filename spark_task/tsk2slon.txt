import pyspark
import pyspark.sql.functions
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('sparkdf').getOrCreate()
pyspark --jars /data/spark/mysql-connector-java-5.1.36-bin.jar

df = sqlContext.read.jdbc(url=jdbc:mysql://cxln2:3306/sqoopex,dbtable=saj_3_14_2023,user=sqoopuser,password=NHkkP876rp,driver=com.mysql.jdbc.Driver") \
    .option("user", "sqoopuser") \
    .option("password", "NHkkP876rp") \
    .option("driver", "com.mysql.jdbc.Driver") \
    .load()


$ pyspark --driver -class -path /data/spark/mysql-connector-java-5.1.36-bin.jar



from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

conf = SparkConf().setAppName("ImportData")
sc = SparkContext.getOrCreate(conf=conf)
sqlContext = SQLContext(sc)

url = "jdbc:mysql://cxln2:3306/sqoopex"
table = "saj_3_14_2023"
user = "sqoopuser"
password = "NHkkP876rp"
target_dir = "/user/bigdatacloudxlab14968/saj_3_14_2023"

jdbc_properties = {
    "user": user,
    "password": password,
    "driver": "com.mysql.jdbc.Driver"
}

# Read data from MySQL database
df = sqlContext.read.jdbc(url=url, table=table, properties=jdbc_properties)

# Write data to HDFS in Parquet format
df.write.parquet(target_dir)

part-00000-52de5bdb-7ef8-4aa5-852c-bf3ae0c792d6.snappy.parquet