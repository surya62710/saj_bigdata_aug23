//df.1.Create a dataframe on the sales data
//2.find  out the count of sale in the particular year of each categeory
//3.write the output into the parquet file.
========================================================
import pyspark
import pyspark.sql.functions
# importing sparksession from pyspark.sql module
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
# creating sparksession and giving an app name
spark = SparkSession.builder.appName('sparkdf').getOrCreate()
df = spark.read.option("header", "true").csv("/user/bigdatacloudxlab14968/Iowa_liquor_sales/Iowa_Liquor_Sales.csv")
df1=df.withColumnRenamed("Invoice/Item Number","Invoice_Item_Number")\
      .withColumnRenamed("Bottle Volume (ml)","Bottle_Volume_ml")\
      .withColumnRenamed("Sale (Dollars)","Sale_Dollars")\
      .withColumnRenamed("Volume Sold (Liters)","Volume_Sold_Liters")\
      .withColumnRenamed("Volume Sold (Gallons)","Volume_Sale_Gallons")\
      .withColumnRenamed("Category Name","Category_Name")
df1.printSchema()
df2 =df1.withColumn('year_sale', df.Date.substr(7, 4))
df2.printSchema()
df3=df2.groupBy("date","year_sale","Category_Name","Sale_Dollars").count()
df4=df3.filter("year_sale==2015")
df4.show(20)
print(df4.count())
print(df.count()) 
df4.write.parquet("/user/bigdatacloudxlab39242/adithya/Iowa_liquor_sales/sales")

==========================================================
out put
===========
root
 |-- Invoice_Item_Number: string (nullable = true)
 |-- Date: string (nullable = true)
 |-- Store Number: string (nullable = true)
 |-- Store Name: string (nullable = true)
 |-- Address: string (nullable = true)
 |-- City: string (nullable = true)
 |-- Zip Code: string (nullable = true)
 |-- Store Location: string (nullable = true)
 |-- County Number: string (nullable = true)
 |-- County: string (nullable = true)
 |-- Category: string (nullable = true)
 |-- Category_Name: string (nullable = true)
 |-- Vendor Number: string (nullable = true)
 |-- Vendor Name: string (nullable = true)
 |-- Item Number: string (nullable = true)
 |-- Item Description: string (nullable = true)
 |-- Pack: string (nullable = true)
 |-- Bottle_Volume_ml: string (nullable = true)
 |-- State Bottle Cost: string (nullable = true)
 |-- State Bottle Retail: string (nullable = true)
 |-- Bottles Sold: string (nullable = true)
 |-- Sale_Dollars: string (nullable = true)
 |-- Volume_Sold_Liters: string (nullable = true)
 |-- Volume_Sale_Gallons: string (nullable = true)
root
 |-- Invoice_Item_Number: string (nullable = true)
|-- Date: string (nullable = true)
 |-- Store Number: string (nullable = true)
 |-- Store Name: string (nullable = true)
 |-- Address: string (nullable = true)
 |-- City: string (nullable = true)
 |-- Zip Code: string (nullable = true)
 |-- Store Location: string (nullable = true)
 |-- County Number: string (nullable = true)
 |-- County: string (nullable = true)
 |-- Category: string (nullable = true)
 |-- Category_Name: string (nullable = true)
 |-- Vendor Number: string (nullable = true)
 |-- Vendor Name: string (nullable = true)
 |-- Item Number: string (nullable = true)
 |-- Item Description: string (nullable = true)
 |-- Pack: string (nullable = true)
 |-- Bottle_Volume_ml: string (nullable = true)
 |-- State Bottle Cost: string (nullable = true)
 |-- State Bottle Retail: string (nullable = true)
 |-- Bottles Sold: string (nullable = true)
 |-- Sale_Dollars: string (nullable = true)
 |-- Volume_Sold_Liters: string (nullable = true)
 |-- Volume_Sale_Gallons: string (nullable = true)
 |-- year_sale: string (nullable = true)

+----------+---------+--------------------+------------+-----+
|      date|year_sale|       Category_Name|Sale_Dollars|count|
+----------+---------+--------------------+------------+-----+
|09/28/2015|     2015|STRAIGHT BOURBON ...|      134.76|    1|
|10/20/2015|     2015|PUERTO RICO & VIR...|       20.25|    4|
|08/12/2015|     2015|STRAIGHT BOURBON ...|       94.44|   16|
|09/03/2015|     2015|             TEQUILA|      556.56|    1|
|09/15/2015|     2015|      IMPORTED VODKA|      132.78|   12|
|01/21/2015|     2015|             TEQUILA|       81.00|    5|
|05/14/2015|     2015|IMPORTED VODKA - ...|       85.50|    4|
|05/06/2015|     2015|   AMERICAN DRY GINS|       21.24|    2|
|06/01/2015|     2015|        FLAVORED RUM|      123.00|   10|
|09/28/2015|     2015|      VODKA 80 PROOF|       64.56|   48|
|02/23/2015|     2015|PUERTO RICO & VIR...|       70.56|   42|
|07/28/2015|     2015|   CANADIAN WHISKIES|      147.84|    1|
|06/15/2015|     2015|          SPICED RUM|      162.00|   83|
|08/13/2015|     2015|      VODKA 80 PROOF|       75.12|   12|
|05/05/2015|     2015|STRAIGHT RYE WHIS...|       57.52|    1|
|02/11/2015|     2015|    BLENDED WHISKIES|       62.28|   21|
|05/21/2015|     2015|      IMPORTED VODKA|      179.94|   18|
|09/01/2015|     2015|    BLENDED WHISKIES|       15.70|    1|
|03/23/2015|     2015|     SCOTCH WHISKIES|      227.28|    2|
|10/22/2015|     2015|DECANTERS & SPECI...|      233.82|    1|
+----------+---------+--------------------+------------+-----+
only showing top 20 rows


================================================================
+---------+------------+--------------------+
|year_sale|Sale_Dollars|       Category_Name|
+---------+------------+--------------------+
|     2015|       14.68|      VODKA FLAVORED|
|     2015|        8.31|PUERTO RICO & VIR...|
|     2015|        7.44|   IMPORTED SCHNAPPS|
|     2015|      145.92|      VODKA 80 PROOF|
|     2015|        7.35|     WHISKEY LIQUEUR|
|     2015|       81.00|      VODKA 80 PROOF|
|     2015|      486.00|             TEQUILA|
|     2015|      182.28|             TEQUILA|
|     2015|       33.09|   CANADIAN WHISKIES|
|     2015|       73.80|  AMERICAN COCKTAILS|
|     2015|       30.00|          SPICED RUM|
|     2015|     2174.40|  AMERICAN COCKTAILS|
|     2015|       66.12|STRAIGHT BOURBON ...|
|     2015|      162.00|          SPICED RUM|
|     2015|       16.50|          SPICED RUM|
|     2015|       30.00|      IMPORTED VODKA|
|     2015|       58.28|   CANADIAN WHISKIES|
|     2015|       17.10| PEPPERMINT SCHNAPPS|
|     2015|      177.36|     WHISKEY LIQUEUR|
|     2015|      149.98|  SINGLE MALT SCOTCH|
+---------+------------+--------------------+

+--------------------+------+
|       Category_Name| count|
+--------------------+------+
|   IMPORTED SCHNAPPS| 30063|
|      PEACH BRANDIES|  4504|
|    AMERICAN ALCOHOL|  4269|
|IMPORTED VODKA - ...| 45181|
|      VODKA 80 PROOF|247131|
|  RASPBERRY SCHNAPPS|  2309|
|BUTTERSCOTCH SCHN...|  6098|
| PEPPERMINT SCHNAPPS| 20452|
|   CANADIAN WHISKIES|187748|
|  AMERICAN COCKTAILS| 48366|
|MISCELLANEOUS SCH...|  6262|
|    APRICOT BRANDIES|  7400|
|     CREME DE ALMOND|   286|
|   CINNAMON SCHNAPPS|  5200|
|   AMERICAN AMARETTO|  9512|
|             TEQUILA| 85511|
|       FLAVORED GINS|  1845|
|MISC. AMERICAN CO...| 25910|
|DISTILLED SPIRITS...| 10305|
|                null|  1182|
+--------------------+------+
====================================================
import pyspark
import pyspark.sql.functions
# importing sparksession from pyspark.sql module
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import *
# creating sparksession and giving an app name
spark = SparkSession.builder.appName('sparkdf').getOrCreate()
df = spark.read.option("header", "true").csv("/user/bigdatacloudxlab14968/Iowa_liquor_sales/Iowa_Liquor_Sales.csv")
df1=df.withColumnRenamed("Invoice/Item Number","Invoice_Item_Number")\
      .withColumnRenamed("Bottle Volume (ml)","Bottle_Volume_ml")\
      .withColumnRenamed("Sale (Dollars)","Sale_Dollars")\
      .withColumnRenamed("Volume Sold (Liters)","Volume_Sold_Liters")\
      .withColumnRenamed("Volume Sold (Gallons)","Volume_Sale_Gallons")\
      .withColumnRenamed("Category Name","Category_Name")\
      .withColumn("Sale_Dollars","Sale_Dollars".cast(IntegerType))
df1.printSchema()
df2 =df1.withColumn('year_sale', df.Date.substr(7, 4))
df2.printSchema()
df3=df2.filter("year_sale==2015")
df3.select("year_sale","Sale_Dollars","Category_Name").show(20)
df4=df3.groupBy("Category_Name").agg(sum("Sale_Dollars"))
df4.show(20)
print(df4.count())
print(df.count())
df4.write.parquet("/user/bigdatacloudxlab39242/adithya/Iowa_liquor_sales/sales")


====================================================
1.for every year most sold item
2.each year total sales by sotre
3.avg sales of the all stores
4top 10 & bottom store which soled
5.slaes by each state for each year
6.difference in sales by year
