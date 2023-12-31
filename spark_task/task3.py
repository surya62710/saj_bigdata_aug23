/*1.for every year most sold item
2.each year total sales by sotre
3.avg sales of the all stores
4top 10 & bottom 10 store which soled
5.slaes by each state for each year
6.difference in sales by year
===================================*/
/*root
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
 |-- Sale_Dollars: integer (nullable = true)
 |-- Volume_Sold_Liters: string (nullable = true)
 |-- Volume_Sale_Gallons: string (nullable = true)
 |-- year_sale: string (nullable = true)
=================================================================
Least selling product in each year*/

import pyspark
import pyspark.sql.functions
# importing sparksession from pyspark.sql module
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import*
# creating sparksession and giving an app name
spark = SparkSession.builder.appName('sparkdf').getOrCreate()
df = spark.read.option("header", "true").csv("/user/bigdatacloudxlab14968/Iowa_liquor_sales/Iowa_Liquor_Sales.csv")
df1=df.withColumnRenamed("Invoice/Item Number","Invoice_Item_Number")\
      .withColumnRenamed("Bottle Volume (ml)","Bottle_Volume_ml")\
      .withColumnRenamed("Sale (Dollars)","Sale_Dollars")\
      .withColumnRenamed("Volume Sold (Liters)","Volume_Sold_Liters")\
      .withColumnRenamed("Volume Sold (Gallons)","Volume_Sale_Gallons")\
      .withColumnRenamed("Category Name","Category_Name")\
      .withColumn("Sale_Dollars",col("Sale_Dollars").cast(IntegerType()))
df1.printSchema()
df2 =df1.withColumn('year_sale', df.Date.substr(7, 4))
df2.printSchema()
df3=df2.groupBy("year_sale","Category_Name").sum("Sale_Dollars")
df3.show()
w2 = Window.partitionBy("year_sale").orderBy(col("sum(Sale_Dollars)"))
df4=df3.withColumn("row",row_number().over(w2)) \
  .filter(col("row") == 1).drop("row")
df4.show()
/*+---------+--------------------+-----------------+
|year_sale|       Category_Name|sum(Sale_Dollars)|
+---------+--------------------+-----------------+
|     2016|              Mezcal|               19|
|     2012|IMPORTED VODKA - ...|              252|
|     2020|     Coffee Liqueurs|               37|
|     2019|     Cocktails / RTD|              312|
|     2017|      American Vodka|                0|
|     2014|IMPORTED VODKA - ...|             1327|
|     2013| SCHNAPPS - IMPORTED|              123|
|     2018|American Distille...|               26|
|     2015|IMPORTED VODKA - ...|              126|
+---------+--------------------+-----------------+
========================================================================*/
import pyspark
import pyspark.sql.functions
# importing sparksession from pyspark.sql module
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import*
# creating sparksession and giving an app name
spark = SparkSession.builder.appName('sparkdf').getOrCreate()
df = spark.read.option("header", "true").csv("/user/bigdatacloudxlab14968/Iowa_liquor_sales/Iowa_Liquor_Sales.csv")
df1=df.withColumnRenamed("Invoice/Item Number","Invoice_Item_Number")\
      .withColumnRenamed("Bottle Volume (ml)","Bottle_Volume_ml")\
      .withColumnRenamed("Sale (Dollars)","Sale_Dollars")\
      .withColumnRenamed("Volume Sold (Liters)","Volume_Sold_Liters")\
      .withColumnRenamed("Volume Sold (Gallons)","Volume_Sale_Gallons")\
      .withColumnRenamed("Category Name","Category_Name")\
      .withColumn("Sale_Dollars",col("Sale_Dollars").cast(IntegerType()))
df1.printSchema()
df2 =df1.withColumn('year_sale', df.Date.substr(7, 4))
df2.printSchema()
df3=df2.groupBy("year_sale","Category_Name").sum("Sale_Dollars")
df3.show()
w2 = Window.partitionBy("year_sale").orderBy(col("sum(Sale_Dollars)").desc())
df4=df3.withColumn("row",row_number().over(w2)) \
  .filter(col("row") == 1).drop("row")
df4.show()
+---------+------------------+-----------------+
|year_sale|     Category_Name|sum(Sale_Dollars)|
+---------+------------------+-----------------+
|     2016| CANADIAN WHISKIES|          7857578|
|     2012| CANADIAN WHISKIES|         24762102|
|     2020|100% Agave Tequila|             5266|
|     2019|   American Vodkas|         24483442|
|     2017|   American Vodkas|           371037|
|     2014| CANADIAN WHISKIES|         27798044|
|     2013| CANADIAN WHISKIES|         26229269|
|     2018|   American Vodkas|           339165|
|     2015| CANADIAN WHISKIES|         30869067|
+---------+------------------+-----------------+

=====================================================================
2.each year total sales by sotre

import pyspark
import pyspark.sql.functions
# importing sparksession from pyspark.sql module
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import*
# creating sparksession and giving an app name
spark = SparkSession.builder.appName('sparkdf').getOrCreate()
df = spark.read.option("header", "true").csv("/user/bigdatacloudxlab14968/Iowa_liquor_sales/Iowa_Liquor_Sales.csv")
df1=df.withColumnRenamed("Invoice/Item Number","Invoice_Item_Number")\
      .withColumnRenamed("Bottle Volume (ml)","Bottle_Volume_ml")\
      .withColumnRenamed("Sale (Dollars)","Sale_Dollars")\
      .withColumnRenamed("Volume Sold (Liters)","Volume_Sold_Liters")\
      .withColumnRenamed("Volume Sold (Gallons)","Volume_Sale_Gallons")\
      .withColumnRenamed("Category Name","Category_Name")\
      .withColumnRenamed("Store Name","Store_Name")\
      .withColumn("Sale_Dollars",col("Sale_Dollars").cast(IntegerType()))
df1.printSchema()
df2 =df1.withColumn('year_sale', df.Date.substr(7, 4))
df2.printSchema()
df3=df2.groupBy("year_sale","Category_Name").sum("Sale_Dollars")
df3.show()
w2 = Window.partitionBy("year_sale").orderBy(col("sum(Sale_Dollars)").desc())
df4=df3.withColumn("row",row_number().over(w2)) \
  .filter(col("row") == 1).drop("row")
df4.show()
df5=df2.groupBy("year_sale","Store_Name").sum("Sale_Dollars")
df5.show()
print(df5.count())
df5.write.csv("/user/bigdatacloudxlab39242/adithya/Iowa_liquor_sales/sales_store")
count of:total stores in each year ==10298
+---------+--------------------+-----------------+
|year_sale|          Store_Name|sum(Sale_Dollars)|
+---------+--------------------+-----------------+
|     2012|Hy-Vee Food Store...|           152551|
|     2015|Hy-Vee Food Store...|          1224673|
|     2015|Walgreens #05852 ...|            90241|
|     2015|Wal-Mart 1285 / O...|           202675|
|     2014|Fareway Stores #8...|           157116|
|     2014|Casey's General S...|            28378|
|     2012|Hartley Wine And ...|            51575|
|     2012|    Hy-Vee / Waverly|           664504|
|     2012|Hartig Drug #14 /...|           173883|
|     2014|Sioux Food Center...|            64976|
|     2014|CVS Pharmacy #854...|            77941|
|     2015|  Xo Food And Liquor|           127732|
|     2013|Fareway Stores #9...|            75428|
|     2015|      Lickety Liquor|           269814|
|     2012|  Hy-Vee / Urbandale|           446841|
|     2014|Brothers Market, ...|            94034|
|     2013| The Hangout Liquors|            22863|
|     2013| Old Calliope Liquor|            23691|
|     2015|Casey's General S...|            66477|
|     2015|Vom Fass / Des Mo...|            81542|
+---------+--------------------+-----------------+
only showing top 20 rows

==========================================================
import pyspark
import pyspark.sql.functions
# importing sparksession from pyspark.sql module
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import*
# creating sparksession and giving an app name
spark = SparkSession.builder.appName('sparkdf').getOrCreate()
df = spark.read.option("header", "true").csv("/user/bigdatacloudxlab14968/Iowa_liquor_sales/Iowa_Liquor_Sales.csv")
df1=df.withColumnRenamed("Invoice/Item Number","Invoice_Item_Number")\
      .withColumnRenamed("Bottle Volume (ml)","Bottle_Volume_ml")\
      .withColumnRenamed("Sale (Dollars)","Sale_Dollars")\
      .withColumnRenamed("Volume Sold (Liters)","Volume_Sold_Liters")\
      .withColumnRenamed("Volume Sold (Gallons)","Volume_Sale_Gallons")\
      .withColumnRenamed("Category Name","Category_Name")\
      .withColumnRenamed("Store Name","Store_Name")\
      .withColumn("Sale_Dollars",col("Sale_Dollars").cast(IntegerType()))
df1.printSchema()
df2 =df1.withColumn('year_sale', df.Date.substr(7, 4))
df2.printSchema()
df3=df2.groupBy("year_sale","Category_Name").sum("Sale_Dollars")
df3.show()
w2 = Window.partitionBy("year_sale").orderBy(col("sum(Sale_Dollars)").desc())
df4=df3.withColumn("row",row_number().over(w2)) \
  .filter(col("row") == 1).drop("row")
df4.show()
df5=df2.groupBy("year_sale","Store_Name").sum("Sale_Dollars")
df5.show()
print(df5.count())
df6=df2.groupBy("Store_Name").mean("Sale_Dollars")
df6.show()
print(df6.count())
==============================================================

Total Count of stores:2361

+--------------------+------------------+
|          Store_Name| avg(Sale_Dollars)|
+--------------------+------------------+
|Buchanan County L...|40.975217989903626|
|    Guppys On The Go|104.66617574864998|
|Hartig Drug Co. /...| 39.40950533462657|
|South Side Food Mart|133.57656500802568|
|          Prime Mart|  83.3485294117647|
|Hy-Vee #5 / Daven...|141.28005652448775|
|Casey's General S...|108.20439189189189|
|Iowa City Fast Break|49.187288708586884|
|Shugar's Super Va...|55.343161037506995|
|Casey's General S...|  58.4519309778143|
|Kum & Go #535 / D...| 62.91329479768786|
|Casey's General S...| 75.22619047619048|
|Select Mart / Sio...| 96.52810077519379|
|Bormanns Neighbor...|           125.464|
|        Prime Mart 7|             863.0|
|Southgate Express...|             153.0|
|Sam's Club 8238 /...| 669.1090686274509|
|Kum & Go #96 / WE...|  76.1557734204793|
|Kum & Go #170 / U...| 82.15325342465754|
|Fast Trip / Des M...|29.010427528675702|
+--------------------+------------------+
only showing top 20 rows


=========================================================
Most Sold Store in each year
+---------+--------------------+-----------------+
|year_sale|          Store_Name|sum(Sale_Dollars)|
+---------+--------------------+-----------------+
|     2016|Hy-Vee #3 / BDI /...|          2174890|
|     2012|Hy-Vee #3 / BDI /...|          7631386|
|     2020|Hy-Vee Food Store...|             3329|
|     2019|      Central City 2|          5670158|
|     2017|Sam's Club 6344 /...|           539605|
|     2014|Hy-Vee #3 / BDI /...|          7914163|
|     2013|Hy-Vee #3 / BDI /...|          7103756|
|     2018|Hy-Vee Food Store...|            87533|
|     2015|Hy-Vee #3 / BDI /...|          8451040|
+---------+--------------------+-----------------+
=============================================================
Top 10 stores in every year
============================
import pyspark
import pyspark.sql.functions
# importing sparksession from pyspark.sql module
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import*
# creating sparksession and giving an app name
spark = SparkSession.builder.appName('sparkdf').getOrCreate()
df = spark.read.option("header", "true").csv("/user/bigdatacloudxlab14968/Iowa_liquor_sales/Iowa_Liquor_Sales.csv")
df1=df.withColumnRenamed("Invoice/Item Number","Invoice_Item_Number")\
      .withColumnRenamed("Bottle Volume (ml)","Bottle_Volume_ml")\
      .withColumnRenamed("Sale (Dollars)","Sale_Dollars")\
      .withColumnRenamed("Volume Sold (Liters)","Volume_Sold_Liters")\
      .withColumnRenamed("Volume Sold (Gallons)","Volume_Sale_Gallons")\
      .withColumnRenamed("Category Name","Category_Name")\
      .withColumnRenamed("Store Name","Store_Name")\
      .withColumn("Sale_Dollars",col("Sale_Dollars").cast(IntegerType()))
df1.printSchema()
df2 =df1.withColumn('year_sale', df.Date.substr(7, 4))
df2.printSchema()
df3=df2.groupBy("year_sale","Category_Name").sum("Sale_Dollars")
df3.show()
w2 = Window.partitionBy("year_sale").orderBy(col("sum(Sale_Dollars)").desc())
df4=df3.withColumn("row",row_number().over(w2)) \
  .filter(col("row") == 1).drop("row")
df4.show()
df5=df2.groupBy("year_sale","Store_Name").sum("Sale_Dollars")
df5.show()
print(df5.count())
df6=df2.groupBy("Store_Name").mean("Sale_Dollars")
df6.show()
print(df6.count())
w3 = Window.partitionBy("year_sale").orderBy(col("sum(Sale_Dollars)").desc())
df7=df5.withColumn("row",row_number().over(w3)) \
  .filter(col("row") <= 10).drop("row")
df7.show()
print(df7.count())
df7.write.csv("/user/bigdatacloudxlab39242/adithya/Iowa_liquor_sales/sales_top10_store_year")
+---------+--------------------+-----------------+
|year_sale|          Store_Name|sum(Sale_Dollars)|
+---------+--------------------+-----------------+
|     2016|Hy-Vee #3 / BDI /...|          2174890|
|     2016|      Central City 2|          2074000|
|     2016|Hy-Vee Wine and S...|          1035242|
|     2016|Sam's Club 6344 /...|           693682|
|     2016|       Lot-A-Spirits|           682741|
|     2016|Sam's Club 8162 /...|           680809|
|     2016|   Benz Distributing|           602435|
|     2016|Hy-Vee Food Store...|           594103|
|     2016|      Wilkie Liquors|           524967|
|     2016|Costco Wholesale ...|           508241|
|     2012|Hy-Vee #3 / BDI /...|          7631386|
|     2012|Central City Liqu...|          6268580|
|     2012|Sam's Club 6344 /...|          3908213|
|     2012|Sam's Club 8162 /...|          3356146|
|     2012|Hy-Vee Wine and S...|          2869748|
|     2012|       Lot-A-Spirits|          2534869|
|     2012|Costco Wholesale ...|          2444859|
|     2012|Sam's Club 8238 /...|          2354655|
|     2012|      Wilkie Liquors|          2010116|
|     2012|Hy-Vee Wine and S...|          1834777|
+---------+--------------------+-----------------+
==========================================================
Bottom 10 store sale in each year
import pyspark
import pyspark.sql.functions
# importing sparksession from pyspark.sql module
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import*
# creating sparksession and giving an app name
spark = SparkSession.builder.appName('sparkdf').getOrCreate()
df = spark.read.option("header", "true").csv("/user/bigdatacloudxlab14968/Iowa_liquor_sales/Iowa_Liquor_Sales.csv")
df1=df.withColumnRenamed("Invoice/Item Number","Invoice_Item_Number")\
      .withColumnRenamed("Bottle Volume (ml)","Bottle_Volume_ml")\
      .withColumnRenamed("Sale (Dollars)","Sale_Dollars")\
      .withColumnRenamed("Volume Sold (Liters)","Volume_Sold_Liters")\
      .withColumnRenamed("Volume Sold (Gallons)","Volume_Sale_Gallons")\
      .withColumnRenamed("Category Name","Category_Name")\
      .withColumnRenamed("Store Name","Store_Name")\
      .withColumn("Sale_Dollars",col("Sale_Dollars").cast(IntegerType()))
df1.printSchema()
df2 =df1.withColumn('year_sale', df.Date.substr(7, 4))
df2.printSchema()
df3=df2.groupBy("year_sale","Category_Name").sum("Sale_Dollars")
df3.show()
w2 = Window.partitionBy("year_sale").orderBy(col("sum(Sale_Dollars)").desc())
df4=df3.withColumn("row",row_number().over(w2)) \
  .filter(col("row") == 1).drop("row")
df4.show()
df5=df2.groupBy("year_sale","Store_Name").sum("Sale_Dollars")
df5.show()
print(df5.count())
df6=df2.groupBy("Store_Name").mean("Sale_Dollars")
df6.show()
print(df6.count())
w3 = Window.partitionBy("year_sale").orderBy(col("sum(Sale_Dollars)"))
df8=df5.withColumn("row",row_number().over(w3)) \
  .filter(col("row") <= 10).drop("row")
df8.show()
df8.write.csv("/user/bigdatacloudxlab39242/adithya/Iowa_liquor_sales/sales_bottom10_store_year")

+---------+--------------------+-----------------+
|year_sale|          Store_Name|sum(Sale_Dollars)|
+---------+--------------------+-----------------+
|     2016|Casey's General S...|                6|
|     2016|Smokin' Joe's #16...|                8|
|     2016|Kum & Go # 1056/ ...|                8|
|     2016|Casey's General S...|               14|
|     2016|Brother's Market/...|               22|
|     2016|          Prime Star|               49|
|     2016|Casey's General S...|               56|
|     2016|Casey's General S...|               64|
|     2016|Casey's General S...|               68|
|     2016|  Oky Doky # 8 Foods|               69|
|     2012|Hartig Drug #12 /...|              505|
|     2012| Westland Fast Break|              660|
|     2012|Direct Liquor / A...|              664|
|     2012|         Big 10 Mart|              726|
|     2012|           JCL Store|             1061|
|     2012|   Werner Distilling|             1087|
|     2012| Crazy Mary's Licker|             1171|
|     2012|Casey's General S...|             1300|
|     2012|Forest Market Con...|             1396|
|     2012|Broadbent Distillery|             1502|
+---------+--------------------+-----------------+
only showing top 20 rows

total sales in each year
+---------+-----------------+
|year_sale|sum(Sale_Dollars)|
+---------+-----------------+
|     2016|         60643712|
|     2012|        216772111|
|     2020|            28839|
|     2019|        162164405|
|     2017|          2131696|
|     2014|        230415841|
|     2013|        219302991|
|     2018|          2118296|
|     2015|        246095366|
+---------+-----------------+
========================================= 
Difference in sales between the years 
import pyspark
import pyspark.sql.functions
# importing sparksession from pyspark.sql module
from pyspark.sql import SparkSession
from pyspark.sql.functions import*
from pyspark.sql.window import Window
from pyspark.sql.types import*
# creating sparksession and giving an app name
spark = SparkSession.builder.appName('sparkdf').getOrCreate()
df = spark.read.option("header", "true").csv("/user/bigdatacloudxlab14968/Iowa_liquor_sales/Iowa_Liquor_Sales.csv")
df1=df.withColumnRenamed("Invoice/Item Number","Invoice_Item_Number")\
      .withColumnRenamed("Bottle Volume (ml)","Bottle_Volume_ml")\
      .withColumnRenamed("Sale (Dollars)","Sale_Dollars")\
      .withColumnRenamed("Volume Sold (Liters)","Volume_Sold_Liters")\
      .withColumnRenamed("Volume Sold (Gallons)","Volume_Sale_Gallons")\
      .withColumnRenamed("Category Name","Category_Name")\
      .withColumnRenamed("Store Name","Store_Name")\
      .withColumn("Sale_Dollars",col("Sale_Dollars").cast(IntegerType()))
df1.printSchema()
df2 =df1.withColumn('year_sale', df.Date.substr(7, 4))
df2.printSchema()
df2.show(5)
df9=df2.groupBy("year_sale").sum("Sale_Dollars")
df9.show()
print(df9.count())
w2 = Window.orderBy(col("year_sale"))
df9.withColumn("lag",lag("sum(Sale_Dollars)",1).over(w2))\
   .withColumn("diff",col('sum(Sale_Dollars)')-col('lag'))\
   .show()
df9.printSchema()
+---------+-----------------+---------+------------------- +
|year_sale|sum(Sale_Dollars)|      lag|      diff          |
+---------+-----------------+---------+--------------------+
|     2012|        216772111|     null|      null          |
|     2013|        219302991|216772111|   2530880=2.5Millon|
|     2014|        230415841|219302991|  11112850=11.1Milln|
|     2015|        246095366|230415841|  15679525=15.6Milln|
|     2016|         60643712|246095366|-185451654=-185.4Mil|
|     2017|          2131696| 60643712| -58512016=-58.5miln|
|     2018|          2118296|  2131696|    -13400=-13.4k   |
|     2019|        162164405|  2118296| 160046109=162.Miln |
|     2020|            28839|162164405|-162135566=162.1miln|
+---------+-----------------+---------+--------------------+
====================================================================

----slaes by each state for each year

import pyspark
import pyspark.sql.functions
# importing sparksession from pyspark.sql module
from pyspark.sql import SparkSession
from pyspark.sql.functions import*
from pyspark.sql.window import Window
from pyspark.sql.types import*
# creating sparksession and giving an app name
spark = SparkSession.builder.appName('sparkdf').getOrCreate()
df = spark.read.option("header", "true").csv("/user/bigdatacloudxlab14968/Iowa_liquor_sales/Iowa_Liquor_Sales.csv")
df1=df.withColumnRenamed("Invoice/Item Number","Invoice_Item_Number")\
      .withColumnRenamed("Bottle Volume (ml)","Bottle_Volume_ml")\
      .withColumnRenamed("Sale (Dollars)","Sale_Dollars")\
      .withColumnRenamed("Volume Sold (Liters)","Volume_Sold_Liters")\
      .withColumnRenamed("Volume Sold (Gallons)","Volume_Sale_Gallons")\
      .withColumnRenamed("Category Name","Category_Name")\
      .withColumnRenamed("Store Name","Store_Name")\
      .withColumnRenamed("Zip Code","ZipCode")\
      .withColumn("Sale_Dollars",col("Sale_Dollars").cast(IntegerType()))
df1.printSchema()
df2 =df1.withColumn('year_sale', df.Date.substr(7, 4))
df2.printSchema()
df2.show(5)
df10=spark.read.option("header", "true").csv("/user/bigdatacloudxlab39242/adithya/Iowa_liquor_sales/zip-codes-database-FREE.csv")
df10.printSchema()
df10.show(10)
df11=df2.join(df10, on="ZipCode").select(df2['*'], df10['State'])
df11.printSchema()
df11.show(5)
df12=df11.groupBy("year_sale","State").sum("Sale_Dollars")
df12.orderBy("year_sale").show()
df13=df11.groupBy("State").count()
df13.show()

TotalCount of zip codes :41734
+---------+-----+-----------------+
|year_sale|State|sum(Sale_Dollars)|
+---------+-----+-----------------+
|     2012|   MN|             8001|
|     2012|   IA|        215638215|
|     2013|   MN|            14638|
|     2013|   IA|        218273350|
|     2014|   IA|        229336501|
|     2014|   MN|            22374|
|     2015|   IA|        245065771|
|     2015|   MN|            22035|
|     2016|   IA|         59876156|
|     2016|   MN|             7363|
|     2017|   IA|          1114369|
|     2018|   MN|              148|
|     2018|   IA|          2005492|
|     2019|   MN|            14141|
|     2019|   IA|        161336197|
|     2020|   IA|            28587|
+---------+-----+-----------------+

+-----+-------+
|State|  count|
+-----+-------+
|   MN|    514|
|   IA|8788116|
+-----+-------+


