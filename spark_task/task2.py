#/user/bigdatacloudxlab14968/Iowa_liquor_sales/Iowa_Liquor_Sales.csv



import pyspark
# importing sparksession from pyspark.sql module
from pyspark.sql import SparkSession
from pyspark.sql.functions as f
# creating sparksession and giving an app name
spark = SparkSession.builder.appName('sparkdf').getOrCreate()
df = spark.read.option("header", "true").csv("/user/bigdatacloudxlab14968/Iowa_liquor_sales/Iowa_Liquor_Sales.csv")
df.printSchema()
print(df.count())
df1=df.distinct()
print(df1.count())
df2=df.groupBy("Date","Category Name","Sale (Dollars)").count()
df2.filter(df2.date=="2015")
#===========================================================================
#Sale (Dollars)
#Item Description

val df3 = spark.read.option("header", "true").csv("/user/bigdatacloudxlab14968/Iowa_liquor_sales/Iowa_Liquor_Sales.csv")
df3.show()

select count(Sale(Dollars))
from table name
GroupBy Category Name
where Date=2015""



df2=df.groupBy("Date","Category Name","Sale (Dollars)").count()
df2.show()
df3=df2.filter(df2.date="2015")
df3.show() 

df2 = df.withColumn("Date", df["Date"].cast('float'))

df2=df.groupBy("Invoice/Item Number").count()
df2.show()

.withColumn("year", ._year(col("Date")

df1 = df.withColumn('year_sale',to_year(df.Date))

df1 = df1.withColumn('years_sale',f.year(f.to_timestamp('Date', 'MM-dd-yyyy')))

=======================================================================
import pyspark
import pyspark.sql.functions as f
# importing sparksession from pyspark.sql module
from pyspark.sql import SparkSession
# creating sparksession and giving an app name
spark = SparkSession.builder.appName('sparkdf').getOrCreate()
df = spark.read.option("header", "true").csv("/user/bigdatacloudxlab14968/Iowa_liquor_sales/Iowa_Liquor_Sales.csv")
df.printSchema()
df1 =df.withColumn('year_sale', df.Date.substr(7, 4))
df1.printSchema()
df2=df1.groupBy("date","year_sale","Category Name","Sale (Dollars)").count()
df3=df2.filter("year_sale==2015")
df3.show(20)
print(df3.count())
print(df.count())


select count(sales)
from df
group By Category Name
where year=2015



count of tota records:8853474
count of grouped data :659005


===================================================
Invoice_Item_Number: string (nullable = true)
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
 |-- Category Name: string (nullable = true)
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

Invoice_Item_Number: string (nullable = true)
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
 |-- Category Name: string (nullable = true)
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
 |-- Volume_Sale_Gallons:
 
~                 