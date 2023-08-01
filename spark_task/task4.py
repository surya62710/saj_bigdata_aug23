Invoice_Item_Number

Date

Category

Category_Name

Vendor Number: string (nullable = true)
 |-- Vendor Name: string (nullable = true)
 |-- Item Number: string (nullable = true)
 |-- Item Description: string (nullable = true)
 |-- Pack: string (nullable = true)
 |-- Bottle_Volume_ml: string (nullable = true)
 |-- State Bottle Cost: string (nullable = true)
 |-- State Bottle Retail

 Bottles Sold: string (nullable = true)
 |-- Sale_Dollars: integer (nullable = true)
 |-- Volume_Sold_Liters: string (nullable = true)
 |-- Volume_Sale_Gallons
 year_sale


df14=df2.select(df2['Invoice_Item Number'],df2['Sale_Dollars'],df2['Category_Name'],df2['Item Number'],df2['Item Description'],df2['Bottle_Volume_ml'],df2['State Bottle Cost'],df2['Sta
te Bottle Retail'],df2['Bottles Sold'],df2['Volume_Sold_Liters'],df2['Volume_Sale_Gallons'])
===================================================================
root
 |-- Invoice_Item_Number: string (nullable = true)
 |-- Sale_Dollars: integer (nullable = true)
 |-- Category_Name: string (nullable = true)
 |-- Item Number: string (nullable = true)
 |-- Item Description: string (nullable = true)
 |-- Bottle_Volume_ml: string (nullable = true)
 |-- State Bottle Cost: string (nullable = true)
 |-- State Bottle Retail: string (nullable = true)
 |-- Bottles Sold: string (nullable = true)
 |-- Volume_Sold_Liters: string (nullable = true)
 |-- Volume_Sale_Gallons: string (nullable = true)



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
      .withColumnRenamed("State Bottle Retail","State_Bottle_Retail")\
      .withColumn("Sale_Dollars",col("Sale_Dollars").cast(IntegerType()))
df1.printSchema()
df2 =df1.withColumn('year_sale', df.Date.substr(7, 4))
df2.printSchema()
df14=df2.select(df2['Invoice_Item_Number'],df2['Sale_Dollars'],df2['Category_Name'],df2['Item Number'],df2['Item Description'],df2['Bottle_Volume_ml'],df2['State Bottle Cost'],        
df2['State_Bottle_Retail'],df2['Bottles Sold'],df2['Volume_Sold_Liters'],df2['Volume_Sale_Gallons'])
df14.printSchema()
df15=df14.where(df14.State_Bottle_Retail>10)
df15.show()
print(df15.count()) 
df16=df2.select(df2['Invoice_Item_Number'],df2['Date'],df2['Store_Name'],df2['Address'],df2['City'],df2['Store Location'],df2['County Number'],
df2['County'])
df16.printSchema()
df16.show()
df17=df15.join(df16, on="Invoice_Item_Number").select(df15['Invoice_Item_Number'], df15['Item Description'],df15['State_Bottle_Retail'])
df17.show() 


8853474

8385727====after removing sale <10 $

total no of records <10$=467,747





Count of records more than 10$()State_Bottle_Retail=5804356

+-------------------+--------------------+-------------------+
|Invoice_Item_Number|    Item Description|State_Bottle_Retail|
+-------------------+--------------------+-------------------+
|    INV-00005200061|Windsor Canadian Pet|              14.18|
|    INV-00016000001|        Maker's Mark|              25.98|
|    INV-00016100011|Smirnoff Vodka 80...|              12.38|
|    INV-00028700050|       Bacardi Limon|              14.25|
|    INV-00029700043|       Kinky Liqueur|              15.00|
|    INV-00030600082|Captain Morgan Sp...|              27.00|
|    INV-00034600106|Remy Martin Vsop ...|              33.72|
|    INV-00049000049|Kessler Blend Whi...|              16.53|
|    INV-00054400038|   Crown Royal Black|              25.50|
|    INV-00055200012|  Malibu Coconut Rum|              11.24|
|    INV-00062700172|Johnnie Walker Black|              31.49|
|    INV-00068900037|    Absolut Ruby Red|              17.24|
|    INV-00073700007|   Kraken Black Mini|              16.13|
|    INV-00074800001|        Black Velvet|              14.93|
|    INV-00289200004|       Hawkeye Vodka|              10.76|
|    INV-00323800031|Captain Morgan Ca...|              13.59|
|    INV-00328900044|             Jameson|              28.47|
|    INV-00453900044|  Malibu Coconut Rum|              15.74|
|    INV-00555000114|   Piehole Pecan Pie|              11.24|
|    INV-00624600013|Red Stag Black Ch...|              15.74|
+-------------------+--------------------+-------------------+
Total records after State_Bottle_Cost < 10$ removing ==3046875
+-------------------+--------------------+-----------------+
|Invoice_Item_Number|    Item Description|State_Bottle_Cost|
+-------------------+--------------------+-----------------+
|    INV-00016000001|        Maker's Mark|            17.32|
|    INV-00030600082|Captain Morgan Sp...|            18.00|
|    INV-00034600106|Remy Martin Vsop ...|            22.48|
|    INV-00049000049|Kessler Blend Whi...|            11.02|
|    INV-00054400038|   Crown Royal Black|            17.00|
|    INV-00062700172|Johnnie Walker Black|            20.99|
|    INV-00068900037|    Absolut Ruby Red|            11.49|
|    INV-00073700007|   Kraken Black Mini|            10.75|
|    INV-00328900044|             Jameson|            18.98|
|    INV-00453900044|  Malibu Coconut Rum|            10.49|
|    INV-00624600013|Red Stag Black Ch...|            10.49|
|    INV-00741100031|Woodford Reserve ...|            21.99|
|    INV-01788000045|       Ciroc Coconut|            18.49|
|    INV-02381800015|Captain Morgan Sp...|            18.00|
|    INV-02453100010|         Hennessy VS|            10.49|
|    INV-02622900001|    Southern Comfort|            10.33|
|    INV-02926700025|         St. Germain|            20.00|
|    INV-03135900008|        Black Velvet|            10.45|
|    INV-03241900039|Southern Comfort PET|            20.26|
|    INV-03581000012| 1800 Silver Tequila|            15.46|
+-------------------+--------------------+-----------------+
