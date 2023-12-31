/*create an external table on sale file in hive
2.create another table from this external table and put data only if sale price is less than 20
2781268
2.1.convert the sale in ruppes
==============================


========================================

Invoice_Item_Number string,
  Date date, 
  Store Number string, 
  Store Name string,
  Address string, 
  City string, 
  Zip Code string, 
  Store Location string, 
  County Number string, 
  County string, 
  Category string, 
  Category_Name string, 
  Vendor Number string, 
  Vendor Name string, 
  Item Number string, 
  Item Description string, 
  Pack string, 
  Bottle_Volume_ml string, 
  State Bottle Cost string, 
  State Bottle Retail string, 
  Bottles Sold string, 
  Sale_Dollars string,
  Volume_Sold_Liters string, 
  Volume_Sale_Gallons string, 

=========================================
creating a external table over a csv file with removing the first row in csv file
===============================================*/
CREATE EXTERNAL TABLE IF NOT EXISTS sale_liquor_iowa
      (Invoice_Item_Number string,======= 8853474
       Date_sold string, ========1867
       Store_Number string,=======2240
       Store_Name string,=========2360
       Address string, ===========3145
       City string, ==============1249
       Zip_Code string,===========806
       Store_Location string,=====1825 
       County_Number string,======= 433
       County string,============== 296
       Category string,============= 271
       Category_Name string, ======233
       Vendor_Number string,======== 472
       Vendor_Name string, =========673
       Item_Number string,===========6591 
       Item_Description string, =====10876
       Pack string, ================6586
       Bottle_Volume_ml string,=====1452 
       State_Bottle_Cost string,=====2500 
       State_Bottle_Retail string,=== 3647
       Bottles_Sold string, =========3510
       Sale_Dollars string,===========5837
       Volume_Sold_Liters string,=====13514
       Volume_Sale_Gallons string)====5920
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/bigdatacloudxlab14968/Iowa_liquor_sales'
tblproperties("skip.header.line.count"="1");

/*total count=8853474
=============================================================

/user/bigdatacloudxlab39242*/


 desc sale_iowa
    > ;
/*OK
invoice_item_number     string                                         
date_sold               date                                        
store_number            string                                         
store_name              string                                      
address                 string                                      
city                    string                                      
zip_code                string                                      
store_location          string                                      
county_number           string                                      
county                  string                                      
category                string                                      
category_name           string                                      
vendor_number           string                                         
vendor_name             string                                  
item_number             string                                         
item_description        string                                      
pack                    string                                      
bottle_volume_ml        string                                         
state_bottle_cost       string                                         
state_bottle_retail     string                                         
bottles_sold            string                                         
sale_dollars            string                                         
volume_sold_liters      string                                         
volume_sale_gallons     string                                         
Time taken: 0.152 seconds, Fetched: 24 row(s)
========================================================

=======================================================
Creating table using the select Result
=====================================================*/
create table salels_20 as
select *,(sale_dollars * 81.29) as sale_rupees
from sale_liquor_iowa 
where sale_dollars<20

/*count =2781268
=====================================================
Changing the column data type
==================================================*/
ALTER TABLE slaels_20 CHANGE sale_dollars sale_dollars int;
/*========================================================*/
creating a hive partition table

hive> create table part_zip (
invoice_item_number     string,                                      
date_sold               string,                                      
store_number            string,                                      
store_name              string,                                      
address                 string,                                      
city                    string,                                                                            
store_location          string,                                      
county_number           string,                                      
county                  string,                                      
category                string, 
Category_Name           string,                                     
vendor_number           string,                                      
vendor_name             string,                                      
item_number             string,                                      
item_description        string,                                      
pack                    string,                                      
bottle_volume_ml        string,                                      
state_bottle_cost       string,                                      
state_bottle_retail     string,                                      
bottles_sold            string,                                      
sale_dollars            int,                                         
volume_sold_liters      string,                                      
volume_sale_gallons     string,                                      
sale_rupees             double)   
partitioned by (zip_code int)  
CLUSTERED BY (county) INTO 30 BUCKETS
row format delimited  
fields terminated by ',';


/*Now, insert the data of salels_20 table into the partition table.*/

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

hive> insert into table part_zip
partition(zip_code)  
select *  
from salels_20;



/*==================================================*/
desc formatted part_salales20;

/*o/p:
----
OK
# col_name              data_type               comment             
                 
invoice_item_number     string                                      
date_sold               string                                      
store_number            string                                      
store_name              string                                      
address                 string                                      
city                    string                                      
zip_code                string                                      
store_location          string                                      
county_number           string                                      
county                  string                                      
category                string                                      
vendor_number           string                                      
vendor_name             string                                      
item_number             string                                      
item_description        string                                      
pack                    string                                      
bottle_volume_ml        string                                      
state_bottle_cost       string                                      
state_bottle_retail     string                                      
bottles_sold            string                                      
sale_dollars            int                                         
volume_sold_liters      string                                      
volume_sale_gallons     string                                      
sale_rupees             double                                      
                 
# Partition Information          
# col_name              data_type               comment             
                 
category_name           string                                      
                 
# Detailed Table Information             
Database:               saj_sale                 
Owner:                  bigdatacloudxlab39242    
CreateTime:             Mon Jan 16 16:21:53 UTC 2023     
LastAccessTime:         UNKNOWN                  
Protect Mode:           None                     
Retention:              0                        
Location:               hdfs://cxln1.c.thelab-240901.internal:8020/apps/hive/warehouse/saj_sale.db/part_salels20         
Table Type:             MANAGED_TABLE            
Table Parameters:                
        transient_lastDdlTime   1673886113          
                 
# Storage Information            
SerDe Library:          org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe       
InputFormat:            org.apache.hadoop.mapred.TextInputFormat         
OutputFormat:           org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat       
Compressed:             No                       
Num Buckets:            -1                       
Bucket Columns:         []                       
Sort Columns:           []                       
Storage Desc Params:             
        field.delim             ,                   
        serialization.format    ,                   
Time taken: 0.074 seconds, Fetched: 55 row(s)

============================================================
group by using date_sold
10/13/2015      9737
10/14/2016      50
10/16/2018      44
10/22/2013      6830
10/23/2014      6569
10/25/2016      55
10/26/2017      56
11/01/2013      36
11/03/2015      9391
11/04/2016      32
11/06/2018      67
11/07/2019      6936
11/12/2013      6770
11/13/2014      6792
11/15/2016      66
11/16/2017      64
11/18/2019      8156
11/23/2013      5098
11/24/2014      10204
11/25/2015      820
11/27/2017      77
11/28/2018      46
11/29/2019      1529
12/02/2013      12104
12/03/2014      12490
12/04/2015      66
12/05/2016      80
12/06/2017      132
12/07/2018      27
12/12/2012      8697
12/13/2013      220
12/15/2015      13720
12/16/2016      41
12/18/2018      80
12/24/2013      605
12/26/2015      13557
12/27/2016      90
12/28/2017      88
12/29/2018      27
====================================================
5356    275
5367    112
5378    63
5389    72
5400    75
5411    534
5422    220
5444    1464
5455    488
5466    1154
5477    69
5488    80
5499    327
5510    329
5521    155
5532    68
5543    76
5554    69
5565    400
5576    152
5587    65
5598    507
5620    171
5631    201
5642    239
5653    112
5664    63
5675    20
5686    201
5697    97
5730    463
5741    313
5752    89
5763    96
5774    91
5785    107
5796    19
5840    120
5851    37
5862    3*/


