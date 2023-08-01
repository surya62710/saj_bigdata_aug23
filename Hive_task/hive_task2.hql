/*/user/bigdatacloudxlab14968/Iowa_liquor_sales*/

CREATE EXTERNAL TABLE IF NOT EXISTS sales_iowa
(invoice_item_number     string,                                         
date_sold                string,                                        
store_number            string,                                         
store_name              string,                                      
address                 string,                                      
city                    string,                                      
zip_code                string,                                      
store_location          string,                                      
county_number           string,                                      
county                  string,                                      
category                string,                                      
category_name           string,                                      
vendor_number           string,                                         
vendor_name             string,                                  
item_number             string,                                         
item_description        string,                                      
pack                    string,                                      
bottle_volume_ml        string,                                         
state_bottle_cost       string,                                         
state_bottle_retail     string,                                         
bottles_sold            string,                                         
sale_dollars            string,                                         
volume_sold_liters      string,                                         
volume_sale_gallons     string)
LOCATION '/user/bigdatacloudxlab14968/Iowa_liquor_sales'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
SET store_name = TRIM(store_name),
    city = TRIM(city),
    category_name = TRIM(category_name),
    vendor_name=TRIM(vendor_name),
    item_description=TRIM(item_description)
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");




CREATE TABLE sale_li_iowa (
invoice_item_number     string,                                         
date_sold                string,                                        
store_number            string,                                         
store_name              string,                                      
address                 string,                                      
city                    string,                                      
zip_code                string,                                      
store_location          string,                                      
county_number           string,                                      
county                  string,                                      
category                string,                                      
category_name           string,                                      
vendor_number           string,                                         
vendor_name             string,                                  
item_number             string,                                         
item_description        string,                                      
pack                    string,                                      
bottle_volume_ml        string,                                         
state_bottle_cost       string,                                         
state_bottle_retail     string,                                         
bottles_sold            string,                                         
sale_dollars            string,                                         
volume_sold_liters      string,                                         
volume_sale_gallons     string
);

INSERT INTO TABLE sale_li_iowa
SELECT invoice_item_number,date_sold,store_number,store_name,TRIM(address), TRIM(city),TRIM(zip_code),TRIM(store_location),county_number, TRIM(county),category,TRIM(Category_Name),vendor_number,vendor_name,item_number,TRIM(item_description),pack,bottle_volume_ml,state_bottle_cost,state_bottle_retail,bottles_sold,sale_dollars,volume_sold_liters,volume_sale_gallons
FROM sale_liquor_iowa;