sqoop export \
--connect jdbc:mysql://cxln2:3306/sqoopex \
--username 'sqoopuser' \
--password 'NHkkP876rp' \
--table forex1 \
--export-dir hdfs://cxln1.c.thelab-240901.internal:8020/user/bigdatacloudxlab27228/saj_spark_result/sql_data \
--input-fields-terminated-by ',' \
--lines-terminated-by '\n' \
--m 1

create table forex1(time varchar(50),open varchar(50),high varchar(50),low varchar(50),close varchar(50));