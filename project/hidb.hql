DROP DATABASE IF EXISTS forex;
create database forex;
use forex;
create external table if not exists eurusd(
time timestamp,
open double,
high double,
low double,
close double)
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as textfile
location '/user/bigdatacloudxlab27228/hdfs_forex_landing'
tblproperties("skip.header.line.count"="1");