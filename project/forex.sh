rm -r /home/bigdatacloudxlab27228/saj5_31_2023/saj_forex_landing
mkdir /home/bigdatacloudxlab27228/saj5_31_2023/saj_forex_landing
cp /home/bigdatacloudxlab27228/saj5_31_2023/EURUSD_5.csv /home/bigdatacloudxlab27228/saj5_31_2023/saj_forex_landing
hdfs dfs -rm -r hdfs_forex_landing
hdfs dfs mkdir hdfs_forex_landing
hdfs dfs -copyFromLocal /home/bigdatacloudxlab27228/saj5_31_2023/saj_forex_landing/EURUSD_1.csv hdfs_forex_landing
hive -f /home/bigdatacloudxlab27228/saj5_31_2023/hidb.hql
spark-submit /home/bigdatacloudxlab27228/saj5_31_2023/forex.py
sh /home/bigdatacloudxlab27228/saj5_31_2023/sqop.sh