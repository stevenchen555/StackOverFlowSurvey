/usr/local/hadoop/bin/hdfs dfsadmin -safemode leave
/usr/local/hadoop/bin/hdfs dfs -rm -r /data/out
/usr/local/hadoop/bin/hdfs dfs -rm -r /data/temp_out
/usr/local/hadoop/bin/hadoop jar RankIDE.jar RankIDE -libjars /usr/lib/jvm/java-1.8.0-openjdk.x86_64/lib/opencsv-4.5.jar /data/surveys /data/out
