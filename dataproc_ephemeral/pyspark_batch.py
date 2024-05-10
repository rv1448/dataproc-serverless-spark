# .config('spark.jars.packages', 'com.google.cloud.spark:spark-3.5-bigquery:0.37.0') \
# print(os.popen("hdfs dfs -ls /tmp").read())

import os

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('BQ_ephemeral_insert').config('spark.jars.packages', 'com.google.cloud.spark:spark-3.5-bigquery:0.37.0,com.google.cloud.bigdataoss:gcs-connector:3.0.0').getOrCreate()
import time

# print(os.popen("ls -l").read())
# os.popen("gcloud storage cp gs://rahulvangala-sandbox/2023_9.csv .")

print(20 * "$")
# print(os.popen("ls -al").read())
# print(os.popen("gsutil cp gs://rahulvangala-sandbox/311_requests/request000000000000.gz /tmp/").read())
# print(os.popen("ls -al ").read())
# print(os.popen("pwd").read())
# print(os.popen("gunzip -v /tmp/request000000000000.gz").read())
# print(os.popen("ls -al /tmp").read())
# print(os.popen("ls -al /tmp").read())
# print(os.popen("ls -al /tmp").read())
# print(os.popen("ls -al /tmp | grep request*").read())

# gsutil cat gs://rahulvangala-sandbox/311_requests/request000000000000.gz | zcat | gsutil cp - gs://rahulvangala-sandbox/311_requests/req
print(os.popen("gsutil cat gs://rahulvangala-sandbox/311_requests/request000000000000.gz | zcat | hdfs dfs -put - /tmp/cluster/file4").read())
print(20 * "$")
# /var/dataproc/tmp/srvls-batch-cec94657-7477-43d2-ad3a-9b58777e5734
    # .load("./request000000000000")
# df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("hdfs:///tmp/file/test.csv").limit(10)    
    
    
df = spark.read.format("csv").option("header","true") \
    .option("inferSchema","true") \
    .load("hdfs:///tmp/cluster/file4").limit(10)
    
print(df.printSchema())
print(df.count())


df.write \
    .format("bigquery")\
    .option("writeDisposition", "WRITE_TRUNCATE")\
    .option("writeMethod","direct")\
    .mode("overwrite")\
    .save("demo_dataset.pyspark_batch2")
    # .option("temporaryGcsBucket","rahulvangala-tmp")\
    # .option("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")\
    # .option("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")\