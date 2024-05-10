import os

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('BQ_ephemeral_insert').config('spark.jars.packages', 'com.google.cloud.spark:spark-3.5-bigquery:0.37.0,com.google.cloud.bigdataoss:gcs-connector:3.0.0').getOrCreate()
import time


df = spark.read.format("csv").option("header","true") \
    .option("inferSchema","true") \
    .load("/Users/rahul.vangala/sada_projects/nest/util/request000000000000.gz").limit(10)
    
# export PYSPARK_PYTHON=/opt/miniconda3/envs/nest/bin/python
# export PYSPARK_DRIVER_PYTHON=python3

# from re import split


# hdfs_path = '/tmp/cluster/file4'
# command = f"hdfs dfs -put - {hdfs_path}" + "_zcat"
# print(command)
# test="app-20170228091742-0025"
# splitter = split(r"[-]", test)
# print(splitter[1])
