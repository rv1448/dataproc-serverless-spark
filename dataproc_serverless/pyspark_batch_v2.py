import os
import time
import logging
from pyspark.sql import SparkSession
from re import split

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_spark_session(): 
    spark = SparkSession.builder \
        .appName('BQ_ephemeral_insert') \
        .config('spark.jars.packages', 'com.google.cloud.spark:spark-3.5-bigquery:0.37.0,com.google.cloud.bigdataoss:gcs-connector:3.0.0') \
        .getOrCreate()
    return spark

def execute_shell_command(command):
    """Execute a shell command and return its output."""
    output = os.popen(command).read()
    logging.info(f"Command Output: {output}")
    return output

# def copy_and_decompress_data_zcat(gcs_path, hdfs_path):
#     """Copy and decompress data using zcat."""
#     zcat_hdfs_path = f"{hdfs_path}_zcat"
#     command = f"gsutil cat {gcs_path} | zcat | hdfs dfs -put - {zcat_hdfs_path}"
#     start_time = time.time()
#     execute_shell_command(command)
#     end_time = time.time()
#     logging.info(f"{command}")
#     logging.info(f"zcat uncompression command execution time: {end_time - start_time:.2f} seconds")
#     return zcat_hdfs_path

def copy_and_decompress_data_rapidgzip(gcs_path, output_gcs_path):
    """Copy and decompress data using rapidgzip."""
  
    command = f"gsutil cat {gcs_path} | rapidgzip -d | gsutil cp - {output_gcs_path}"
    start_time = time.time()
    execute_shell_command(command)
    end_time = time.time()
    logging.info(f"{command}")
    logging.info(f"rapidgzip uncompression command execution time: {end_time - start_time:.2f} seconds")
    return output_gcs_path

def read_data_from_hdfs_write_to_bq(spark, output_gcs_path, bq_table):
    """Read data from HDFS and write to BigQuery."""
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(output_gcs_path) \
        .limit(10)
    df.printSchema()
    count = df.count()
    logging.info(f"Row count: {count}")
    df.write \
        .format("bigquery") \
        .option("writeDisposition", "WRITE_TRUNCATE") \
        .option("writeMethod", "direct") \
        .mode("overwrite") \
        .save(bq_table)

def main():
    """Main function to orchestrate data handling tasks."""
    spark = create_spark_session()
    app_id = spark.sparkContext.applicationId
    logging.info(f"Spark Application ID: {app_id}")
    id = split(r"[-]", app_id)
    gcs_path = 'gs://rahulvangala-sandbox/crimes_input/part-crimes.gz'
    output_gcs_path = 'gs://rahulvangala-sandbox/crimes_output/part-crimes2-' + id[1]
     
    rgzip_hdfs_path = copy_and_decompress_data_rapidgzip(gcs_path, output_gcs_path)
    read_data_from_hdfs_write_to_bq(spark, output_gcs_path, bq_table='demo_dataset.pyspark_batch6')

if __name__ == "__main__":
    main()