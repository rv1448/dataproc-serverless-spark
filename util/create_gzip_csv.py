from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("BigQuery to GCS").getOrCreate()

# Set Google Cloud Project ID sada-rahulvangala-sandbox.demo_dataset.crime1
project_id = "sada-rahulvangala-sandbox"
# Set BigQuery source table
bigquery_table = "demo_dataset.crime1"
# Set GCS destination path
gcs_dest_path = "gs://rahulvangala-sandbox/input/"

# Read data from BigQuery
df = spark.read \
    .format("bigquery") \
    .option("project", project_id) \
    .option("table", bigquery_table) \
    .load()

df.count() 
# Write data to Google Cloud Storage in Parquet format
# df.coalesce(1).write.mode("overwrite").option("header","true").option("codec", "org.apache.hadoop.io.compress.GzipCodec").save(gcs_dest_path)

df.coalesce(1).write.mode("overwrite").option("header","true").option("codec", "org.apache.hadoop.io.compress.GzipCodec").csv("gs://rahulvangala-sandbox/crimes_input/")
