from pyspark.sql import SparkSession

iceberg_spark_package = "org.apache.iceberg:iceberg-spark3-runtime:0.12.0"
# biglake_iceberg_catalog_jar = "gs://spark-lib/biglake/biglake-catalog-iceberg1.2.0-0.1.1-with-dependencies.jar"
project_id = "sada-rahulvangala-sandbox"
location = "us-central1"
blms_catalog = "dpms-v1"
gcs_data_warehouse_folder = "gs://rahulvangala-sandbox/iceberg-warehouse"
hive_metastore_uri = "thrift://10.125.64.27:9083"

spark = SparkSession.builder \
    .appName('Iceberg BigLake Integration') \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,com.google.cloud.bigdataoss:gcs-connector:3.0.0") \
    .config("spark.jars", "/Users/rahul.vangala/sada_projects/nest/biglake-catalog-iceberg0.14.0-0.1.0-with-dependencies.jar") \
    .config("spark.sql.catalog.test_iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.test_iceberg.catalog-impl", "org.apache.iceberg.gcp.biglake.BigLakeCatalog") \
    .config("spark.sql.catalog.test_iceberg.gcp_project", project_id) \
    .config("spark.sql.catalog.test_iceberg.gcp_location", location) \
    .config("spark.sql.catalog.test_iceberg.blms_catalog", blms_catalog) \
    .config("spark.sql.catalog.test_iceberg.warehouse", gcs_data_warehouse_folder) \
    .config("spark.sql.catalog.spark_hms_catalog.uri", hive_metastore_uri) \
    .config("spark.sql.catalog.your_catalogName.type", "hive") \
    .config("spark.sql.warehouse.dir","gs://rahulvangala-sandbox/iceberg-warehouse") \
    .getOrCreate() 

spark.sql("SHOW TABLES IN test_iceberg").show()