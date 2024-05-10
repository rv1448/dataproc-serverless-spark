CONFS="spark.sql.catalog.iceberg_catalog=org.apache.iceberg.spark.SparkCatalog,"
CONFS+="spark.sql.catalog.iceberg_catalog.catalog-impl=org.apache.iceberg.gcp.biglake.BigLakeCatalog,"
CONFS+="spark.sql.catalog.iceberg_catalog.blms_catalog=dpms-v1,"
CONFS+="spark.sql.catalog.iceberg_catalog.gcp_project=sada-rahulvangala-sandbox,"
CONFS+="spark.sql.catalog.iceberg_catalog.gcp_location=us-central1,"

CONFS+="spark.sql.catalog.iceberg_catalog.warehouse=gs://rahulvangala-sandbox/iceberg-warehouse,"
# CONFS+="spark.sql.warehouse.dir=gs://rahulvangala-sandbox/iceberg-warehouse," 
CONFS+="spark.sql.catalog.spark_hms_catalog.uri=thrift://10.125.64.27:9083" 
CONFS+="spark.jars.packages=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2"

gcloud dataproc jobs submit spark-sql --cluster=cluster-logs3 \
  --project=sada-rahulvangala-sandbox \
  --region=us-central1 \
  --jars=gs://spark-lib/biglake/biglake-catalog-iceberg1.2.0-0.1.1-with-dependencies.jar \
  --properties="${CONFS}" \
  --file=/Users/rahul.vangala/sada_projects/nest/create_namespace.sql