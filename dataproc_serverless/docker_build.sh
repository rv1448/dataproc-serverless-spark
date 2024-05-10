IMAGE=us-central1-docker.pkg.dev/sada-rahulvangala-sandbox/datapipeline/gam_gsutil_local:latest
# Download the BigQuery connector.
# gsutil cp \
#   gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.22.2.jar .

# Download the Miniconda3 installer.
# wget https://repo.anaconda.com/miniconda/Miniconda3-py39_4.10.3-Linux-x86_64.sh\
# Miniconda3-py39_24.1.2-0-Linux-x86_64.sh
# wget https://repo.anaconda.com/miniconda/Miniconda3-py39_24.1.2-0-Linux-x86_64.sh
# Python module example
# cp ./py_spark_batch.py .

# Build and push the image.
docker build -t "${IMAGE}" .
docker push "${IMAGE}"