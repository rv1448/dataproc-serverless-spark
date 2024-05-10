gcloud dataproc batches submit pyspark ./pyspark_batch_v2.py \
    --container-image="us-central1-docker.pkg.dev/sada-rahulvangala-sandbox/datapipeline/gam_gsutil_local:latest" \
    --project sada-rahulvangala-sandbox --region us-central1 --subnet default \
    --version 2.2 \
    --deps-bucket gs://rahulvangala-sandbox