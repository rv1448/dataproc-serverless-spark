gcloud dataproc jobs submit pyspark ~/sada_projects/nest/dataproc_ephemeral/pyspark_batch.py \
    --region=us-central1 --project=sada-rahulvangala-sandbox \
    --cluster=cluster-logs3 \
    -- --