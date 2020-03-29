#!/usr/bin/env bash

gcloud sql instances create flights \
    --tier=db-n1-standard-1 --activation-policy=ALWAYS --gce-zone=europe-west1-b --database-version=MYSQL_5_7

echo "Please go to the GCP console and change the root password of the instance"
