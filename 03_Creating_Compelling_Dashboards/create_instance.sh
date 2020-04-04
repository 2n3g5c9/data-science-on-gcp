#!/usr/bin/env bash

TIER="db-f1-micro"
DATABASE_VERSION="MYSQL_5_7"

gcloud sql instances create flights \
    --tier="$TIER" \
    --database-version="$DATABASE_VERSION" \
    --activation-policy=ALWAYS \
    --region=europe-west1

echo "Please go to the GCP console and change the root password of the instance"
