#!/bin/bash
#
# Upload CSV flight data to BUCKET.

DATA_DIRECTORY=data
BUCKET=data-science-on-gcp.marcm.dev

echo "Uploading to bucket $BUCKET..."
gsutil -m cp $DATA_DIRECTORY/csv/*.csv gs://$BUCKET/flights/raw