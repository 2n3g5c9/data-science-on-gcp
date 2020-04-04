#!/usr/bin/env bash

if [ "$#" -ne 1 ]; then
    echo "Usage: ./populate_table.sh bucket-name"
    exit
fi

BUCKET=$1
echo "Populating Cloud SQL instance flights from gs://${BUCKET}/flights/raw/..."

# To run mysqlimport and mysql, authorize CloudShell
bash authorize_cloudshell.sh

# The table name for mysqlimport comes from the filename, so rename our CSV files, changing bucket name as needed
counter=0
#for FILE in $(gsutil ls gs://${BUCKET}/flights/raw/2015*.csv); do
#   gsutil cp $FILE flights.csv-${counter}
for FILE in 2015_01.csv 2015_07.csv; do
   gsutil cp gs://${BUCKET}/flights/raw/${FILE} flights.csv-${counter}
   counter=$((counter+1))
done

# Import CSV files
MYSQL_IP=$(gcloud sql instances describe flights --format="value(ipAddresses.ipAddress)")
mysqlimport --local --host="$MYSQL_IP" --user=root --ignore-lines=1 --fields-terminated-by="," --password bts flights.csv-*
rm flights.csv-*
