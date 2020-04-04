#!/usr/bin/env bash

# To run mysqlimport and mysql, authorize CloudShell
bash authorize_cloudshell.sh

# Connect to MySQL using its IP address and do the import
MYSQL_IP=$(gcloud sql instances describe flights --format="value(ipAddresses.ipAddress)")
mysql --host="$MYSQL_IP" --user=root --password --verbose < create_table.sql
