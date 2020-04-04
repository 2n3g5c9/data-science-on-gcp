#!/usr/bin/env bash

bash authorize_cloudshell.sh

MYSQL_IP=$(gcloud sql instances describe flights | grep ipAddress | tr ' ' '\n' | tail -1)
cat contingency.sql | sed 's/DEP_DELAY_THRESH/20/g' | sed 's/ARR_DELAY_THRESH/15/g' | mysql --host="$MYSQL_IP" --user=root --password --verbose
