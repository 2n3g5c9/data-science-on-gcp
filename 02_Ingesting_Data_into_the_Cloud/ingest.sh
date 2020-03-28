#!/usr/bin/env bash
#
# Perform all of the necessary actions from download to upload on GCS.

bash download.sh
bash zip_to_csv.sh
bash quotes_comma.sh
bash upload.sh
