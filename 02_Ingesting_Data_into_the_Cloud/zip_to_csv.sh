#!/usr/bin/env bash
#
# Extract zipped flight data to DATA_DIRECTORY.

DATA_DIRECTORY=./data
YEAR=2015

mkdir -p $DATA_DIRECTORY/csv

for MONTH in `seq -w 1 12`; do
   unzip $DATA_DIRECTORY/zip/$YEAR\_$MONTH.zip -d $DATA_DIRECTORY/zip/
   mv $DATA_DIRECTORY/zip/*ONTIME.csv $DATA_DIRECTORY/csv/$YEAR\_$MONTH.csv
done
