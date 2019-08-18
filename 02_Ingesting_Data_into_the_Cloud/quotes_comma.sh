#!/bin/bash
#
# Remove quotes in the CSV flight data.

DATA_DIRECTORY=data
YEAR=2015

for MONTH in `seq -w 1 12`; do
    echo $DATA_DIRECTORY/csv/$YEAR\_$MONTH.csv
    sed 's/,$//g' $DATA_DIRECTORY/csv/$YEAR\_$MONTH.csv | sed 's/"//g' > tmp
    mv tmp $DATA_DIRECTORY/csv/$YEAR\_$MONTH.csv
done