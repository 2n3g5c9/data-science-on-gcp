#!/usr/bin/env python3

import apache_beam as beam
import csv

if __name__ == '__main__':
	with beam.Pipeline('DirectRunner') as pipeline:

		airports = (pipeline
					| beam.io.ReadFromText('airports.csv.gz')
					| beam.Map(lambda line: next(csv.reader([line])))
					| beam.Map(lambda fields: (fields[0], (fields[21], fields[26])))
					)

		(airports
		 | beam.Map(lambda airport_data: '{},{}'.format(airport_data[0], ','.join(airport_data[1])) )
		 | beam.io.WriteToText('extracted_airports')
		 )

		pipeline.run()
