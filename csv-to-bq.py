import logging
import io

import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.pipeline import PipelineOptions

p = beam.Pipeline(options=PipelineOptions(pipeline_args))

(
 p 
 | beam.Create(urls)
 | 'Finding latest file' >> fileio.MatchAll()
 | 'Get file handlers' >> fileio.ReadMatches()
 | 'Read each file handler' >> beam.FlatMap(
       lambda rf: csv.reader(io.TextIOWrapper(rf.open())))
 | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
         known_args.output,
         schema=schema ,
         create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
         write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)))

p.run().wait_until_finish()