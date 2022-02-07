from __future__ import division
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText, WriteToBigQuery
from apache_beam.dataframe.io import read_csv
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, SetupOptions, WorkerOptions
import pandas as pd
import pandas_gbq
import logging
from google.cloud import storage
import argparse
import csv
from apache_beam.dataframe.convert import to_pcollection


#Define Constants
PROJECT_ID = ''
DATASET_ID = ''
INPUT_TABLE_NAME = ''
OUTPUT_TABLE_NAME = ''
OUTPUT_TABLE_ID = PROJECT_ID + '.' + DATASET_ID + '.' + OUTPUT_TABLE_NAME
BUCKET = ''
FILENAME_PREFIX = ''
FILENAME = ''
FILE_EXT = '.txt'
URI_BUCKET = 'gs://' + BUCKET
INPUT_FILE_LOCATION = URI_BUCKET + '/' + FILENAME_PREFIX + FILENAME + FILE_EXT
STAGING_PREFIX = FILENAME_PREFIX + 'staging/'
STAGING_LOCATION = BUCKET + '/' + STAGING_PREFIX
TEMP_PREFIX = FILENAME_PREFIX + 'temp/'
TEMP_LOCATION = BUCKET + '/' + TEMP_PREFIX
URI_STAGING_LOCATION = 'gs://' + STAGING_LOCATION
URI_TEMP_LOCATION = 'gs://' + TEMP_LOCATION
MAX_WORKERS = 500
JOB_NAME = ''

     


pipeline_options = PipelineOptions(
    runner='DataflowRunner',
    project=PROJECT_ID,
    job_name=JOB_NAME,
    region='us-east4',
    subnetwork= '',
    use_public_ips=False,
    staging_location = URI_STAGING_LOCATION,
    temp_location = URI_TEMP_LOCATION,   
    machine_type = "n1-standard-1",
    max_num_workers = MAX_WORKERS,
    autoscaling_algorithm='THROUGHPUT_BASED'#None
    )
   
 
######################################
#           Import Schema            #
######################################


schema_str = ""
for i in sorted(SCHEMA_DEFINITION.keys()):
    schema_str += "{}:{},".format(i,SCHEMA_DEFINITION[i])
SCHEMA = schema_str[0:-1]

#query column headers from INFORMATION SCHEMA 
col_query = '''
    select 
     col.column_name
    from 
     `{0}.{1}.INFORMATION_SCHEMA.COLUMNS` col
    where 
     col.table_name = '{2}'
    order by 
     col.ordinal_position
   '''.format(PROJECT_ID, DATASET_ID, INPUT_TABLE_NAME)

COLUMN_DF = pandas_gbq.read_gbq(col_query,project_id=PROJECT_ID,use_bqstorage_api=True)
   
#Convert to list then add the scramble columns.
COLUMN_LIST = COLUMN_DF.astype(str).values.flatten().tolist()

print("COL")
    
class TextToBQ(beam.DoFn):
    # Do lazy initializaiton here. Otherwise, error messages pop up, associated with "A large DoFn instance that is serialized for transmission to remote workers.""
    def __init__(self,column_list):
        self.col_list = column_list
            
    #Converts row of txt to dictionary
    def process(self,line):
        import logging
        line = line.split("|")
        logging.warning(len(line))
        logging.warning(line)
        
        df = {}
        
        for i in range(0,len(self.col_list)):
            if line[i] == "" or self.col_list[i] == line[i]:
                df[self.col_list[i]] = None                         
            else:
                df[self.col_list[i]] = line[i]
                 
        yield {k: df[k] for k in sorted(df)}
        

#Big Query Configuration
DESTINATION_TABLE_CONFIG = {
    "table": OUTPUT_TABLE_NAME,
    "dataset": DATASET_ID,
    "project": PROJECT_ID,                    
    "schema": SCHEMA,
    "create_disposition": beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    "write_disposition": beam.io.BigQueryDisposition.WRITE_APPEND
    }

class DataFlowPipeline:
    """THIS IS THE CLASS THAT ACTUALLY RUNS THE JOB"""

    def run(self):
        """This is the job runner it holds the beam pipeline"""
            
        with beam.Pipeline(options=pipeline_options) as p: 
            #Runs the same beam pipeline on each different coverage type
           (p | "Read from File" >> beam.io.ReadFromText(INPUT_FILE_LOCATION) \
              | "TXT to CSV"     >> beam.ParDo(TextToBQ(COLUMN_LIST)) 
              | "Write to BQ"    >> beam.io.WriteToBigQuery(**DESTINATION_TABLE_CONFIG))
            
######################################
#    Delete Staging and Temp Files   #
######################################
def clean_up():
    bucket = storage.Client().bucket(BUCKET)
    staging_blobs = bucket.list_blobs(prefix=STAGING_PREFIX)
    temp_blobs = bucket.list_blobs(prefix=TEMP_PREFIX)
    for blob in staging_blobs:
        blob.delete()
    for blob in temp_blobs:
        blob.delete()

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    print('setting up config for runner...')
    trainer = DataFlowPipeline()
    trainer.run()
    clean_up()
    print('The runner is done!')

