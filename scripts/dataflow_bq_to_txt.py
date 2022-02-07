######################################
#
# This script creates a pipe ('|') delimited file out of a table in BigQuery
#
# TO RUN JOB ON DATAFLOW
# python dataflow_bq_to_txt.py 
#
######################################



#Import Libraries
from __future__ import division
import apache_beam as beam
from apache_beam.io import ReadFromBigQuery, WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, SetupOptions, WorkerOptions
import google.cloud.storage.client as storage
import logging
import pandas as pd
import pandas_gbq

#Toggle for DEBUG mode
DEBUG = 1

#Define Constants
PROJECT_ID = ''
DATASET_ID = ''
INPUT_TABLE_NAME = ''
INPUT_TABLE_ID = PROJECT_ID + '.' + DATASET_ID + '.' + INPUT_TABLE_NAME
BUCKET = ''
FILENAME_PREFIX = ''
FILENAME = ''
HEADER_FILENAME = FILENAME_PREFIX + 'header.txt'
URI_BUCKET = 'gs://' + BUCKET
OUTPUT_FILE_LOCATION = URI_BUCKET + '/' + FILENAME_PREFIX
OUTPUT_FILENAME = OUTPUT_FILE_LOCATION + FILENAME
STAGING_PREFIX = FILENAME_PREFIX + 'staging/'
STAGING_LOCATION = BUCKET + '/' + STAGING_PREFIX
TEMP_PREFIX = FILENAME_PREFIX + 'temp/'
TEMP_LOCATION = BUCKET + '/' + TEMP_PREFIX
URI_STAGING_LOCATION = 'gs://' + STAGING_LOCATION
URI_TEMP_LOCATION = 'gs://' + TEMP_LOCATION
FILE_EXT = '.txt'
NUM_SHARDS = 31
MAX_WORKERS = 500
JOB_NAME = ''


######################################
#         SQL to be pulled           #
######################################
pull_table_query =  """
             select *
             from 
             {0}    
            """.format(INPUT_TABLE_ID)

######################################
# Create Header List for Export File #
######################################
def create_header():
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

   COLUMN_LIST = pandas_gbq.read_gbq(col_query,project_id=PROJECT_ID,use_bqstorage_api=True)

   HEADER_LIST = COLUMN_LIST.astype(str).values.flatten().tolist()
   HEADER_STRING = '|'.join(HEADER_LIST)
   if DEBUG:
       print(HEADER_STRING) 
      
   # Create a new blob and upload the file's content.
   header_file = bucket.blob(HEADER_FILENAME)
   header_file.upload_from_string(HEADER_STRING, content_type="text/plain")
   return


######################################
#        Pipeline Configuration      #
######################################
#Set options for pipeline
pipeline_options = PipelineOptions(
    runner='DataflowRunner',
    project=PROJECT_ID',
    job_name=JOB_NAME,
    region='us-east4',
    subnetwork= '',
    use_public_ips=False,
    staging_location = URI_STAGING_LOCATION,
    temp_location = URI_TEMP_LOCATION,
    machine_type = "n1-standard-1", #This should almost never change
    max_num_workers = MAX_WORKERS,
    autoscaling_algorithm='THROUGHPUT_BASED'
   )
  
#Set options for the file to be exported
DESTINATION_FILE_CONFIG = {
  'file_path_prefix' : OUTPUT_FILENAME
  ,'file_name_suffix' : FILE_EXT
  ,'num_shards' : NUM_SHARDS # number of files to be created
  }


#ParDo class
class JoiningDoFn2(beam.DoFn):
    # Do lazy initializaiton here. Otherwise, error messages pop up, associated with "A large DoFn instance that is serialized for transmission to remote workers.""
    def __init__(self):
        import pandas as pd
        self.pd = pd
    def process(self,dic):
        return ['|'.join(str(x).replace('|', '_').replace('None', '') for x in dic.values())]
       
######################################
#               Pipeline             #
######################################

class DataFlowPipeline:
    """THIS IS THE CLASS THAT ACTUALLY RUNS THE JOB"""

    def run(self):
        """This is the job runner it holds the beam pipeline"""
        with beam.Pipeline(options=pipeline_options) as p:
            #Driver averaging pipeline
            ent_modeling = p | 'read table' >> beam.io.Read(beam.io.ReadFromBigQuery(query=pull_table_query, use_standard_sql=True)) \
                             | 'ParDo' >> beam.ParDo(JoiningDoFn2())  \
                             | 'Write Result to file' >> beam.io.WriteToText(**DESTINATION_FILE_CONFIG)
          

######################################
#          Stack Shards              #
######################################
def stack_shards():
    prefix = FILENAME_PREFIX + FILENAME
    outfile = FILENAME_PREFIX + FILENAME + FILE_EXT
    header_blob = bucket.blob(HEADER_FILENAME)
    blobs = [header_blob] #Define list for blobs starting with header file
    #verify all shards exists and create a list of shards
    for shard in range(NUM_SHARDS):
        sfile = '%s-%05d-of-%05d' % (prefix, shard, NUM_SHARDS) + FILE_EXT
        logging.info('Looking for file {}'.format(sfile))
        blob = bucket.blob(sfile)
        if not blob.exists():  #Fail if all blobs are not present
            raise ValueError('Shard {} not present'.format(sfile))
        blobs.append(blob) #create list of blobs
      # all shards exist, so compose
    bucket.blob(outfile).compose(blobs)
    logging.info('Successfully created {}'.format(outfile))
    return blobs
    
######################################
#    Delete Staging and Temp Files   #
######################################
def cleanup(shard_list):
    bucket = storage.Client().bucket(BUCKET)
    staging_blobs = bucket.list_blobs(prefix=STAGING_PREFIX)
    temp_blobs = bucket.list_blobs(prefix=TEMP_PREFIX)
    for blob in staging_blobs:
        blob.delete()
    for blob in temp_blobs:
        blob.delete()
    
    for blob in shard_list:
        blob.delete()
    return
    

######################################
#              Main                  #
######################################
if __name__ == "__main__":
    # initialize storage client and bucket
    client = storage.Client(project=PROJECT_ID) #initialize storage client
    bucket = client.bucket(BUCKET)
    #create header blob
    create_header()
    #initialize logging for pipeline
    logging.getLogger().setLevel(logging.INFO)
    logging.info('Setting up config for runner...')
    #initialize and run pipeline
    trainer = DataFlowPipeline()
    logging.info("Launching Pipeline")
    trainer.run()
    #stack shards
    shard_list = stack_shards()
    #delete shards, header, temp, and staging files
    clean_up(shard_list)
