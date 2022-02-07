#Loads records from the TU files where there was a credit match

#import libraries
import sys
import pandas as pd
import numpy as np
import hashlib
import datetime as dt
from google.cloud import storage
from google.cloud import bigquery as bq  

#define constants
GCP_PROJECT = 'GCP_PROJECT'  
SCHEMA_NAME = 'SCHEMA'                                                                               
TARGET_TABLE = 'TABLE_NAME'  
TEMP_TARGET_TABLE = 'temp_' + TARGET_TABLE

DATASET_ID = GCP_PROJECT + '.' + SCHEMA_NAME
TABLE_ID =  DATASET_ID + '.' + TARGET_TABLE  
TEMP_TABLE_ID = DATASET_ID + '.' + TEMP_TARGET_TABLE

#toggle debug mode
DEBUG = False

#Parse filename argument to get components

#ensure correct amount of arguments were passed
#there should be 2, the name of the script and the name of the file to be encrypted
if len(sys.argv) != 2:
    print("Wrong Number of Arguments. Command should be as follows: python accepts.py GCS_URI")
    sys.exit(2)
   
#determine file to be encrypted
GCS_LOCATION  = sys.argv[1]
FILE_NAME_LIST = GCS_LOCATION.split('/')

print('File to be processed: {}'.format(GCS_LOCATION))

BUCKET_NAME = FILE_NAME_LIST[2]
PREFIX_NAME = ('/').join(FILE_NAME_LIST[3:-1])
FILE_NAME = FILE_NAME_LIST[-1]


if DEBUG:
    print(FILE_NAME_LIST)
    print(BUCKET_NAME)
    print(PREFIX_NAME)
    print(FILE_NAME)
                                                                                                                      
#initialize BQ client                                                                                        
bqclient = bq.Client(location="US", project=GCP_PROJECT)  


#Load CSV
raw_credit = pd.read_csv(GCS_LOCATION, sep='|')

if DEBUG:
    print(raw_credit.dtypes)
    print(raw_credit.head())



#Determine OpCo

first_cust_id = str(raw_credit['customerInput_customerid'].iloc[1])

OPCO = 'OPCO1' if first_cust_id.find('-') > -1 else 'OPCO2'
print("file belongs to {} customers".format(OPCO))

#Import Schema
SCHEMA = eval(open("ent_credit_schema.txt").read())

#schema list for bq
SCHEMA_LIST = []
for key, value in list(SCHEMA.items()):
    #deal with trailing spaces in keys
    SCHEMA[key.strip()] = SCHEMA.pop(key)
    key = key.strip()
    
    #add to table schema
    SCHEMA_LIST.append(bq.SchemaField(key.strip(), SCHEMA[key]['type'],SCHEMA[key]['mode'],SCHEMA[key]['description']))

if DEBUG:
    print(SCHEMA_LIST)

#feature engineering pt 1 - column formatting

fe_credit = raw_credit

#replace NaNs with ''
fe_credit = fe_credit.fillna('')

#cleanup column headers
fe_credit.columns = fe_credit.columns.str.replace('pppcl_','')
fe_credit.columns = fe_credit.columns.str.replace('customerInput_','')
fe_credit.columns = fe_credit.columns.str.replace('creditAsOfDate_','')

#column rename(s)
fe_credit = fe_credit.rename(columns={
    "field1":"new_field1"
    ,"filed2":"new_field2"
  })



#change some columns to strings
convert_to_string_list = ['field1','field2']
for col in convert_to_string_list:
    fe_credit[col] = fe_credit[col].astype(str)
    
#change credit_date to date
fe_credit['year'] = fe_credit["creditAsOfDate"].str[:4]
fe_credit['month'] = fe_credit["creditAsOfDate"].str[5:7]
fe_credit['day'] = fe_credit["creditAsOfDate"].str[8:10]
fe_credit['credit_date'] = pd.to_datetime(fe_credit[['year','month','day']])

#change dob into date
try:
    fe_credit['year'] = fe_credit["dob"].str[:4]
    fe_credit['month'] = fe_credit["dob"].str[5:7]
    fe_credit['day'] = '01'
    fe_credit['dob'] =  pd.to_datetime(fe_credit[['year','month','day']])
except:
    #invalid input
    fe_credit['dob'] = pd.NA
   
if DEBUG:
    print(fe_credit.dtypes)
    print(fe_credit.head())


#feature engineering pt 2 - record_hash_cd

#define columns to be hashed
hash_columns = HASH_COLUMN_LIST

#convert hash columns to strings if need be
for i in range(len(hash_columns)):
    col = hash_columns[i]
    fe_credit[col + '_hash'] = fe_credit[col].astype(str)
    hash_columns[i] = col + '_hash'
    
    
#create single source column for hashing
fe_credit['hash_src'] = fe_credit[hash_columns].agg('|'.join, axis=1)


fe_credit['record_hash_cd'] = fe_credit['hash_src'].apply(lambda x:(hashlib.sha256(x.encode('utf-8')).hexdigest().upper()))

if DEBUG:
    print(fe_credit.head())





#feature engineering pt 3 - opco, credit_hit, uodate/ingestion_ts

fe_credit['opco'] = OPCO
fe_credit['credit_hit'] = True
fe_credit['duplicate_manipulated_ind'] = False
fe_credit['ingestion_ts'] = dt.datetime.now()
fe_credit['updated_ts'] = pd.Timestamp(year=1900,month=1,day=1,hour=0)
fe_credit['ingestion_filename'] = FILE_NAME

if DEBUG:
    print(fe_credit.head())


#feature engineering pt 4 - null columns
for key in SCHEMA:
    if key.strip() not in fe_credit.columns:
        fe_credit[key] = pd.NA
        print('Null column added for {}'.format(key))
        
#Define dataset and table
dataset = bq.Dataset(DATASET_ID)

try:
    dataset = bqclient.create_dataset(dataset)
except:
    print("Dataset " + DATASET_ID + " already exists")

try:
    table = bq.Table(TABLE_ID,SCHEMA_LIST)
    table.time_partitioning = bq.TimePartitioning(
      type_=bq.TimePartitioningType.MONTH
      ,field = 'credit_date'
      ,
     )
    table_results = bqclient.create_table(table)
    print("Table " + TABLE_ID + " created")
except:
    print("Table " + TABLE_ID + " already exists")


#create staging table

#Delete temp table if exists                                                                                             
bqclient.delete_table(TEMP_TABLE_ID, not_found_ok=True)  

#Define and create staging table
table = bq.Table(TEMP_TABLE_ID,SCHEMA_LIST)
table_results = bqclient.create_table(table)

print("Table " + TEMP_TABLE_ID + " created")

#trim dataframe into only the columns to be uploaded
final_credit = fe_credit[SCHEMA.keys()].drop_duplicates()

if DEBUG:
    print(final_credit.head())
    print(final_credit.dtypes)

#load dataframe into staging table

#Load CSV into BQ
job_config = bq.LoadJobConfig(
    schema=SCHEMA_LIST
    ,write_disposition='WRITE_TRUNCATE'
 )

load_job = bqclient.load_table_from_dataframe(
    final_credit
    ,table
    ,job_config=job_config
 )

load_job.result()  # Waits for table load to complete.

ROWS_LOADED = load_job.output_rows
print('Loaded {} rows into {}.'.format(
    ROWS_LOADED, table.path))

#Dedupe staging table, keeping the record with the highest amfam auto score. If there is still a tie use the record with the highest tu_consumer_id

DEDUPE_QUERY = '''
merge {0} as staging using(
 select 
  st.record_hash_cd
  ,dd.dups_ind
  ,max(st.tu_consumer_id) as tu_consumer_id
 from
  {0} st
 inner join 
  (
  select
   stage.record_hash_cd
   ,max(stage.amfam_custom_auto_score) as amfam_custom_auto_score
   ,case when count(1) > 1 then True else False end as dups_ind
  from 
   {0} stage
  group by
   stage.record_hash_cd
  ) dd
  on st.record_hash_cd = dd.record_hash_cd
  and st.amfam_custom_auto_score = dd.amfam_custom_auto_score
 group by
  st.record_hash_cd
  ,dd.dups_ind
) as max_score
 on staging.record_hash_cd = max_score.record_hash_cd
 and staging.tu_consumer_id = max_score.tu_consumer_id
when matched then
 update set
  staging.duplicate_manipulated_ind = max_score.dups_ind
when not matched by source then 
 delete
;
'''.format(TEMP_TABLE_ID)


dedupe = bqclient.query(DEDUPE_QUERY)  # API request
dedupe.result() # Waits for table load to complete.

#row count query 0B processed
ROW_QUERY = '''
select 1
from {0}
'''.format(TEMP_TABLE_ID)

row_count = bqclient.query(ROW_QUERY)  # API request
#row_count.result() # Waits for table load to complete.

ROWS_REMAINING = row_count.result().total_rows

print("ROWS IN: {}".format(str(ROWS_LOADED)))
print("ROWS AFTER DEDUPE: {}".format(str(ROWS_REMAINING)))
print("ROWS REMOVED: {}".format(str(ROWS_LOADED - ROWS_REMAINING)))
      

#query metadata to see how many rows remain 


#CDC process to load new rows and update old ones

MERGE_QUERY = '''
merge {0} as ent using(
select 
 {2}
from 
 {1}
) as credit
on
  ent.record_hash_cd = credit.record_hash_cd
when matched then
  update set
   ent.field1 = credit.field1
   ,ent.field2 = credit.field2
when not matched by target then
 insert({2})
 values(
  {2}
  )
'''.format(TABLE_ID,TEMP_TABLE_ID,','.join(SCHEMA.keys()))

add_credit = bqclient.query(MERGE_QUERY)  # API request
add_credit.result() # Waits for table load to complete.
print('Loaded {} bytes processing  this addition to {}.'.format(add_credit.total_bytes_processed, TABLE_ID))

#cleanup

#Delete temp table if exists                                                                                             
bqclient.delete_table(TEMP_TABLE_ID, not_found_ok=True)  
