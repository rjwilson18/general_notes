#import libraries
from google.cloud import bigquery as bq

#define constants
PROJECT_ID = ''                                                                                  
DATASET_NAME = 'udf_library'                                                                               
DATASET_DESCRIPTION = 'Contains User Defined Functions for use in BQ'
DATASET_LOCATION = 'US'
DATASET_ID = PROJECT_ID + '.' + DATASET_NAME


                                                                                                                      
#initialize BQ client                                                                                        
bqclient = bq.Client(location=DATASET_LOCATION, project=PROJECT_ID)  


#Create dataset if it doesn't exist 
dataset = bq.Dataset(DATASET_ID)
dataset.location = 'US'
dataset.description = DATASET_DESCRIPTION

try:
    dataset = bqclient.create_dataset(dataset)
    print("Created dataset {0}.{1} in location {2}".format(bqclient.project, dataset.dataset_id,dataset.location))
except:
    print("Dataset " + DATASET_ID + " already exists")
