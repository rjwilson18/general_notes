## By default, running a script on a Google VM will use the service account for that VM. 
## As of this writing, service accounts do not have the ability to connect to data outside of the project that service account exists in.
## That makes it tough to run queries from the dev project against a prod data source
## Below is some sample code that can be added to a script to make it prompt the user to authenticate as themselves
## After that is done, the script will run with whatever permissions the user has, allowing it to cross projects if the user can


## You may need to install the pydata-google-auth package to use this
#pip install --upgrade pydata-google-auth

## Import the following at a minimum
import google.auth
import pydata_google_auth

## These imports are needed for the sample uses below. Your code may not need these or need other ones
from pyspark.sql import SparkSession
from google.cloud import bigquery


# force creation of credentials for local user vs. service account.    
# The service account will not have permissions to big query and a permissions error will be generated
# credentials expire across sessions so each time you run a notebook make sure this is executed once.  
# The try statement below should prevent this from running more than once
# ******* clear the output so the secret isn't available before loading this back up to gitlab *******

## This will propmt the user to visit a link. On that link, they will authenticate themselves and be given a code. That code should be copied and pasted back into the prompt

try: 
    if credentials:
        print('credentials exist and were not re-requested. restart kernal to force reauth or change code.')
except:
    pydata_google_auth.cache.REAUTH
    credentials = pydata_google_auth.get_user_credentials(
        scopes=['https://www.googleapis.com/auth/cloud-platform',
            'https://www.googleapis.com/auth/bigquery'],
        credentials_cache=pydata_google_auth.cache.REAUTH,
         )


## To set up a BigQuery connection that uses the stored credentials, set up as following:
client = bigquery.Client(credentials=credentials)


## To set up a SparkSession connection that uses the stored credentials, set up as following:
spark = SparkSession.builder \
.appName('GCP Spark Test') \
.config('spark.jars.packages','com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.19.1') \
.getOrCreate()

## NOTE: The spark jar version may change as things are upgraded

spark.conf.set("viewsEnabled","true")
spark.conf.set("materializationDataset","test")
spark.conf.set("gcpAccessToken", credentials.token)


## The results of a query can be loaded directly into a dataframe using the below syntax
## The materializationDataset configuration dataset from above is where the query results will be saved as a temporary table before being loaded into the SparkSession. That table will be set to expire in 24 hours

df_sql = spark.read.format("bigquery").load(sql)
