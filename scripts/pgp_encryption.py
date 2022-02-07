#import libraries
import datetime
start = datetime.datetime.now()

import os
import sys
import gnupg
import pprint
from google.cloud import storage

#toggle for debug mode
DEBUG = 0

#define constants
PROJECT_ID = 'GCP_PROJECT_ID'
BUCKET_NAME = 'BUCKET_NAME'
KEY_FILE = 'key.asc'

#ensure correct amount of arguments were passed
#there should be 2, the name of the script and the name of the file to be encrypted
if len(sys.argv) != 2:
    print("Wrong Number of Arguments. Command should be as follows: python tu_encryption.py FILE_TO_ENCRYPT")
    sys.exit(2)
   
#determine file to be encrypted
TARGET_FILE = sys.argv[1]
print("Target file is : {}".format(TARGET_FILE))

EXPORT_FILE = TARGET_FILE + '.gpg'

#establish key location
gpg = gnupg.GPG()
key_data = open(KEY_FILE).read()
import_result = gpg.import_keys(key_data)

if DEBUG:
    print(import_result.fingerprints)

if DEBUG:
    pprint.pprint(gpg.list_keys())
  
#initiate storage client and bucket constructor
gcs_client = storage.Client(project=PROJECT_ID)
bucket = gcs_client.bucket(BUCKET_NAME)

#open blob
blob = bucket.get_blob(TARGET_FILE)
with blob.open("rb") as f:
    status = gpg.encrypt_file(
        f, 
        recipients=import_result.fingerprints,
        output=EXPORT_FILE,
        always_trust=True
        )

print('ok: ' + str(status.ok))
print('status: '+ status.status)
print('stderr: '+ status.stderr)

if status.ok:
    #place file back in gcs
    outblob = bucket.blob(EXPORT_FILE)
    outblob.upload_from_filename(EXPORT_FILE)
    #delete original file and local files
    blob.delete()
    os.remove(EXPORT_FILE)

end = datetime.datetime.now()

print('start: ' + str(start))
print('end: ' + str(end))
difference = end - start
print(difference)
