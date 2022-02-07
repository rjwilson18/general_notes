#import libraries
import datetime
import os
import gnupg
import pprint

#define constants
KEY_FILE = 'key/DEG_2023.asc'

#install GPG
os.system('sudo apt-get install gpg')

#establish key location
gpg = gnupg.GPG()
key_data = open(KEY_FILE).read()
import_result = gpg.import_keys(key_data)


fingerprint = import_result.fingerprints.pop()

for key in gpg.list_keys():
    if key.get('fingerprint') == fingerprint:
            expire_dt = datetime.datetime.fromtimestamp(int(key.get('expires')))
            print(expire_dt)

