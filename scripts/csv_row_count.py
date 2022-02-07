#This bit of code will read in a CSV file from GCS and relatively quickly count the new line characters
#This is intended to give another way to count the rows in a large CSV file that doesn't work well loading into Pandas
#Keep in mind it will also count the header row if there is one, so it may overcount by 1 depending on the files
#As a timing example, this took about 3 minutes to count the rows in a 79 million row CSV

import gcsfs

project = ''
filepath = ''

def blocks(files, size=65536):
    while True:
        b = files.read(size)
        if not b: break
        yield b

fs = gcsfs.GCSFileSystem(project=project)
with fs.open(filepath, "r") as f:    
    print ( sum(bl.count("\n") for bl in blocks(f)) )
