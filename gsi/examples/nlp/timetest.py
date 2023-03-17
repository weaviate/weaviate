import os
from datetime import datetime

DATADIR = "/mnt/nas1/news20/"
def absoluteFilePaths(directory):
    for dirpath,_,filenames in os.walk(directory):
        for f in filenames:
            yield os.path.abspath(os.path.join(dirpath, f))
paths = absoluteFilePaths(DATADIR)

def timetest():
    cur, t_start = -1, datetime.now()
    for i, path in enumerate(paths):
        with open(path, errors='ignore') as file:
            data = file.read()
            cur += 1
            file.close()
        if i != cur:
            print(path, i)
            break

    print(datetime.now() - t_start)
    
timetest()