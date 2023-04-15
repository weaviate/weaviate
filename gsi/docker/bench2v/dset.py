import os
import numpy as np

database_dset = None
query_dset = None
gt_dset = None

def load():
    '''Load the dataset via memory mapping'''

    global database_dset, query_dset, gt_dset

    database_file = os.getenv("DATABASE_FILE")
    if not os.path.exists(database_file):
        raise Exception("Cannot locate numpy file at %s" % database_file)
    print("Loading %s in mmap_mode..." % database_file)
    database_dset = np.load(database_file, mmap_mode='r')

    query_file = os.getenv("QUERY_FILE")
    if not os.path.exists(query_file):
        raise Exception("Cannot locate numpy file at %s" % query_file)
    print("Loading %s in mmap_mode..." % query_file)
    query_dset = np.load(query_file, mmap_mode='r')

    gt_file = os.getenv("GROUND_TRUTH_FILE")
    if not os.path.exists(gt_file):
        raise Exception("Cannot locate numpy file at %s" % gt_file)
    print("Loading %s in mmap_mode..." % gt_file)
    gt_dset = np.load(gt_file, mmap_mode='r')

def get(idx):
    '''Get the item at index "idx" in the database dataset'''

    global dset

    sz = database_dset.shape
    if idx>=sz[0]:
        raise Exception("Index at %d is out of range" % idx )

    return database_dset[idx]

def query(idx):
    '''Get the item at index "idx" in the query set'''

    global query_dset, gt_dset

    sz = query_dset.shape
    if idx>=sz[0]:
        raise Exception("Index at %d is out of range" % idx )

    gt = None
    if type(gt_dset) != type(None):
        gt = gt_dset[idx][0:10]
        #print("Ground truth=", gt)

    return (query_dset[idx], gt)


if __name__ == "__main__":
    '''Entry point for unit test.'''

    load()

    sz = database_dset.shape

    print("Getting element at idx=0")
    print( get(0) )

    print("Getting element at idx=", sz[0]-1)
    print( get(sz[0]-1) )

