import os
import numpy as np

dset = None

def load():
    '''Load the dataset via memory mapping'''

    global dset

    npy_file = os.getenv("NUMPY_FILE")
    print("npyfile=", npy_file)
    if not os.path.exists(npy_file):
        raise Exception("Cannot locate numpy file at %s" % npy_file)

    print("Loading %s in mmap_mode..." % npy_file)
    dset = np.load(npy_file, mmap_mode='r')

def get(idx):
    '''Get the item at index "idx" '''

    global dset

    sz = dset.shape
    if idx>=sz[0]:
        raise Exception("Index at %d is out of range" % idx )

    return dset[idx]

if __name__ == "__main__":
    '''Entry point for unit test.'''

    load()

    sz = dset.shape

    print("Getting element at idx=0")
    print( get(0) )

    print("Getting element at idx=", sz[0]-1)
    print( get(sz[0]-1) )

