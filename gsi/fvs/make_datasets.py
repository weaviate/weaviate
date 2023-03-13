#
# Standard imports
#
import os
import sys
import struct

#
# Installed/external packages
#
import numpy
import datasets

#
# Constants
#

# Store/retrieve bigann competition datasets at/to this location
BIGANN_COMP_DATA = "/mnt/nas1/fvs_benchmark_datasets/bigann_competition_data/"

# Store/retrieve FVS benchmark datasets at/to this location
FVS_DATA_DIR = "/mnt/nas1/fvs_benchmark_datasets/"

# Deep1B query set filenames
DEEP1B_QUERY_OG_SET = "deep-queries.npy"
DEEP1B_QUERY_1000_SET = "deep-queries-1000.npy"
DEEP1B_QUERY_100_SET = "deep-queries-100.npy"
DEEP1B_QUERY_10_SET = "deep-queries-10.npy"

# Deep20M filenames
DEEP20M =  "deep-20M.npy"
DEEP20M_GT_1000 = "deep-20M-gt-1000.npy"
DEEP20M_GT_100 = "deep-20M-gt-100.npy"
DEEP20M_GT_10 = "deep-20M-gt-10.npy"

# 
# Configure modules
#

# Override the relative default "data" dir and point to NAS storage
datasets.BASEDIR = BIGANN_COMP_DATA

#
# Functions
#
def append_floatarray(fname, arr):
    '''This will create/append to a numpy file and add vectors to it.'''

    if len(arr.shape)!=2:
        raise Exception("expected an ndarray of two dimenions") 

    # declare the special bytes for the numpy header
    preheader = b'\x93\x4e\x55\x4d\x50\x59\x01\x00\x76\x00'
    fmt_header = "{'descr': '<f4', 'fortran_order': False, 'shape': (%d, %d), }"
    empty = b'\x20'
    fin = b'\x0a'

    # Get file descriptor and determine create/append mode
    # as well as current size if in append mode.
    append = False
    cur_items = 0
    fsize = 0
    f = None
    if os.path.exists(fname):
        fsize = os.path.getsize(fname)
        append = True
        if (fsize-128) % (arr.shape[1]*4) != 0:
            raise Exception("unexpected file size (%d,%d,%d)" % ( fsize, fsize-128, arr.shape[1] ) )
        cur_items = int( (fsize-128) / (arr.shape[1]*4) )
        f = open(fname,"r+b")
    else:
        f = open(fname,"wb")
        append = False
 
    # 
    # Write numpy header
    #
    f.seek(0)
    idx =0
    for i in range(len(preheader)):
        f.write( bytes([preheader[i]]) )
        idx += 1
    header = bytes( fmt_header % (cur_items+arr.shape[0],arr.shape[1]), 'ascii' )
    for i in range(len(header)):
        f.write( bytes([header[i]]) )
        idx += 1
    for i in range(idx, 127):
        f.write( bytes([empty[0]]) )
        idx += 1
    f.write( bytes([fin[0]]) )

    #
    # Append the array to the end of the file
    #
    if append:
        f.seek( fsize )
    for i in range(arr.shape[0]):
        flist = arr[i].tolist()
        buf = struct.pack( '%sf' % len(flist), *flist)
        f.write(buf)
    f.flush()
    f.close()

    return (cur_items+arr.shape[0],arr.shape[1])

def test_append():
    '''A unit test for the append function above.'''
    arr = numpy.ones( (2, 96) )
    append_floatarray("./test.npy", arr)
    arr = numpy.load("./test.npy")
    print("test=", arr)

# Verify Deep1B original queries
fpath = os.path.join(FVS_DATA_DIR, DEEP1B_QUERY_OG_SET)
if not os.path.exists(fpath):
    raise Exception("Deep1B original queries dataset does not exist->%s" %fpath)

# Create/verify deep-20M
fname = os.path.join( FVS_DATA_DIR, DEEP20M )
if not os.path.exists(fname):
    print("Creating", fname, "...")

    print("Downloading Competition Deep1B base, query, and gt...")
    ds = datasets.DATASETS["deep-20M"]()
    ds.prepare(True)

    for dt in ds.get_dataset_iterator(bs=1000):
        newsize = append_floatarray(fname, dt)
        print("appended, newsize=", newsize)
        if newsize[0]==20000000:
            break

    print("done") 

    if False:
        print("counting...")
        count = 0
        for dt in ds.get_dataset_iterator():
            count += 1
        print("%d" % count, type(dt), dt.shape, dt.dtype)

        arr = numpy.empty( (0,96), dt.dtype )
        print("arr shape", dt.shape)

        print("appending")
        for dt in ds.get_dataset_iterator():
            arr = numpy.concatenate( (arr, dt), axis=0 )
            print(dt.shape, arr.shape)

        print("saving",fname)
        numpy.save( fname, arr )
        print("done")
else:
    # Verify it
    print("Found %s.  Verifying it (this may take a sec.)" % fname)
    arr = numpy.load(fname)
    if arr.shape[0]!=20000000:
        raise Exception("Bad size for %s" % fname, arr.shape)
    print("Verified.")

# DEEP1B query set - 1000
fname = os.path.join( FVS_DATA_DIR, DEEP1B_QUERY_1000_SET )
if not os.path.exists(fname):
    ds = datasets.DATASETS["deep-20M"]()
    ds.prepare(False)

    queries = ds.get_queries()
    print(queries.shape)
    queries = queries[:1000,:]
    print(queries.shape)

    print("saving",fname)
    numpy.save( fname, queries )
    print("done")

else:
    # Verify it
    print("Found %s.  Verifying it..." % fname)
    arr = numpy.load(fname)
    if arr.shape[0]!=1000:
        raise Exception("Bad size for %s" % fname, arr.shape)
    print("Verified.")


# DEEP20M of DEEP1B, gt set - 1000
fname = os.path.join( FVS_DATA_DIR, DEEP20M_GT_1000)
if not os.path.exists(fname):
    ds = datasets.DATASETS["deep-20M"]()
    ds.prepare(False)

    I, D = ds.get_groundtruth()
    print(I.shape)
    I = I[:1000,:]
    print(I.shape)

    print("saving",fname)
    numpy.save( fname, I )
    print("done")

else:
    # Verify it
    print("Found %s.  Verifying it..." % fname)
    arr = numpy.load(fname)
    if arr.shape[0]!=1000:
        raise Exception("Bad size for %s" % fname, arr.shape)
    print("Verified.")


# DEEP1B query set - 100
fname = os.path.join( FVS_DATA_DIR, DEEP1B_QUERY_100_SET )
if not os.path.exists(fname):
    ds = datasets.DATASETS["deep-20M"]()
    ds.prepare(False)

    queries = ds.get_queries()
    print(queries.shape)
    queries = queries[:100,:]
    print(queries.shape)

    print("saving",fname)
    numpy.save( fname, queries )
    print("done")

else:
    # Verify it
    print("Found %s.  Verifying it..." % fname)
    arr = numpy.load(fname)
    if arr.shape[0]!=100:
        raise Exception("Bad size for %s" % fname, arr.shape)
    print("Verified.")


# DEEP20M of DEEP1B, gt set - 100
fname = os.path.join( FVS_DATA_DIR, DEEP20M_GT_100)
if not os.path.exists(fname):
    ds = datasets.DATASETS["deep-20M"]()
    ds.prepare(False)

    I, D = ds.get_groundtruth()
    print(I.shape)
    I = I[:100,:]
    print(I.shape)

    print("saving",fname)
    numpy.save( fname, I )
    print("done")

else:
    # Verify it
    print("Found %s.  Verifying it..." % fname)
    arr = numpy.load(fname)
    if arr.shape[0]!=100:
        raise Exception("Bad size for %s" % fname, arr.shape)
    print("Verified.")

# DEEP1B query set - 10
fname = os.path.join( FVS_DATA_DIR, DEEP1B_QUERY_10_SET )
if not os.path.exists(fname):
    ds = datasets.DATASETS["deep-20M"]()
    ds.prepare(False)

    queries = ds.get_queries()
    print(queries.shape)
    queries = queries[:10,:]
    print(queries.shape)

    print("saving",fname)
    numpy.save( fname, queries )
    print("done")

else:
    # Verify it
    print("Found %s.  Verifying it..." % fname)
    arr = numpy.load(fname)
    if arr.shape[0]!=10:
        raise Exception("Bad size for %s" % fname, arr.shape)
    print("Verified.")


# DEEP20M of DEEP1B, gt set - 10
fname = os.path.join( FVS_DATA_DIR, DEEP20M_GT_10)
if not os.path.exists(fname):
    ds = datasets.DATASETS["deep-20M"]()
    ds.prepare(False)

    I, D = ds.get_groundtruth()
    print(I.shape)
    I = I[:10,:]
    print(I.shape)

    print("saving",fname)
    numpy.save( fname, I )
    print("done")

else:
    # Verify it
    print("Found %s.  Verifying it..." % fname)
    arr = numpy.load(fname)
    if arr.shape[0]!=10:
        raise Exception("Bad size for %s" % fname, arr.shape)
    print("Verified.")


