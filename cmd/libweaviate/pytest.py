from ctypes import *
lib = CDLL('libweaviate.so')
startWeaviate = lib.startWeaviate

from numpy.ctypeslib import ndpointer
# Define the types of the output and arguments of
# this function.
lib.startWeaviate.restype = None
lib.startWeaviate.argtypes = [c_void_p, c_void_p, c_void_p, c_void_p]  # cast everything to void*, lalala


#lib.startWeaviate('./data', 'http://localhost:8080', 'node1', 'none')

lib.dumpBucket.restype = None
lib.dumpBucket.argtypes = [c_void_p, c_void_p] # absolutely nothing will go wrong

lib.dumpBucket('data/classa_a1CVm6qhR9q6_lsm/property_title', 'title')

